// Package proxy 实现只处理 GET /assets/{id} 的缓存反向代理。
//
// 缓存策略（与需求逐条对应）：
//
//   - 仅缓存 200 响应，且必须带 ETag、max-age 在 0～60 秒之间、
//     显式 Vary: Accept-Language、不含 no-store/private；
//   - 语言变体仅支持 zh / en，缓存键为 (资源ID, 变体)；其余变体一律不缓存
//     并在响应上声明 no-store，因此一种语言的缓存内容不可能交给另一种语言；
//   - 新鲜期过后用 If-None-Match 条件请求回源；源站 304 会以原 max-age
//     刷新 storedAt，即“延长原响应的新鲜期”；
//   - 同键并发请求通过 roundGroup 合并为一次回源；等待者取消不影响他人；
//     同步轮与 SWR 后台轮在同一把锁下原子互斥，同一 (资源, 变体) 任意
//     时刻至多一轮回源；
//   - 回源遇到网络错误或 5xx 时，可返回已过期但不超过 30 秒的旧响应，
//     并显式打上 X-Cache-Status: stale 与 Warning 110 标记；
//   - 源站显式声明 stale-while-revalidate=N 时启用后台刷新模式：条目过期
//     且仍在该窗口内时，非条件请求立即拿到带显式陈旧标记的完整旧内容，
//     同时每 (资源, 变体) 只启动一轮脱离客户端生命周期的后台条件回源；
//     304 续期、可缓存 200 替换条目、4xx 依旧失效，网络错误/5xx 保留旧内容；
//     后台结果提交前必须确认它仍对应当前缓存条目，迟到刷新不会覆盖新内容；
//     未声明该指令的资源保持原有同步再验证与故障兜底行为；
//   - 客户端 If-None-Match 只在正确变体的缓存条目上校验，跨语言不可能 304；
//     陈旧条目永不凭客户端校验器直接回 304（一律走条件回源）。
package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"cacheproxy/internal/lang"
)

const (
	staleServeWindow = 30 * time.Second // 出错时旧响应最多可过期多久仍可兜底
	maxBodyBytes     = 1 << 20
)

// Option 注入代理依赖（测试用）。
type Option func(*Proxy)

// WithClock 注入可控时钟。
func WithClock(c Clock) Option { return func(p *Proxy) { p.clock = c } }

// WithClient 注入回源 HTTP 客户端。
func WithClient(c *http.Client) Option { return func(p *Proxy) { p.client = c } }

// Proxy 是缓存代理。
type Proxy struct {
	origin *url.URL
	client *http.Client
	clock  Clock
	mu     sync.RWMutex
	store  map[string]*entry
	rounds *roundGroup // 每 (资源, 变体) 至多一轮在飞回源（同步/后台原子互斥）
}

// entry 是一条按 (资源, 变体) 存储的缓存条目。
type entry struct {
	header     http.Header // 仅保留可安全复用的响应头
	body       []byte
	etag       string
	maxAge     int
	swrEnabled bool
	swr        time.Duration
	storedAt   time.Time
}

// age 返回条目自存储以来经过的时间。
func (p *Proxy) age(e *entry) time.Duration {
	return p.clock.Now().Sub(e.storedAt)
}

// fresh 判断条目是否仍在新鲜期内（边界：age == maxAge 即过期）。
func (p *Proxy) fresh(e *entry) bool {
	return p.age(e) < time.Duration(e.maxAge)*time.Second
}

// staleUsable 判断过期条目是否仍处于出错兜底窗口（含 30 秒整边界）。
func (p *Proxy) staleUsable(e *entry) bool {
	a := p.age(e)
	return a >= time.Duration(e.maxAge)*time.Second &&
		a <= time.Duration(e.maxAge)*time.Second+staleServeWindow
}

// staleWhileRevalidating 判断过期条目是否仍处于源站声明的
// stale-while-revalidate 窗口（含边界 age == maxAge+swr）。
// 未声明该指令的条目恒为 false，保持原有同步再验证行为。
func (p *Proxy) staleWhileRevalidating(e *entry) bool {
	if !e.swrEnabled {
		return false
	}
	a := p.age(e)
	return a >= time.Duration(e.maxAge)*time.Second &&
		a <= time.Duration(e.maxAge)*time.Second+e.swr
}

// New 创建代理，originBase 为源站基址（如 http://origin:8081）。
func New(originBase string, opts ...Option) (*Proxy, error) {
	u, err := url.Parse(originBase)
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		origin: u,
		client: &http.Client{Timeout: 15 * time.Second},
		clock:  systemClock{},
		store:  map[string]*entry{},
		rounds: newRoundGroup(),
	}
	for _, o := range opts {
		o(p)
	}
	return p, nil
}

func cacheKey(id string, v lang.Variant) string { return id + "\x00" + string(v) }

// ServeHTTP 仅接受 GET /assets/{id}。
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/assets/") {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/assets/")
	if id == "" || strings.Contains(id, "/") {
		http.NotFound(w, r)
		return
	}

	variant, supported := lang.Parse(r.Header.Get("Accept-Language"))
	if !supported {
		// 不支持的语言变体：不参与缓存键合并，逐请求回源，并强制 no-store。
		p.serveUnsupportedVariant(w, r)
		return
	}
	key := cacheKey(id, variant)
	clientINM := r.Header.Get("If-None-Match")

	if e := p.lookup(key); e != nil && p.fresh(e) {
		writeStored(w, p.clock, e, "hit", clientINM)
		return
	}

	if e := p.lookup(key); e == nil && clientINM != "" {
		// 冷路径上的客户端条件请求：本地没有“正确变体”的表示可供校验，
		// 而响应取决于客户端持有的具体校验器，故直接透传（不做合并）；
		// 若源站给出可缓存的 200，仍然写入共享缓存。
		p.forwardColdConditional(w, r, key)
		return
	}

	// 非条件请求命中“已过期但仍在 stale-while-revalidate 窗口内”的条目：
	// 立即返回带显式陈旧标记的完整旧内容，同时至多启动一轮后台条件回源。
	// startAsyncIfFree 与同步路径的 joinSync 共用同一把锁：同键已有任何一轮
	// （后台或同步）时都不会再发起第二轮回源。
	if clientINM == "" {
		if e := p.lookup(key); e != nil && p.staleWhileRevalidating(e) {
			if rr, started := p.rounds.startAsyncIfFree(key); started {
				p.runRefresh(rr, key, id, variant, e)
			}
			writeStale(w, p.clock, e, "")
			return
		}
	}

	// 同步路径（冷缺失、窗口外再验证、客户端条件请求）：加入/开启该键唯一
	// 一轮回源。陈旧条目不能凭客户端 If-None-Match 本地回 304；若等待的是
	// 后台轮，其 304 续期后再交 writeStored 按客户端校验器决定 304/完整 200。
	var f *round
	for {
		rr, leader := p.rounds.joinSync(key)
		if leader {
			f = rr
			break
		}
		if rr.role == roundSync {
			// 同步轮等待者：可随时放弃，不影响 leader 与其他等待者。
			if !rr.wait(r.Context().Done()) || r.Context().Err() != nil {
				return
			}
			writeOutcome(w, p.clock, rr.res, clientINM)
			return
		}
		// 与一轮后台刷新汇合：等它提交（可被本请求取消打断）后复查缓存。
		if !rr.wait(r.Context().Done()) || r.Context().Err() != nil {
			return
		}
		if e := p.lookup(key); e != nil && p.fresh(e) {
			writeStored(w, p.clock, e, "revalidated", clientINM)
			return
		}
		// 后台轮没带来新鲜条目（5xx/网络错误保留旧条目、4xx 删除条目）：
		// joinSync 会在同一把锁内原子接管为同步 leader，循环只此一次。
	}

	// 本请求是该键同步回源的 leader。
	var out outcome
	if e := p.lookup(key); e != nil && p.fresh(e) {
		// Double-check：可能刚被冷条件透传路径填充。
		out = outcome{kind: ocStored, entry: e, label: "hit"}
	} else {
		// 关键：回源上下文脱离客户端生命周期。某个等待者（甚至发起者）取消
		// 请求都不会中断这次回源；即便发起者断开，本协程仍会等回源完成并
		// 发布结果，保证其他等待者与后来加入的请求不会被挂死。
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
		out = p.fetch(ctx, key, id, variant, p.lookup(key))
		cancel()
	}
	f.setOutcome(out)       // 先发布结果唤醒同步等待者
	p.rounds.finish(key, f) // 再拆键，避免迟到者开启重复回源
	if r.Context().Err() != nil {
		return // 发起者已断开，结果留给等待者
	}
	writeOutcome(w, p.clock, out, clientINM)
}

func (p *Proxy) lookup(key string) *entry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.store[key]
}

func (p *Proxy) put(key string, e *entry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store[key] = e
}

// ---------- 回源 ----------

// decisionKind 是一次回源在“尚未触碰缓存”时得出的处置结论。
type decisionKind int

const (
	decRenewed  decisionKind = iota // 304：旧表示续期
	decReplaced                     // 可缓存 200：新表示替换
	decInvalid                      // 4xx / 不可缓存 200：旧条目必须作废，响应透传
	decFail                         // 网络错误 / 读错 / 5xx
)

// decision 是一次回源的结论与构造响应所需的全部材料；是否落缓存由调用方
// （同步路径或后台 SWR 路径）按各自语义决定。
type decision struct {
	kind   decisionKind
	entry  *entry // decRenewed / decReplaced 下提交后的新条目
	reason string // decFail 原因（origin-500 / origin-network-error ...）
	status int    // decInvalid 的透传状态码
	header http.Header
	body   []byte
}

// revalidate 对源站执行一次（existing 非 nil 时为条件的）回源并得出结论。
// 本函数不修改缓存，便于同步路径与后台 SWR 路径共用同一套判定。
func (p *Proxy) revalidate(ctx context.Context, key string, existing *entry, req *http.Request) decision {
	resp, err := p.client.Do(req)
	if err != nil {
		return decision{kind: decFail, reason: "origin-network-error"}
	}
	defer resp.Body.Close()
	body, readErr := readLimited(resp.Body)

	switch {
	case resp.StatusCode == http.StatusNotModified:
		if existing == nil {
			// 冷路径不应得到 304；原样透传，不触碰缓存。
			return decision{kind: decInvalid, status: resp.StatusCode,
				header: sanitizeHeaders(resp.Header), body: body}
		}
		if readErr != nil {
			return decision{kind: decFail, reason: "origin-read-error"}
		}
		return decision{kind: decRenewed, entry: p.renewedEntry(existing, resp)}

	case resp.StatusCode == http.StatusOK:
		if readErr != nil {
			return decision{kind: decFail, reason: "origin-read-error"}
		}
		if pol, ok := cacheable(resp.Header); ok {
			return decision{kind: decReplaced,
				entry: newEntry(resp, body, pol, p.clock.Now())}
		}
		// 源站已不再允许缓存此表示：旧条目作废，响应透传。
		return decision{kind: decInvalid, status: http.StatusOK,
			header: sanitizeHeaders(resp.Header), body: body}

	case resp.StatusCode >= 500:
		// 5xx 不删除旧条目：窗口内仍可服务过期内容（同步兜底或后台保留）。
		return decision{kind: decFail, reason: "origin-" + strconv.Itoa(resp.StatusCode)}

	default:
		// 4xx 等：源站否定了该资源，旧表示必须作废，响应透传。
		if readErr != nil {
			return decision{kind: decFail, reason: "origin-read-error"}
		}
		return decision{kind: decInvalid, status: resp.StatusCode,
			header: sanitizeHeaders(resp.Header), body: body}
	}
}

// renewedEntry 依据 304 响应构造续期条目：以（可能被 304 更新的）缓存头
// 重新计算新鲜度口径，内容沿用旧表示，storedAt 刷新为当前时刻。
func (p *Proxy) renewedEntry(existing *entry, resp *http.Response) *entry {
	refreshed := *existing
	refreshed.header = cloneHeader(existing.header)
	if etag := resp.Header.Get("ETag"); etag != "" {
		refreshed.etag = etag
		refreshed.header.Set("ETag", etag)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "" {
		refreshed.header.Set("Cache-Control", cc)
	}
	if pol, ok := cacheable(refreshed.header); ok {
		refreshed.maxAge = pol.maxAge
		refreshed.swrEnabled = pol.swrEnabled
		refreshed.swr = pol.swr
	}
	refreshed.storedAt = p.clock.Now()
	return &refreshed
}

// fetch 是同步路径的一次回源：执行条件请求、按结论落缓存并产出对客户端的 outcome。
func (p *Proxy) fetch(ctx context.Context, key, id string, variant lang.Variant, existing *entry) outcome {
	req := p.buildOriginRequest(ctx, rAsset{id: id, variant: variant})
	if existing != nil {
		req.Header.Set("If-None-Match", existing.etag)
	}
	d := p.revalidate(ctx, key, existing, req)
	return p.applyDecision(key, existing, d)
}

// applyDecision 把回源结论应用到缓存，并映射为同步响应 outcome。
// 与后台提交一样，任何写入都基于发起回源时持有的 existing 做指针校验：
// 若条目已被并发的后台刷新推进，本轮旧结论必须让位，避免回退。
func (p *Proxy) applyDecision(key string, existing *entry, d decision) outcome {
	switch d.kind {
	case decRenewed:
		if existing != nil && !p.commitIfCurrent(key, existing, d.entry) {
			return p.currentEntryOutcome(key, existing)
		}
		if existing == nil {
			p.put(key, d.entry)
		}
		return outcome{kind: ocStored, entry: d.entry, label: "revalidated"}
	case decReplaced:
		if existing != nil && !p.commitIfCurrent(key, existing, d.entry) {
			return p.currentEntryOutcome(key, existing)
		}
		if existing == nil {
			p.put(key, d.entry)
		}
		return outcome{kind: ocStored, entry: d.entry, label: "miss"}
	case decInvalid:
		if existing != nil && !p.removeIfCurrent(key, existing) {
			return p.currentEntryOutcome(key, existing)
		}
		return outcome{kind: ocBypass, status: d.status, header: d.header, body: d.body}
	default: // decFail
		return p.failureOutcome(existing, d.reason)
	}
}

// currentEntryOutcome 用于“本轮结论迟到、缓存已被并发刷新推进”的场景：
// 让位给当前条目；极端情况下当前条目也缺失时退回失败兜底。
func (p *Proxy) currentEntryOutcome(key string, fallback *entry) outcome {
	if cur := p.lookup(key); cur != nil {
		return outcome{kind: ocStored, entry: cur, label: "revalidated"}
	}
	return p.failureOutcome(fallback, "origin-revalidate-lost-race")
}

// ---------- 后台 stale-while-revalidate ----------

// runRefresh 在脱离任何客户端生命周期的协程里执行一轮后台条件回源：
// 客户端取消（甚至无人等待）都不会中断它。round r 已由 startAsyncIfFree
// 原子登记（同键唯一一轮）；结论在 commitRefresh 中校验“仍是当前条目”
// 后才提交，最后关闭 round 并拆键。
func (p *Proxy) runRefresh(r *round, key, id string, variant lang.Variant, base *entry) {
	go func() {
		defer p.rounds.finish(key, r)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 30*time.Second)
		defer cancel()
		req := p.buildOriginRequest(ctx, rAsset{id: id, variant: variant})
		req.Header.Set("If-None-Match", base.etag) // 后台刷新永远是条件回源
		d := p.revalidate(ctx, key, base, req)
		p.commitRefresh(key, base, d)
		close(r.done) // 先提交缓存，再通知等待者
	}()
}

// commitRefresh 提交后台刷新结论。任何写入前都必须确认发起本轮刷新时所依据
// 的条目仍是当前缓存条目——若它已被别的路径（同步再验证、替换等）更新或
// 删除，本轮结果属于“迟到提交”，必须整体丢弃，绝不能把旧内容重新写成新版本。
func (p *Proxy) commitRefresh(key string, base *entry, d decision) {
	switch d.kind {
	case decRenewed, decReplaced:
		p.commitIfCurrent(key, base, d.entry)
	case decInvalid:
		// 4xx / 不再可缓存：只有当前条目仍是本轮依据时才作废。
		p.removeIfCurrent(key, base)
	case decFail:
		// 网络错误 / 5xx / 读错：不得抹去仍可用的旧内容，什么都不做。
	}
}

// commitIfCurrent 仅当 store[key] 仍是 base 时才替换为 next。
func (p *Proxy) commitIfCurrent(key string, base, next *entry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store[key] != base {
		return false // 已被更新或删除：迟到结果丢弃
	}
	p.store[key] = next
	return true
}

// removeIfCurrent 仅当 store[key] 仍是 base 时才删除。
func (p *Proxy) removeIfCurrent(key string, base *entry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store[key] != base {
		return false
	}
	delete(p.store, key)
	return true
}

// failureOutcome 在回源失败时选择“过期兜底”或 502。
func (p *Proxy) failureOutcome(existing *entry, reason string) outcome {
	if existing != nil && p.staleUsable(existing) {
		return outcome{kind: ocStale, entry: existing, reason: reason}
	}
	return outcome{kind: ocError, reason: reason}
}

// forwardColdConditional 透传冷路径上的客户端条件请求。
func (p *Proxy) forwardColdConditional(w http.ResponseWriter, r *http.Request, key string) {
	variant, _ := lang.Parse(r.Header.Get("Accept-Language"))
	id := strings.TrimPrefix(r.URL.Path, "/assets/")
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
	defer cancel()

	req := p.buildOriginRequest(ctx, rAsset{id: id, variant: variant})
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		req.Header.Set("If-None-Match", inm)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		w.Header().Set("X-Cache-Status", "error")
		http.Error(w, "origin unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	body, _ := readLimited(resp.Body)

	if resp.StatusCode == http.StatusOK {
		if pol, ok := cacheable(resp.Header); ok {
			e := newEntry(resp, body, pol, p.clock.Now())
			p.put(key, e)
			writeStored(w, p.clock, e, "miss", "")
			return
		}
	}
	writePassthrough(w, resp.StatusCode, resp.Header, body, "bypass")
}

// serveUnsupportedVariant 处理 zh/en 之外的语言变体：逐请求回源、强制不缓存。
func (p *Proxy) serveUnsupportedVariant(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/assets/")
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
	defer cancel()

	req := p.buildOriginRequest(ctx, rAsset{id: id, variant: "", rawAcceptLanguage: r.Header.Get("Accept-Language")})
	resp, err := p.client.Do(req)
	if err != nil {
		w.Header().Set("X-Cache-Status", "error")
		http.Error(w, "origin unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	body, _ := readLimited(resp.Body)

	h := sanitizeHeaders(resp.Header)
	// 明确声明该变体响应不得缓存，防止污染任何语言的缓存。
	h.Set("Cache-Control", "no-store")
	if resp.StatusCode == http.StatusNotModified {
		h.Del("Content-Length")
	}
	writePassthrough(w, resp.StatusCode, h, body, "bypass")
}

type rAsset struct {
	id                string
	variant           lang.Variant // 受支持变体
	rawAcceptLanguage string       // 不受支持时保留原始值
}

func (p *Proxy) buildOriginRequest(ctx context.Context, a rAsset) *http.Request {
	u := *p.origin
	u.Path = "/assets/" + a.id
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if a.rawAcceptLanguage != "" {
		req.Header.Set("Accept-Language", a.rawAcceptLanguage)
	} else if a.variant != "" {
		req.Header.Set("Accept-Language", string(a.variant))
	}
	return req
}

// ---------- outcome 与响应写出 ----------

type outcomeKind int

const (
	ocStored outcomeKind = iota // 可用缓存条目（新鲜）
	ocStale                     // 出错兜底的过期条目
	ocBypass                    // 不可缓存/非错误响应，透传
	ocError                     // 回源失败且无兜底
)

type outcome struct {
	kind   outcomeKind
	entry  *entry
	label  string // ocStored 的 X-Cache-Status
	reason string // ocStale/ocError 的原因
	status int    // ocBypass
	header http.Header
	body   []byte
}

func writeOutcome(w http.ResponseWriter, c Clock, out outcome, clientINM string) {
	switch out.kind {
	case ocStored:
		writeStored(w, c, out.entry, out.label, clientINM)
	case ocStale:
		writeStale(w, c, out.entry, out.reason)
	case ocBypass:
		writePassthrough(w, out.status, out.header, out.body, "bypass")
	case ocError:
		w.Header().Set("X-Cache-Status", "error")
		w.Header().Set("X-Cache-Error", out.reason)
		http.Error(w, "origin unavailable", http.StatusBadGateway)
	}
}

// storedHeaderKeys 是缓存条目允许跨响应复用的头。
var storedHeaderKeys = []string{
	"Content-Type",
	"Content-Language",
	"Cache-Control",
	"ETag",
	"Vary",
}

func newEntry(resp *http.Response, body []byte, pol cachePolicy, now time.Time) *entry {
	h := http.Header{}
	for _, k := range storedHeaderKeys {
		if v := resp.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	return &entry{
		header:     h,
		body:       body,
		etag:       resp.Header.Get("ETag"),
		maxAge:     pol.maxAge,
		swrEnabled: pol.swrEnabled,
		swr:        pol.swr,
		storedAt:   now,
	}
}

// writeStored 写出新鲜缓存内容；客户端校验器匹配时改为 304。
func writeStored(w http.ResponseWriter, c Clock, e *entry, label, clientINM string) {
	if etagStrongMatch(clientINM, e.etag) {
		h := w.Header()
		h.Set("ETag", e.etag)
		h.Set("Vary", "Accept-Language")
		h.Set("Cache-Control", e.header.Get("Cache-Control"))
		h.Set("X-Cache-Status", label)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	h := w.Header()
	copyStoredHeader(h, e.header)
	h.Set("X-Cache-Status", label)
	h.Set("Age", strconv.FormatInt(int64(c.Now().Sub(e.storedAt).Seconds()), 10))
	w.Header().Set("Content-Length", strconv.Itoa(len(e.body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(e.body)
}

// writeStale 写出带显式标记的过期兜底响应（始终 200，附 Warning 110）。
func writeStale(w http.ResponseWriter, c Clock, e *entry, reason string) {
	h := w.Header()
	copyStoredHeader(h, e.header)
	h.Set("X-Cache-Status", "stale")
	h.Set("X-Cache-Stale", "1")
	if reason != "" {
		h.Set("X-Cache-Error", reason)
	}
	h.Set("Warning", `110 cacheproxy "response is stale"`)
	age := int64(c.Now().Sub(e.storedAt).Seconds())
	h.Set("Age", strconv.FormatInt(age, 10))
	w.Header().Set("Content-Length", strconv.Itoa(len(e.body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(e.body)
}

func copyStoredHeader(dst, src http.Header) {
	for _, k := range storedHeaderKeys {
		if v := src.Get(k); v != "" {
			dst.Set(k, v)
		}
	}
}

func writePassthrough(w http.ResponseWriter, status int, h http.Header, body []byte, label string) {
	dst := w.Header()
	for k, vs := range h {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
	dst.Set("X-Cache-Status", label)
	if status != http.StatusNotModified {
		dst.Set("Content-Length", strconv.Itoa(len(body)))
	}
	w.WriteHeader(status)
	if status != http.StatusNotModified {
		_, _ = w.Write(body)
	}
}

// ---------- 头处理 / 读取工具 ----------

var hopByHop = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"TE", "Trailers", "Transfer-Encoding", "Upgrade",
}

// sanitizeHeaders 复制响应头并剔除逐跳头。
func sanitizeHeaders(src http.Header) http.Header {
	dst := http.Header{}
	for k, vs := range src {
		if isHop(k) {
			continue
		}
		copied := make([]string, len(vs))
		copy(copied, vs)
		dst[k] = copied
	}
	// Connection 头里列出的额外逐跳字段也要剔除。
	for _, conn := range src.Values("Connection") {
		for _, f := range strings.Split(conn, ",") {
			f = strings.TrimSpace(f)
			if f != "" {
				dst.Del(f)
			}
		}
	}
	return dst
}

func isHop(k string) bool {
	for _, h := range hopByHop {
		if strings.EqualFold(k, h) {
			return true
		}
	}
	return false
}

func cloneHeader(h http.Header) http.Header {
	c := make(http.Header, len(h))
	for k, v := range h {
		c[k] = append([]string(nil), v...)
	}
	return c
}

func readLimited(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBodyBytes+1))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(data) > maxBodyBytes {
		return data[:maxBodyBytes], errBodyTooLarge
	}
	return data, err
}

var errBodyTooLarge = errors.New("response body exceeds 1MiB limit")
