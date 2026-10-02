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
//   - 同键并发请求通过 flightGroup 合并为一次回源；等待者取消不影响他人；
//   - 回源遇到网络错误或 5xx 时，可返回已过期但不超过 30 秒的旧响应，
//     并显式打上 X-Cache-Status: stale 与 Warning 110 标记；
//   - 源站显式声明 stale-while-revalidate=N 时，过期但仍在该窗口内的请求
//     先立即返回带陈旧标记的完整旧内容，同时只启动一轮后台条件回源；
//     客户端 If-None-Match 不享受该模式，仍走同步条件再验证；后台结果
//     必须通过 CAS 提交，迟到刷新不会覆盖已被更新的条目；
//   - 客户端 If-None-Match 只在正确变体的缓存条目上校验，跨语言不可能 304。
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
	origin  *url.URL
	client  *http.Client
	clock   Clock
	mu      sync.RWMutex
	store   map[string]*entry
	flights *flightGroup
}

// entry 是一条按 (资源, 变体) 存储的缓存条目。
type entry struct {
	header   http.Header // 仅保留可安全复用的响应头
	body     []byte
	etag     string
	maxAge   int
	storedAt time.Time
	// swrSet/swr 记录源站在产生此条目时是否显式声明了
	// stale-while-revalidate 窗口及其秒数；只有显式声明才启用后台更新模式。
	swrSet bool
	swr    int
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

// withinSWR 判断过期条目是否仍处于源站显式声明的 stale-while-revalidate
// 窗口内（含 maxAge+swr 整边界；显式声明 swr=0 时仅 age==maxAge 这一刻成立）。
func (p *Proxy) withinSWR(e *entry) bool {
	if !e.swrSet {
		return false
	}
	a := p.age(e)
	return a >= time.Duration(e.maxAge)*time.Second &&
		a <= time.Duration(e.maxAge)*time.Second+time.Duration(e.swr)*time.Second
}

// New 创建代理，originBase 为源站基址（如 http://origin:8081）。
func New(originBase string, opts ...Option) (*Proxy, error) {
	u, err := url.Parse(originBase)
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		origin:  u,
		client:  &http.Client{Timeout: 15 * time.Second},
		clock:   systemClock{},
		store:   map[string]*entry{},
		flights: newFlightGroup(),
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

	var cached *entry
	if e := p.lookup(key); e != nil {
		cached = e
		if p.fresh(e) {
			writeStored(w, p.clock, e, "hit", clientINM)
			return
		}
	}

	if cached == nil && clientINM != "" {
		// 冷路径上的客户端条件请求：本地没有“正确变体”的表示可供校验，
		// 而响应取决于客户端持有的具体校验器，故直接透传（不做合并）；
		// 若源站给出可缓存的 200，仍然写入共享缓存。
		p.forwardColdConditional(w, r, key)
		return
	}

	// 过期条目 + 源站显式启用 stale-while-revalidate + 非条件请求：
	// 立即返回完整旧内容（显式陈旧标记），同时只启动一轮后台条件回源。
	// 客户端携带 If-None-Match 时绝不走这条路——不允许凭陈旧条目直接 304。
	if cached != nil && clientINM == "" && p.withinSWR(cached) {
		p.serveStaleWhileRevalidate(w, r, key, id, variant, cached)
		return
	}

	// 冷缺失，或未声明 SWR / 超出 SWR 窗口，或客户端条件请求：同步再验证。
	p.serveSync(w, r, key, id, variant, clientINM)
}

// serveSync 处理冷缺失与同步再验证：同键并发合并为一次回源。
// leader 使用脱离客户端生命周期的上下文完成回源；等待者可随时取消，
// 取消只影响自己，不影响 leader 与其他等待者。
func (p *Proxy) serveSync(w http.ResponseWriter, r *http.Request, key, id string, variant lang.Variant, clientINM string) {
	f, leader, ok := p.flights.Do(key)
	if !ok {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if leader {
		var out outcome
		// Double-check：迟到的 leader 可能发现缓存已被冷条件透传路径填充。
		if e := p.lookup(key); e != nil && p.fresh(e) {
			out = outcome{kind: ocStored, entry: e, label: "hit"}
		} else {
			// 关键：回源上下文脱离客户端生命周期。某个等待者（甚至发起者）
			// 取消请求都不会中断这次回源；即便发起者断开，本协程仍会等回源
			// 完成并发布结果，保证其他等待者与后来加入的请求不会被挂死。
			ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
			out = p.fetch(ctx, key, id, variant, p.lookup(key))
			cancel()
		}
		f.setOutcome(out)        // 先发布结果唤醒等待者
		p.flights.Finish(key, f) // 再拆键，避免迟到者开启重复回源
		if r.Context().Err() != nil {
			return // 发起者已断开，结果留给等待者
		}
		writeOutcome(w, p.clock, out, clientINM)
		return
	}

	if !f.Wait(r.Context().Done()) {
		// 等待者主动放弃：leader 与其他等待者不受影响。
		return
	}

	// 客户端可能在结果就绪前后断开；已断开则不要再写响应。
	if r.Context().Err() != nil {
		return
	}
	writeOutcome(w, p.clock, f.outcome(), clientINM)
}

// serveStaleWhileRevalidate 处理 SWR 窗口内的过期非条件请求：立即给出带
// 明确陈旧标记的完整旧内容，并确保同一资源、同一语言只有一轮后台条件回源。
// 成为等待者的请求同样立即拿到旧内容——后台结果不阻塞任何 SWR 客户端。
func (p *Proxy) serveStaleWhileRevalidate(w http.ResponseWriter, r *http.Request, key, id string, variant lang.Variant, e *entry) {
	f, leader, _ := p.flights.Do(key)
	if !leader {
		// 已有一轮后台刷新在跑：不新建，立即返回陈旧内容。
		writeStale(w, p.clock, e, "")
		return
	}

	// 成为 leader 后、启动后台协程前再复核一次：Do 与发起请求之间是有窗口的，
	// 条目可能恰好在此刻被其他路径（如同步回源）更新。当前条目仍有效就直接
	// 作答；否则不启动后台刷新，拆键并退回常规路径重新判定。
	if cur := p.lookup(key); cur != e {
		p.flights.Finish(key, f)
		if cur != nil && p.fresh(cur) {
			writeStored(w, p.clock, cur, "hit", "")
			return
		}
		p.serveSync(w, r, key, id, variant, "")
		return
	}

	// 后台刷新脱离发起它的客户端：该请求写完陈旧响应即结束，
	// 但回源仍会跑完并发布，共享给同步等待者与后续请求。
	go p.runBackgroundRefresh(key, id, variant, e, f)
	// 客户端取消只影响自己的响应写出，不中断共享刷新。
	writeStale(w, p.clock, e, "")
}

// runBackgroundRefresh 在独立协程里完成一轮后台条件回源，并通过 CAS 守卫
// 提交结果：只有回源对应的条目仍是当前缓存条目时才允许写入，迟到刷新
// （条目已被其他路径更新）的结果一律丢弃。
func (p *Proxy) runBackgroundRefresh(key, id string, variant lang.Variant, base *entry, f *flight) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 提交前守卫（第一道）：启动前再次确认条目仍是发起本轮时的条目。
	if cur := p.lookup(key); cur == nil || cur != base {
		out := outcome{kind: ocError, reason: "superseded"}
		if cur != nil {
			out = outcome{kind: ocStored, entry: cur, label: "hit"}
		}
		f.setOutcome(out)
		p.flights.Finish(key, f)
		return
	}

	out := p.fetch(ctx, key, id, variant, base)
	f.setOutcome(out)        // 先唤醒同步等待者（如加入的条件请求）
	p.flights.Finish(key, f) // 再拆键
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

// putIfAbsent 仅在键不存在时写入，返回是否写入成功。冷缺失回源用它避免
// 覆盖并发路径（如冷条件透传）已建立的条目。
func (p *Proxy) putIfAbsent(key string, e *entry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.store[key]; exists {
		return false
	}
	p.store[key] = e
	return true
}

// swapIf 仅在当前条目仍是 old 时，把它替换为 new（new 为 nil 表示删除）。
// 这是后台刷新的提交守卫：回源基于某个旧条目发出，提交时若缓存条目已被
// 更新，则本轮结果已过时，必须丢弃，不能把旧内容重新写成新版本。
// 返回提交是否生效以及当前实际条目。
func (p *Proxy) swapIf(key string, old, new *entry) (*entry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur, exists := p.store[key]
	if !exists || cur != old {
		return cur, false
	}
	if new == nil {
		delete(p.store, key)
	} else {
		p.store[key] = new
	}
	return new, true
}

// ---------- 回源 ----------

// fetch 执行一次回源。existing 非 nil 时表示持有过期条目，做条件再验证。
//
// 提交规则保证后台刷新不会用迟到结果覆盖更新后的条目：
//   - 304 续期 / 可缓存 200：仅当当前条目仍是发起回源时的 existing 才写入；
//   - 4xx 等否定响应：仅在条目仍是 existing 时作废旧条目；
//   - 5xx / 网络错误：不动缓存条目，交给陈旧兜底；
//   - existing 为 nil（冷缺失）时，用 putIfAbsent 避免覆盖并发填充。
func (p *Proxy) fetch(ctx context.Context, key, id string, variant lang.Variant, existing *entry) outcome {
	req := p.buildOriginRequest(ctx, rAsset{id: id, variant: variant})
	if existing != nil {
		req.Header.Set("If-None-Match", existing.etag)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return p.failureOutcome(existing, "origin-network-error")
	}
	defer resp.Body.Close()
	body, readErr := readLimited(resp.Body)

	switch {
	case resp.StatusCode == http.StatusNotModified:
		if existing == nil {
			// 冷路径不应得到 304；原样透传。
			return bypassOutcome(resp, body)
		}
		if readErr != nil {
			return p.failureOutcome(existing, "origin-read-error")
		}
		// 304：以原 max-age 刷新新鲜期（延长原响应的新鲜期）。
		// 源站若在 304 上更新了 ETag/缓存头，一并采用。
		refreshed := *existing
		refreshed.header = cloneHeader(existing.header)
		if etag := resp.Header.Get("ETag"); etag != "" {
			refreshed.etag = etag
			refreshed.header.Set("ETag", etag)
		}
		if cc := resp.Header.Get("Cache-Control"); cc != "" {
			refreshed.header.Set("Cache-Control", cc)
		}
		if ttl, ok := cacheable(refreshed.header); ok {
			refreshed.maxAge = ttl
		}
		// 304 头里的 SWR 指令若显式出现则更新；未出现则沿用原条目窗口
		// （304 仅更新被携带的元数据）。
		if swr, present := staleWhileRevalidate(refreshed.header); present {
			refreshed.swrSet = true
			refreshed.swr = swr
		}
		refreshed.storedAt = p.clock.Now()
		if cur, committed := p.swapIf(key, existing, &refreshed); committed {
			return outcome{kind: ocStored, entry: &refreshed, label: "revalidated"}
		} else {
			// 提交守卫：条目已被更新，丢弃本轮迟到续期。
			return p.supersededOutcome(cur)
		}

	case resp.StatusCode == http.StatusOK:
		if readErr != nil {
			return p.failureOutcome(existing, "origin-read-error")
		}
		if ttl, ok := cacheable(resp.Header); ok {
			swr, swrPresent := staleWhileRevalidate(resp.Header)
			e := newEntry(resp, body, ttl, p.clock.Now())
			e.swrSet = swrPresent
			e.swr = swr
			if existing == nil {
				if !p.putIfAbsent(key, e) {
					// 冷回源期间条目已被并发填充：不覆盖，直接复用当前条目。
					if cur := p.lookup(key); cur != nil {
						return outcome{kind: ocStored, entry: cur, label: "hit"}
					}
					p.put(key, e)
				}
				return outcome{kind: ocStored, entry: e, label: "miss"}
			}
			if cur, committed := p.swapIf(key, existing, e); committed {
				return outcome{kind: ocStored, entry: e, label: "miss"}
			} else {
				// 迟到刷新：旧版本不得覆盖已更新的当前条目。
				return p.supersededOutcome(cur)
			}
		}
		// 源站已不再允许缓存此表示：仅当条目仍是 existing 时作废。
		if existing != nil {
			p.swapIf(key, existing, nil)
		}
		return bypassOutcome(resp, body)

	case resp.StatusCode >= 500:
		// 5xx 不删除旧条目：在兜底窗口内仍可服务过期内容；
		// 已进入 SWR 窗口的条目同样保持不变，后台刷新失败不抹去可用旧内容
		// （后续 SWR 请求仍能返回它，见 serveStaleWhileRevalidate）。
		return p.failureOutcome(existing, "origin-"+strconv.Itoa(resp.StatusCode))

	default:
		// 4xx 等：源站否定了该资源，旧表示按现有失效语义作废；
		// 但仍以提交守卫确认它还是发起回源时的那条，避免误删更新后的条目。
		if existing != nil {
			p.swapIf(key, existing, nil)
		}
		if readErr != nil {
			return p.failureOutcome(existing, "origin-read-error")
		}
		return bypassOutcome(resp, body)
	}
}

// supersededOutcome 构造“迟到刷新被提交守卫拒绝”后的结果：
// 当前条目若仍可用则交给调用方（绝不用旧内容覆盖它），否则视为内部失效。
func (p *Proxy) supersededOutcome(cur *entry) outcome {
	if cur == nil {
		return outcome{kind: ocError, reason: "superseded"}
	}
	return outcome{kind: ocStored, entry: cur, label: "hit"}
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
		if ttl, ok := cacheable(resp.Header); ok {
			e := newEntry(resp, body, ttl, p.clock.Now())
			if swr, present := staleWhileRevalidate(resp.Header); present {
				e.swrSet = true
				e.swr = swr
			}
			// 与并发回源赛跑时不覆盖已建立的条目。
			if p.putIfAbsent(key, e) {
				writeStored(w, p.clock, e, "miss", "")
			} else if cur := p.lookup(key); cur != nil {
				writeStored(w, p.clock, cur, "hit", "")
			} else {
				p.put(key, e)
				writeStored(w, p.clock, e, "miss", "")
			}
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

func bypassOutcome(resp *http.Response, body []byte) outcome {
	return outcome{
		kind:   ocBypass,
		status: resp.StatusCode,
		header: sanitizeHeaders(resp.Header),
		body:   body,
	}
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

func newEntry(resp *http.Response, body []byte, ttl int, now time.Time) *entry {
	h := http.Header{}
	for _, k := range storedHeaderKeys {
		if v := resp.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	return &entry{
		header:   h,
		body:     body,
		etag:     resp.Header.Get("ETag"),
		maxAge:   ttl,
		storedAt: now,
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
