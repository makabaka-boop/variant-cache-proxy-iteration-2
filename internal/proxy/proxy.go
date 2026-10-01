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

	// 冷缺失或过期再验证：同键并发合并为一次回源。
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

func (p *Proxy) remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.store, key)
}

// ---------- 回源 ----------

// fetch 执行一次回源。existing 非 nil 时表示持有过期条目，做条件再验证。
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
		refreshed.storedAt = p.clock.Now()
		p.put(key, &refreshed)
		return outcome{kind: ocStored, entry: &refreshed, label: "revalidated"}

	case resp.StatusCode == http.StatusOK:
		if readErr != nil {
			return p.failureOutcome(existing, "origin-read-error")
		}
		if ttl, ok := cacheable(resp.Header); ok {
			e := newEntry(resp, body, ttl, p.clock.Now())
			p.put(key, e)
			return outcome{kind: ocStored, entry: e, label: "miss"}
		}
		if existing != nil {
			p.remove(key) // 源站已不再允许缓存此表示
		}
		return bypassOutcome(resp, body)

	case resp.StatusCode >= 500:
		// 5xx 不删除旧条目：在兜底窗口内仍可服务过期内容。
		return p.failureOutcome(existing, "origin-"+strconv.Itoa(resp.StatusCode))

	default:
		// 4xx 等：源站否定了该资源，旧表示必须作废，响应透传。
		if existing != nil {
			p.remove(key)
		}
		if readErr != nil {
			return p.failureOutcome(existing, "origin-read-error")
		}
		return bypassOutcome(resp, body)
	}
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
