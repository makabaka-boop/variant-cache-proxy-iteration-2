package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cacheproxy/internal/lang"
	"cacheproxy/internal/origin"
)

// ---------- 白盒夹具：真实 HTTP 端到端 + 注入时钟 ----------

type swrHarness struct {
	t      *testing.T
	clock  *fakeClock
	ctrl   *origin.Controller
	origin *httptest.Server
	proxy  *httptest.Server
	p      *Proxy
}

func newSWRHarness(t *testing.T) *swrHarness {
	t.Helper()
	clk := newFakeClock()
	ctrl := origin.New()
	osrv := httptest.NewServer(ctrl.Handler())
	t.Cleanup(osrv.Close)

	p, err := New(osrv.URL, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	psrv := httptest.NewServer(p)
	t.Cleanup(psrv.Close)
	return &swrHarness{t: t, clock: clk, ctrl: ctrl, origin: osrv, proxy: psrv, p: p}
}

func (h *swrHarness) do(asset, acceptLang, ifNoneMatch string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodGet, h.proxy.URL+"/assets/"+asset, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	if acceptLang != "" {
		req.Header.Set("Accept-Language", acceptLang)
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	resp, err := h.proxy.Client().Do(req)
	if err != nil {
		h.t.Fatalf("request failed: %v", err)
	}
	return resp
}

// readResp 读出并关闭响应体。
func readResp(t *testing.T, resp *http.Response) (int, http.Header, string) {
	t.Helper()
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Clone(), string(data)
}

func (h *swrHarness) originCount(bucketStatus string) int64 {
	return h.ctrl.SnapshotStats().Status[bucketStatus]
}

func (h *swrHarness) setSpec(id string, spec origin.AssetSpec) {
	h.ctrl.SetSpec(id, spec)
}

// waitForRefreshIdle 等待该键的后台刷新轮次结束（确定性验证，不依赖真实时序）。
func (h *swrHarness) waitForRefreshIdle(key string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.p.rounds.current(key) == nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	h.t.Fatalf("background refresh for %q did not finish", key)
}

func (h *swrHarness) waitOriginAtLeast(statusKey string, want int64) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.originCount(statusKey) >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	h.t.Fatalf("origin %s count = %d, want >= %d", statusKey, h.originCount(statusKey), want)
}

func swrSpec(maxAge, swr int) origin.AssetSpec {
	n := swr
	return origin.AssetSpec{MaxAge: maxAge, SWR: &n, Version: 1, HasETag: true}
}

// ---------- 用例 ----------

// SWR 基本流程：窗口内返回显式标记的旧内容并后台 304 续期；后续请求本地命中。
func TestSWR_ServesMarkedStaleAndBackgroundRevalidates(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s1", swrSpec(10, 20))

	code, hdr, body := readResp(t, h.do("s1", "zh", ""))
	if code != 200 || hdr.Get("X-Cache-Status") != "miss" {
		t.Fatalf("prime want 200/miss, got %d/%q", code, hdr.Get("X-Cache-Status"))
	}
	if !strings.Contains(hdr.Get("Cache-Control"), "stale-while-revalidate=20") {
		t.Fatalf("response must carry the same SWR window, got %q", hdr.Get("Cache-Control"))
	}
	etag := hdr.Get("ETag")

	// 进入过期区间：第 12 秒（过期 2 秒，在 20 秒 SWR 窗口内）。
	h.clock.Advance(12 * time.Second)
	code, hdr, staleBody := readResp(t, h.do("s1", "zh", ""))
	if code != 200 || hdr.Get("X-Cache-Status") != "stale" {
		t.Fatalf("SWR want 200/stale, got %d/%q", code, hdr.Get("X-Cache-Status"))
	}
	if hdr.Get("X-Cache-Stale") != "1" || hdr.Get("Warning") == "" {
		t.Fatalf("SWR stale must carry explicit stale markers, got %+v", hdr)
	}
	if _, ok := hdr["X-Cache-Error"]; ok {
		t.Fatal("SWR stale is not an error fallback; X-Cache-Error must be absent")
	}
	if hdr.Get("Age") != "12" {
		t.Fatalf("Age want 12, got %q", hdr.Get("Age"))
	}
	if staleBody != body {
		t.Fatal("SWR must return the complete stale body")
	}

	// 后台条件回源确实发生且只有一轮（304 续期）。
	key := cacheKey("s1", lang.ZH)
	h.waitForRefreshIdle(key)
	if got := h.originCount("zh:304"); got != 1 {
		t.Fatalf("want exactly one background conditional 304, got %d", got)
	}
	// 后台请求携带的是旧条目的 ETag。
	if got := h.originCount("zh:total"); got != 2 {
		t.Fatalf("want prime + 1 background fetch, got %d", got)
	}

	// 窗口内随后的请求：缓存已被 304 续期，直接本地命中，无额外回源。
	code, hdr, _ = readResp(t, h.do("s1", "zh", ""))
	if code != 200 || hdr.Get("X-Cache-Status") != "hit" {
		t.Fatalf("post-background-304 want 200/hit, got %d/%q", code, hdr.Get("X-Cache-Status"))
	}
	if hdr.Get("ETag") != etag {
		t.Fatal("304 renewal must keep the same ETag")
	}
	if got := h.originCount("zh:total"); got != 2 {
		t.Fatalf("hit must stay local, origin total = %d", got)
	}
}

// SWR 边界：age == maxAge 立即异步；age == maxAge+swr 仍可异步；再多 1 秒恢复同步。
func TestSWR_WindowBoundaries(t *testing.T) {
	// 边界点一：age == maxAge（过期瞬间）即走 SWR，后台 304 续期。
	t.Run("at maxAge serves stale and refreshes async", func(t *testing.T) {
		h := newSWRHarness(t)
		h.setSpec("s2", swrSpec(10, 20))
		readResp(t, h.do("s2", "zh", ""))
		key := cacheKey("s2", lang.ZH)
		h.clock.Advance(10 * time.Second)
		code, hdr, _ := readResp(t, h.do("s2", "zh", ""))
		if code != 200 || hdr.Get("X-Cache-Status") != "stale" {
			t.Fatalf("at maxAge want stale, got %d/%q", code, hdr.Get("X-Cache-Status"))
		}
		h.waitForRefreshIdle(key)
		if got := h.originCount("zh:304"); got != 1 {
			t.Fatalf("want one async 304, got %d", got)
		}
	})

	// 边界点二：age == maxAge+swr（含端点）仍走 SWR。
	t.Run("at maxAge+swr still serves stale", func(t *testing.T) {
		h := newSWRHarness(t)
		h.setSpec("s2b", swrSpec(10, 20))
		readResp(t, h.do("s2b", "zh", ""))
		key := cacheKey("s2b", lang.ZH)
		h.clock.Advance(30 * time.Second)
		code, hdr, _ := readResp(t, h.do("s2b", "zh", ""))
		if code != 200 || hdr.Get("X-Cache-Status") != "stale" {
			t.Fatalf("at maxAge+swr want stale, got %d/%q", code, hdr.Get("X-Cache-Status"))
		}
		h.waitForRefreshIdle(key)
		if got := h.originCount("zh:304"); got != 1 {
			t.Fatalf("want one async 304 at boundary, got %d", got)
		}
	})

	// 边界点三：再多 1 秒越过窗口，恢复同步条件再验证（源站 304 -> revalidated）。
	t.Run("one second beyond window falls back to sync", func(t *testing.T) {
		h := newSWRHarness(t)
		h.setSpec("s2c", swrSpec(10, 20))
		readResp(t, h.do("s2c", "zh", ""))
		h.clock.Advance(31 * time.Second)
		code, hdr, _ := readResp(t, h.do("s2c", "zh", ""))
		if code != 200 || hdr.Get("X-Cache-Status") != "revalidated" {
			t.Fatalf("beyond window want sync revalidated, got %d/%q", code, hdr.Get("X-Cache-Status"))
		}
		if got := h.originCount("zh:304"); got != 1 {
			t.Fatalf("want exactly one sync 304, got %d", got)
		}
	})
}

// 同一资源同一变体：窗口内的并发请求只启动一轮后台回源，全部拿旧内容。
func TestSWR_SingleBackgroundRoundPerKey(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s3", swrSpec(10, 30))
	readResp(t, h.do("s3", "zh", ""))
	h.clock.Advance(12 * time.Second)

	// 闸门挂住后台回源，连续发起多个 SWR 请求。
	h.ctrl.CloseGate()
	const n = 5
	for i := 0; i < n; i++ {
		code, hdr, _ := readResp(t, h.do("s3", "zh", ""))
		if code != 200 || hdr.Get("X-Cache-Status") != "stale" {
			t.Fatalf("swr request %d want stale 200, got %d/%q", i, code, hdr.Get("X-Cache-Status"))
		}
	}
	// 首个请求应已触发一轮后台回源并在闸门前排队。
	h.waitOriginAtLeast("zh:total", 2)
	time.Sleep(80 * time.Millisecond)
	if got := h.originCount("zh:total"); got != 2 {
		t.Fatalf("want only one background fetch for %d SWR requests, got %d", n, got-1)
	}
	h.ctrl.OpenGate()
	h.waitForRefreshIdle(cacheKey("s3", lang.ZH))
	if got := h.originCount("zh:total"); got != 2 {
		t.Fatalf("after gate still want one background fetch, got %d", got-1)
	}
}

// 客户端 If-None-Match 不得凭陈旧条目直接 304：走同步条件回源。
func TestSWR_ClientConditionalNever304FromStale(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s4", swrSpec(10, 30))
	_, hdr, _ := readResp(t, h.do("s4", "zh", ""))
	etag := hdr.Get("ETag")
	h.clock.Advance(12 * time.Second) // 过期但在 SWR 窗口内

	// 即便客户端 ETag 与陈旧条目一致，也必须回源条件验证，由源站决定 304。
	code, rh, _ := readResp(t, h.do("s4", "zh", etag))
	if code != http.StatusNotModified {
		t.Fatalf("stale entry + matching client validator must revalidate upstream, got %d", code)
	}
	if rh.Get("X-Cache-Status") != "revalidated" {
		t.Fatalf("upstream 304 after stale want revalidated, got %q", rh.Get("X-Cache-Status"))
	}
	// 同步路径不应额外触发后台刷新。
	key := cacheKey("s4", lang.ZH)
	time.Sleep(50 * time.Millisecond)
	if h.p.rounds.current(key) != nil {
		h.waitForRefreshIdle(key)
		t.Fatal("client conditional request must not spawn a background refresh")
	}

	// 不匹配的客户端校验器：代理只携带自己缓存条目的 ETag 做条件再验证，
	// 源站 304 证明旧表示仍有效（与客户端手里是什么无关）。
	h.clock.Advance(12 * time.Second) // 续期后再次过期
	code, rh, _ = readResp(t, h.do("s4", "zh", `"nope"`))
	if code != 200 || rh.Get("X-Cache-Status") != "revalidated" {
		t.Fatalf("non-matching client validator still triggers upstream 304 renewal, got %d/%q", code, rh.Get("X-Cache-Status"))
	}
}

// 后台 200（内容已更新）替换条目；后续请求拿到新版本。
func TestSWR_Background200ReplacesEntry(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s5", swrSpec(10, 30))
	_, hdr, body1 := readResp(t, h.do("s5", "zh", ""))
	if !strings.Contains(body1, "v1") {
		t.Fatalf("prime body want v1, got %q", body1)
	}
	h.clock.Advance(12 * time.Second)

	// 源站内容升级到 v2：条件回源得到完整 200。
	h.ctrl.SetVersion("s5", 2)
	_, h2, _ := readResp(t, h.do("s5", "zh", ""))
	if h2.Get("X-Cache-Status") != "stale" {
		t.Fatalf("first SWR response still old/stale, got %q", h2.Get("X-Cache-Status"))
	}
	h.waitForRefreshIdle(cacheKey("s5", lang.ZH))
	if got := h.originCount("zh:200"); got != 2 { // prime 200 + 后台 200
		t.Fatalf("want background 200 replacement, 200-count=%d", got)
	}

	code, h3, body3 := readResp(t, h.do("s5", "zh", ""))
	if code != 200 || h3.Get("X-Cache-Status") != "hit" {
		t.Fatalf("post-replace want hit, got %d/%q", code, h3.Get("X-Cache-Status"))
	}
	if !strings.Contains(body3, "v2") {
		t.Fatalf("entry must be replaced with v2, got %q", body3)
	}
	if h3.Get("ETag") == hdr.Get("ETag") {
		t.Fatal("replacement must carry the new ETag")
	}
}

// 后台 4xx：旧条目按现有失效语义作废，下一次请求成为冷缺失。
func TestSWR_Background4xxInvalidates(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s6", swrSpec(10, 30))
	readResp(t, h.do("s6", "zh", ""))
	h.clock.Advance(12 * time.Second)

	h.ctrl.SetStatus("s6", http.StatusNotFound)
	_, hdr, _ := readResp(t, h.do("s6", "zh", ""))
	if hdr.Get("X-Cache-Status") != "stale" {
		t.Fatalf("SWR still serves stale once, got %q", hdr.Get("X-Cache-Status"))
	}
	h.waitForRefreshIdle(cacheKey("s6", lang.ZH))
	if e := h.p.lookup(cacheKey("s6", lang.ZH)); e != nil {
		t.Fatal("background 404 must invalidate the old entry")
	}

	// 清除注入后再请求：冷缺失重新 200 入缓存。
	h.ctrl.SetStatus("s6", 0)
	code, rh, _ := readResp(t, h.do("s6", "zh", ""))
	if code != 200 || rh.Get("X-Cache-Status") != "miss" {
		t.Fatalf("after invalidation want cold miss, got %d/%q", code, rh.Get("X-Cache-Status"))
	}
}

// 后台 5xx 不得抹去旧内容：条目保留，后续 SWR 请求仍能拿到旧内容并重试刷新。
func TestSWR_Background5xxKeepsStale(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s7", swrSpec(10, 30))
	_, _, body1 := readResp(t, h.do("s7", "zh", ""))
	h.clock.Advance(12 * time.Second)

	h.ctrl.SetFail("s7", true)
	_, hdr, body := readResp(t, h.do("s7", "zh", ""))
	if hdr.Get("X-Cache-Status") != "stale" || body != body1 {
		t.Fatal("SWR response must keep serving old content regardless of background failure")
	}
	h.waitForRefreshIdle(cacheKey("s7", lang.ZH))
	if got := h.originCount("zh:500"); got != 1 {
		t.Fatalf("want one background 500, got %d", got)
	}
	if e := h.p.lookup(cacheKey("s7", lang.ZH)); e == nil {
		t.Fatal("background 5xx must not erase the usable stale entry")
	}

	// 失败的一轮结束后允许重试：再次 SWR 请求触发新一轮后台刷新（仍失败），旧内容照旧。
	_, hdr2, body2 := readResp(t, h.do("s7", "zh", ""))
	if hdr2.Get("X-Cache-Status") != "stale" || body2 != body1 {
		t.Fatal("retry SWR must still serve old content")
	}
	h.waitForRefreshIdle(cacheKey("s7", lang.ZH))
	if got := h.originCount("zh:500"); got != 2 {
		t.Fatalf("want a second background attempt, 500-count=%d", got)
	}

	// 源站恢复：再来一次 SWR，后台 304 续期。
	h.ctrl.SetFail("s7", false)
	readResp(t, h.do("s7", "zh", ""))
	h.waitForRefreshIdle(cacheKey("s7", lang.ZH))
	code, rh, _ := readResp(t, h.do("s7", "zh", ""))
	if code != 200 || rh.Get("X-Cache-Status") != "hit" {
		t.Fatalf("after recovery want hit, got %d/%q", code, rh.Get("X-Cache-Status"))
	}
}

// 后台网络错误同样不得抹去旧内容。
func TestSWR_BackgroundNetworkErrorKeepsStale(t *testing.T) {
	clk := newFakeClock()

	var (
		mu       sync.Mutex
		dropConn atomic.Bool
	)
	// 自定义源站：正常时按 zh/en + 版本应答；dropConn 时直接关连接制造网络错误。
	osrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		drop := dropConn.Load()
		mu.Unlock()
		if drop {
			conn, bufrw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			_ = bufrw.Flush()
			_ = conn.Close()
			return
		}
		variant, _ := lang.Parse(r.Header.Get("Accept-Language"))
		h := w.Header()
		h.Set("Vary", "Accept-Language")
		h.Set("Content-Language", string(variant))
		h.Set("Content-Type", "text/plain; charset=utf-8")
		h.Set("Cache-Control", "public, max-age=10, stale-while-revalidate=30")
		etag := fmt.Sprintf("%q", "s8-"+string(variant)+"-v1")
		h.Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "asset=s8 lang="+string(variant)+" v1\n")
	}))
	t.Cleanup(osrv.Close)

	p, err := New(osrv.URL, WithClock(clk), WithClient(&http.Client{Timeout: 2 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	psrv := httptest.NewServer(p)
	t.Cleanup(psrv.Close)

	get := func() *http.Response {
		req, _ := http.NewRequest(http.MethodGet, psrv.URL+"/assets/s8", nil)
		req.Header.Set("Accept-Language", "zh")
		resp, err := psrv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	_, hdr, body1 := readResp(t, get())
	if hdr.Get("X-Cache-Status") != "miss" {
		t.Fatalf("prime want miss, got %q", hdr.Get("X-Cache-Status"))
	}
	clk.Advance(12 * time.Second)

	dropConn.Store(true)
	_, hdr2, body2 := readResp(t, get())
	if hdr2.Get("X-Cache-Status") != "stale" || body2 != body1 {
		t.Fatal("network error during SWR must still return the old content")
	}
	key := cacheKey("s8", lang.ZH)
	// 等待后台刷新（连接被立即关闭）结束。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p.rounds.current(key) == nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if e := p.lookup(key); e == nil {
		t.Fatal("network error must not erase the stale entry")
	}

	// 恢复后：新的 SWR 请求触发后台 304，随后本地命中。
	dropConn.Store(false)
	readResp(t, get())
	for time.Now().Before(deadline) {
		if p.rounds.current(key) == nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	code, h3, _ := readResp(t, get())
	if code != 200 || h3.Get("X-Cache-Status") != "hit" {
		t.Fatalf("recovery want hit, got %d/%q", code, h3.Get("X-Cache-Status"))
	}
}

// 双语言隔离：SWR 刷新与陈旧内容按变体独立，互不串用、互不合并。
func TestSWR_DualLanguageIsolation(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s9", swrSpec(10, 30))
	_, zhH, zhBody := readResp(t, h.do("s9", "zh", ""))
	_, enH, enBody := readResp(t, h.do("s9", "en", ""))
	if zhBody == enBody || zhH.Get("ETag") == enH.Get("ETag") {
		t.Fatal("zh/en must have independent bodies and ETags")
	}
	h.clock.Advance(12 * time.Second)

	// 闸门挂住：zh 与 en 的 SWR 请求各启动一轮后台刷新，源站在飞峰值为 2。
	h.ctrl.CloseGate()
	_, zhdr, _ := readResp(t, h.do("s9", "zh", ""))
	_, enhdr, enStale := readResp(t, h.do("s9", "en", ""))
	if zhdr.Get("X-Cache-Status") != "stale" || enhdr.Get("X-Cache-Status") != "stale" {
		t.Fatalf("both variants want stale markers, got %q %q", zhdr.Get("X-Cache-Status"), enhdr.Get("X-Cache-Status"))
	}
	if !strings.Contains(enStale, "lang=en") {
		t.Fatalf("en SWR must never serve zh content: %q", enStale)
	}
	h.waitOriginAtLeast("zh:total", 2)
	h.waitOriginAtLeast("en:total", 2)
	time.Sleep(80 * time.Millisecond)
	if h.ctrl.SnapshotStats().MaxInFlight != 2 {
		t.Fatalf("per-language background refreshes want maxInFlight 2, got %d", h.ctrl.SnapshotStats().MaxInFlight)
	}
	if got := h.originCount("zh:total"); got != 2 {
		t.Fatalf("zh wants 2 origin hits, got %d", got)
	}
	if got := h.originCount("en:total"); got != 2 {
		t.Fatalf("en wants 2 origin hits, got %d", got)
	}
	h.ctrl.OpenGate()
	h.waitForRefreshIdle(cacheKey("s9", lang.ZH))
	h.waitForRefreshIdle(cacheKey("s9", lang.EN))

	// 两个变体都续期到同一时刻。先推进到再次过期，让 en 的后台 304 先完成，
	// 确认 en 独立保留自己的 v1；之后才升级 zh 并只刷新 zh。
	h.clock.Advance(12 * time.Second)
	readResp(t, h.do("s9", "en", ""))
	h.waitForRefreshIdle(cacheKey("s9", lang.EN))
	_, _, enKept := readResp(t, h.do("s9", "en", ""))
	if !strings.Contains(enKept, "v1") || !strings.Contains(enKept, "lang=en") {
		t.Fatalf("en must keep its own v1: %q", enKept)
	}

	h.ctrl.SetVersion("s9", 2)
	readResp(t, h.do("s9", "zh", ""))
	h.waitForRefreshIdle(cacheKey("s9", lang.ZH))

	_, _, zhNow := readResp(t, h.do("s9", "zh", ""))
	_, _, enNow := readResp(t, h.do("s9", "en", ""))
	if !strings.Contains(zhNow, "v2") || !strings.Contains(zhNow, "lang=zh") {
		t.Fatalf("zh must be upgraded to v2 zh content: %q", zhNow)
	}
	if !strings.Contains(enNow, "v1") || !strings.Contains(enNow, "lang=en") {
		t.Fatalf("en must keep its own v1 content: %q", enNow)
	}
}

// 未声明 SWR 的资源：过期后维持原有同步再验证；5xx 时维持 30 秒故障兜底。
func TestSWR_NoDirectiveKeepsLegacyBehavior(t *testing.T) {
	h := newSWRHarness(t)
	// 默认 spec 不带 SWR。
	h.setSpec("s10", origin.AssetSpec{MaxAge: 10, HasETag: true, Version: 1})
	readResp(t, h.do("s10", "zh", ""))
	h.clock.Advance(12 * time.Second)

	// 5xx：同步回源失败，走旧的 stale 兜底（带 X-Cache-Error），不产生“立即返回+后台刷新”。
	h.ctrl.SetFail("s10", true)
	code, hdr, _ := readResp(t, h.do("s10", "zh", ""))
	if code != 200 || hdr.Get("X-Cache-Status") != "stale" {
		t.Fatalf("legacy failure want stale fallback, got %d/%q", code, hdr.Get("X-Cache-Status"))
	}
	if hdr.Get("X-Cache-Error") == "" {
		t.Fatal("legacy stale fallback must carry X-Cache-Error")
	}
	time.Sleep(50 * time.Millisecond)
	if r := h.p.rounds.current(cacheKey("s10", lang.ZH)); r != nil {
		t.Fatal("resources without SWR directive must never start background refresh")
	}

	// 恢复：同步 304 续期。
	h.ctrl.SetFail("s10", false)
	code, hdr, _ = readResp(t, h.do("s10", "zh", ""))
	if code != 200 || hdr.Get("X-Cache-Status") != "revalidated" {
		t.Fatalf("legacy sync revalidation want revalidated, got %d/%q", code, hdr.Get("X-Cache-Status"))
	}
}

// 迟到提交：后台刷新结果提交前若缓存条目已被更新，旧结果必须丢弃，不得回退。
func TestSWR_LateBackgroundResultDoesNotOverwrite(t *testing.T) {
	clk := newFakeClock()

	var (
		version   = 1
		blockSWR  atomic.Bool // 挂起带“非当前 ETag”的条件请求（迟到的后台轮次）
		proceedCh = make(chan struct{})
		versionMu sync.Mutex
	)

	getVersion := func() int { versionMu.Lock(); defer versionMu.Unlock(); return version }
	setVersion := func(v int) { versionMu.Lock(); version = v; versionMu.Unlock() }

	osrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		variant, _ := lang.Parse(r.Header.Get("Accept-Language"))
		v := getVersion()
		etag := fmt.Sprintf("%q", fmt.Sprintf("s11-%s-v%d", variant, v))
		h := w.Header()
		h.Set("Vary", "Accept-Language")
		h.Set("Content-Language", string(variant))
		h.Set("Content-Type", "text/plain; charset=utf-8")
		h.Set("Cache-Control", "public, max-age=10, stale-while-revalidate=30")
		h.Set("ETag", etag)

		// 挂起期间的任何请求都阻塞（后台轮次持旧 ETag 到达即被测试挂住），
		// 放行后再按“此刻”的源站版本应答。
		if blockSWR.Load() {
			<-proceedCh
		}

		if inm := r.Header.Get("If-None-Match"); inm != "" && inm == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, fmt.Sprintf("asset=s11 lang=%s v%d\n", variant, v))
	}))
	t.Cleanup(osrv.Close)

	p, err := New(osrv.URL, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	psrv := httptest.NewServer(p)
	t.Cleanup(psrv.Close)
	key := cacheKey("s11", lang.ZH)

	syncGet := func() {
		req, _ := http.NewRequest(http.MethodGet, psrv.URL+"/assets/s11", nil)
		req.Header.Set("Accept-Language", "zh")
		resp, err := psrv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	// 1) 预热 v1。
	syncGet()
	base := p.lookup(key)
	if base == nil || !strings.Contains(string(base.body), "v1") {
		t.Fatal("prime must cache v1")
	}

	// 2) 过期进窗口，挂起后台刷新，让它持旧 ETag 卡在源站。
	clk.Advance(12 * time.Second)
	blockSWR.Store(true)
	syncGet() // SWR：返回 v1 旧内容，后台轮次挂起
	waitUntil := func(cond func() bool) {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		t.Fatal("condition timeout")
	}
	waitUntil(func() bool { return p.rounds.current(key) != nil })

	// 3) 缓存条目在后台轮次挂起期间被另一条路径更新为 v2（直接提交新条目，
	//    等价于同步再验证/另一次替换先完成）。
	clk.Advance(time.Second)
	setVersion(2)
	v2 := &entry{
		header:     cloneHeader(base.header),
		body:       []byte("asset=s11 lang=zh v2\n"),
		etag:       `"s11-zh-v2"`,
		maxAge:     10,
		swrEnabled: true,
		swr:        30 * time.Second,
		storedAt:   clk.Now(),
	}
	v2.header.Set("ETag", v2.etag)
	p.put(key, v2)

	// 4) 放行被挂起的后台轮次：源站对旧 ETag 回 200 v2 —— 但无论它拿到什么，
	//    提交时发现 base 已不是当前条目，必须整体丢弃。
	close(proceedCh)
	waitUntil(func() bool { return p.rounds.current(key) == nil })

	cur := p.lookup(key)
	if cur != v2 {
		t.Fatalf("late background result must not replace the current entry; cur body=%q", cur.body)
	}
	if string(cur.body) != "asset=s11 lang=zh v2\n" || cur.etag != `"s11-zh-v2"` {
		t.Fatalf("cache must remain v2, got body=%q etag=%q", cur.body, cur.etag)
	}
}

// 白盒补充：commitIfCurrent / removeIfCurrent 的指针守卫本身覆盖
// 304 续期（decRenewed）与 4xx 失效（decInvalid）两条提交路径——
// 当前条目不是发起刷新时的依据时，续期不得回写、作废不得删除。
func TestSWR_CommitGuardCoversRenewAndInvalidate(t *testing.T) {
	clk := newFakeClock()
	p := &Proxy{clock: clk, store: map[string]*entry{}, rounds: newRoundGroup()}
	key := cacheKey("sX", lang.ZH)
	base := &entry{etag: `"old"`, maxAge: 10, storedAt: clk.Now()}
	next := &entry{etag: `"new"`, maxAge: 10, storedAt: clk.Now()}

	// 条目已前进到 next：基于 base 的 304 续期提交必须失败，next 原样保留。
	p.store[key] = next
	if p.commitIfCurrent(key, base, &entry{etag: `"old-renewed"`}) {
		t.Fatal("renewal against a stale base must be rejected")
	}
	if p.lookup(key) != next {
		t.Fatal("current entry must remain untouched after rejected renewal")
	}
	if p.removeIfCurrent(key, base) {
		t.Fatal("invalidation against a stale base must be rejected")
	}
	if p.lookup(key) != next {
		t.Fatal("current entry must not be removed by a stale invalidation")
	}

	// 条目仍是 base 时两条路径都生效。
	p.store[key] = base
	if !p.commitIfCurrent(key, base, next) || p.lookup(key) != next {
		t.Fatal("matching-base renewal must commit")
	}
	p.store[key] = base
	if !p.removeIfCurrent(key, base) || p.lookup(key) != nil {
		t.Fatal("matching-base invalidation must remove")
	}
}

// 同步回源结论迟到（并发后台刷新已推进条目）时，applyDecision 必须让位，
// 绝不能用本轮基于旧条目的续期/作废覆盖更新后的内容。
func TestSWR_SyncApplyDefersToNewerEntry(t *testing.T) {
	clk := newFakeClock()
	p := &Proxy{clock: clk, store: map[string]*entry{}, rounds: newRoundGroup()}
	key := cacheKey("sY", lang.ZH)
	base := &entry{
		header: http.Header{}, body: []byte("old"), etag: `"old"`,
		maxAge: 10, storedAt: clk.Now(),
	}
	fresh := &entry{
		header: http.Header{}, body: []byte("fresh"), etag: `"fresh"`,
		maxAge: 10, storedAt: clk.Now(),
	}

	// 同步轮次基于 base 得到 304 续期；提交前缓存已是别的条目 fresh。
	p.store[key] = fresh
	out := p.applyDecision(key, base, decision{kind: decRenewed, entry: &entry{etag: `"old-renewed"`}})
	if p.lookup(key) != fresh {
		t.Fatal("sync renewal must not overwrite a concurrently advanced entry")
	}
	if out.kind != ocStored || out.entry != fresh {
		t.Fatalf("sync path must serve the current entry, got kind=%d entry=%v", out.kind, out.entry)
	}

	// 同步轮次基于 base 得到可缓存 200 替换：同样让位。
	replacement := &entry{body: []byte("replaced")}
	out = p.applyDecision(key, base, decision{kind: decReplaced, entry: replacement})
	if p.lookup(key) != fresh {
		t.Fatal("sync replacement must not overwrite the newer entry")
	}

	// 同步轮次基于 base 得到 4xx：不得删除已更新的条目。
	out = p.applyDecision(key, base, decision{kind: decInvalid, status: http.StatusNotFound})
	if p.lookup(key) != fresh {
		t.Fatal("sync 4xx against a stale base must not remove the newer entry")
	}
	if out.kind != ocStored || out.entry != fresh {
		t.Fatal("stale-base invalidation must defer to the current fresh entry")
	}
}

// 触发 SWR 的客户端连接结束（响应已返回即关闭）后，被闸门挂住的后台刷新
// 仍继续执行并提交续期；后台刷新是其他客户端共享的，不依附任何单个请求。
func TestSWR_BackgroundRefreshOutlivesClientAndShared(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s13", swrSpec(10, 30))
	readResp(t, h.do("s13", "zh", ""))
	h.clock.Advance(12 * time.Second)

	h.ctrl.CloseGate()
	// 触发请求：拿到 stale 旧内容后响应即结束（readResp 关闭了连接/上下文）。
	_, hdr, _ := readResp(t, h.do("s13", "zh", ""))
	if hdr.Get("X-Cache-Status") != "stale" {
		t.Fatalf("want stale, got %q", hdr.Get("X-Cache-Status"))
	}
	key := cacheKey("s13", lang.ZH)
	// 后台回源此刻被挂在闸门上（触发它的客户端已经走了）。
	h.waitOriginAtLeast("zh:total", 2)

	// 窗口内另一个客户端加入：不得开第二轮，直接复用旧内容。
	_, hdr2, _ := readResp(t, h.do("s13", "zh", ""))
	if hdr2.Get("X-Cache-Status") != "stale" {
		t.Fatalf("second client must reuse the single shared refresh, got %q", hdr2.Get("X-Cache-Status"))
	}
	time.Sleep(80 * time.Millisecond)
	if got := h.originCount("zh:total"); got != 2 {
		t.Fatalf("shared refresh must stay single while gated, got %d", got)
	}

	h.ctrl.OpenGate()
	h.waitForRefreshIdle(key) // 没有任何客户端在等，它仍自行完成
	if got := h.originCount("zh:304"); got != 1 {
		t.Fatalf("detached refresh must complete its 304, got %d", got)
	}
	code, h3, _ := readResp(t, h.do("s13", "zh", ""))
	if code != 200 || h3.Get("X-Cache-Status") != "hit" {
		t.Fatalf("detached refresh must populate cache for later clients, got %d/%q", code, h3.Get("X-Cache-Status"))
	}
}

// 请求取消不中断其他客户端共享的刷新：SWR 后台刷新脱离客户端生命周期；
// 窗口外的同步合并轮次里，某个请求取消同样不影响共享回源与其他等待者。
func TestSWR_CancellationDoesNotKillSharedSyncRound(t *testing.T) {
	h := newSWRHarness(t)
	h.setSpec("s12", swrSpec(10, 30))
	readResp(t, h.do("s12", "zh", ""))
	h.clock.Advance(41 * time.Second) // 超出 SWR 窗口，走同步再验证
	h.ctrl.CloseGate()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ch1 := make(chan int, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx1, http.MethodGet, h.proxy.URL+"/assets/s12", nil)
		req.Header.Set("Accept-Language", "zh")
		resp, err := h.proxy.Client().Do(req)
		if err != nil {
			ch1 <- 0
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		ch1 <- resp.StatusCode
	}()
	h.waitOriginAtLeast("zh:total", 2)
	cancel1()
	<-ch1
	time.Sleep(50 * time.Millisecond)
	if got := h.originCount("zh:total"); got != 2 {
		t.Fatalf("canceled sync requester must not abort the shared round, total=%d", got)
	}

	// 第二个请求加入同一同步轮次；放行后正常拿到 304 续期的内容。
	ch2 := make(chan string, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, h.proxy.URL+"/assets/s12", nil)
		req.Header.Set("Accept-Language", "zh")
		resp, err := h.proxy.Client().Do(req)
		if err != nil {
			ch2 <- ""
			return
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		ch2 <- string(data)
	}()
	time.Sleep(80 * time.Millisecond)
	h.ctrl.OpenGate()
	body := <-ch2
	if !strings.Contains(body, "lang=zh") {
		t.Fatalf("surviving shared round must deliver zh content, got %q", body)
	}
}
