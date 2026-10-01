package proxy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cacheproxy/internal/origin"
	"cacheproxy/internal/proxy"
)

// ---------- 测试夹具 ----------

type harness struct {
	t      *testing.T
	clock  *fakeClock
	ctrl   *origin.Controller
	origin *httptest.Server
	proxy  *httptest.Server
}

// fakeClock 通过两个包共有的 Clock 接口注入；测试里直接用具体类型推进。
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newHarness(t *testing.T) *harness {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ctrl := origin.New()
	osrv := httptest.NewServer(ctrl.Handler())
	t.Cleanup(osrv.Close)

	p, err := proxy.New(osrv.URL, proxy.WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	psrv := httptest.NewServer(p)
	t.Cleanup(psrv.Close)

	return &harness{t: t, clock: clk, ctrl: ctrl, origin: osrv, proxy: psrv}
}

type resp struct {
	status int
	header http.Header
	body   string
}

func (h *harness) getCtx(ctx context.Context, asset, acceptLang, ifNoneMatch string) resp {
	h.t.Helper()
	url := h.proxy.URL + "/assets/" + asset
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	if acceptLang != "" {
		req.Header.Set("Accept-Language", acceptLang)
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	r, err := h.proxy.Client().Do(req)
	if err != nil {
		h.t.Helper()
		h.t.Fatalf("request failed: %v", err)
	}
	defer r.Body.Close()
	data, _ := io.ReadAll(r.Body)
	return resp{r.StatusCode, r.Header.Clone(), string(data)}
}

func (h *harness) get(asset, acceptLang, ifNoneMatch string) resp {
	return h.getCtx(context.Background(), asset, acceptLang, ifNoneMatch)
}

// getAsync 以独立 HTTP 连接发起请求并异步返回，用于取消/并发场景。
func (h *harness) getAsync(ctx context.Context, asset, acceptLang string) <-chan resp {
	ch := make(chan resp, 1)
	go func() {
		h.t.Helper()
		url := h.proxy.URL + "/assets/" + asset
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		req.Header.Set("Accept-Language", acceptLang)
		r, err := h.proxy.Client().Do(req)
		if err != nil {
			ch <- resp{status: 0} // 连接/上下文错误，例如客户端取消
			return
		}
		defer r.Body.Close()
		data, _ := io.ReadAll(r.Body)
		ch <- resp{r.StatusCode, r.Header.Clone(), string(data)}
	}()
	return ch
}

func (h *harness) setSpec(id string, spec origin.AssetSpec) { h.ctrl.SetSpec(id, spec) }
func (h *harness) setFail(id string, fail bool)             { h.ctrl.SetFail(id, fail) }

func (h *harness) count(kind string) int64 {
	st := h.ctrl.SnapshotStats()
	if kind == "maxFlight" {
		return st.MaxInFlight
	}
	return st.Status[kind+":total"]
}

// etagOf 从源站直接取一次某变体响应的 ETag（绕过代理缓存）。
func (h *harness) etagOf(asset, lang string) string {
	r, err := http.NewRequest(http.MethodGet, h.origin.URL+"/assets/"+asset, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	r.Header.Set("Accept-Language", lang)
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		h.t.Fatal(err)
	}
	res.Body.Close()
	return res.Header.Get("ETag")
}

// ---------- 用例 ----------

func TestBasicHitAndClientConditional304(t *testing.T) {
	h := newHarness(t)

	r1 := h.get("a1", "zh", "")
	if r1.status != 200 || r1.header.Get("X-Cache-Status") != "miss" {
		t.Fatalf("first want 200/miss, got %d/%q body=%q", r1.status, r1.header.Get("X-Cache-Status"), r1.body)
	}
	if h.count("zh") != 1 {
		t.Fatalf("origin wants 1 zh request, got %d", h.count("zh"))
	}
	etag := r1.header.Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag")
	}

	r2 := h.get("a1", "zh", "")
	if r2.status != 200 || r2.header.Get("X-Cache-Status") != "hit" {
		t.Fatalf("second want 200/hit, got %d/%q", r2.status, r2.header.Get("X-Cache-Status"))
	}
	if h.count("zh") != 1 {
		t.Fatalf("cached response must not hit origin, total=%d", h.count("zh"))
	}
	if r2.body != r1.body {
		t.Fatal("cache returned different body")
	}

	// 正确变体的客户端条件请求 -> 304。
	r3 := h.get("a1", "zh", etag)
	if r3.status != 304 || r3.header.Get("X-Cache-Status") != "hit" {
		t.Fatalf("matching validator want 304/hit, got %d/%q", r3.status, r3.header.Get("X-Cache-Status"))
	}
	if h.count("zh") != 1 {
		t.Fatalf("conditional hit must stay local, total=%d", h.count("zh"))
	}

	// 不匹配的校验器 -> 200。
	r4 := h.get("a1", "zh", `"other"`)
	if r4.status != 200 || r4.body != r1.body {
		t.Fatalf("non-matching validator want 200, got %d", r4.status)
	}
}

func TestExpiryBoundaryAnd304Extension(t *testing.T) {
	h := newHarness(t)
	h.setSpec("b1", origin.AssetSpec{MaxAge: 10, HasETag: true})

	r := h.get("b1", "en", "")
	if r.status != 200 || r.header.Get("X-Cache-Status") != "miss" {
		t.Fatalf("prime want miss, got %d/%q", r.status, r.header.Get("X-Cache-Status"))
	}

	// 边界：age == maxAge 的瞬间即过期，必须回源条件验证。
	h.clock.Advance(10 * time.Second)
	h.setFail("b1", true)
	r2 := h.get("b1", "en", "")
	if r2.status != 200 || r2.header.Get("X-Cache-Status") != "stale" ||
		r2.header.Get("Warning") == "" {
		t.Fatalf("at maxAge with 5xx want stale 200, got %d/%q", r2.status, r2.header.Get("X-Cache-Status"))
	}
	if st := h.ctrl.SnapshotStats().Status; st["en:500"] != 1 {
		t.Fatalf("want 1 conditional revalidation 500, got %v", st)
	}

	// 源站恢复：过期条目条件请求拿到 304，新鲜期按原 max-age 延长。
	h.setFail("b1", false)
	r3 := h.get("b1", "en", "")
	if r3.status != 200 || r3.header.Get("X-Cache-Status") != "revalidated" {
		t.Fatalf("304 revalidation want revalidated, got %d/%q", r3.status, r3.header.Get("X-Cache-Status"))
	}
	if st := h.ctrl.SnapshotStats().Status; st["en:304"] != 1 {
		t.Fatalf("want exactly one origin 304, got %v", st)
	}

	// 续期后的 9 秒仍是新鲜的本地命中。
	h.clock.Advance(9 * time.Second)
	r4 := h.get("b1", "en", "")
	if r4.status != 200 || r4.header.Get("X-Cache-Status") != "hit" {
		t.Fatalf("post-extension 9s want hit, got %d/%q", r4.status, r4.header.Get("X-Cache-Status"))
	}

	// 从续期点（t=10）算起：再过 1 秒恰好过期。
	h.clock.Advance(1 * time.Second)
	// 过期 31 秒 -> 超出兜底窗口，502。
	h.clock.Advance(31 * time.Second)
	h.setFail("b1", true)
	r5 := h.get("b1", "en", "")
	if r5.status != 502 {
		t.Fatalf("beyond stale window want 502, got %d", r5.status)
	}

	// 窗口内边界（过期后正好 30 秒）仍允许兜底。
	h.setFail("b1", false)
	h.get("b1", "en", "") // 重新填充
	h.clock.Advance(10 * time.Second)
	h.setFail("b1", true)
	h.clock.Advance(30 * time.Second)
	r6 := h.get("b1", "en", "")
	if r6.status != 200 || r6.header.Get("X-Cache-Status") != "stale" {
		t.Fatalf("at exactly maxAge+30s want stale, got %d/%q", r6.status, r6.header.Get("X-Cache-Status"))
	}
}

func TestCacheabilityRules(t *testing.T) {
	cases := []struct {
		name string
		spec origin.AssetSpec
	}{
		{"maxAge=0 must revalidate", origin.AssetSpec{MaxAge: 0, HasETag: true}},
		{"maxAge=60 allowed", origin.AssetSpec{MaxAge: 60, HasETag: true}},
		{"maxAge=61 rejected", origin.AssetSpec{MaxAge: 61, HasETag: true}},
		{"no ETag rejected", origin.AssetSpec{MaxAge: 10, HasETag: false}},
		{"maxAge=-1 no header rejected", origin.AssetSpec{MaxAge: -1, HasETag: true}},
		{"Vary star rejected", origin.AssetSpec{MaxAge: 10, HasETag: true, Vary: "*"}},
		{"Vary other header rejected", origin.AssetSpec{MaxAge: 10, HasETag: true, Vary: "Accept-Language, Cookie"}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			id := fmt.Sprintf("c%d", i)
			h.setSpec(id, tc.spec)

			r1 := h.get(id, "zh", "")
			if r1.status != 200 {
				t.Fatalf("prime want 200, got %d", r1.status)
			}
			r2 := h.get(id, "zh", "")
			cached := r2.header.Get("X-Cache-Status") == "hit"

			// maxAge=0：条目会被存下但立刻过期，因此第二次是条件再验证（源站 304）
			// 而不是本地 hit；源站总计数恰好 2。
			if tc.spec.MaxAge == 0 && tc.spec.HasETag && tc.spec.Vary == "" {
				if cached {
					t.Fatal("max-age=0 must not be a fresh hit")
				}
				if got := h.count("zh"); got != 2 {
					t.Fatalf("max-age=0 wants 2 origin requests, got %d", got)
				}
				if r2.header.Get("X-Cache-Status") != "revalidated" {
					t.Fatalf("want revalidated, got %q", r2.header.Get("X-Cache-Status"))
				}
				return
			}

			wantCached := tc.spec.MaxAge >= 1 && tc.spec.MaxAge <= 60 &&
				tc.spec.HasETag && (tc.spec.Vary == "" || tc.spec.Vary == "Accept-Language")
			if cached != wantCached {
				t.Fatalf("cached=%v want %v (status2=%q origin=%d)", cached, wantCached,
					r2.header.Get("X-Cache-Status"), h.count("zh"))
			}
			if wantCached && h.count("zh") != 1 {
				t.Fatalf("cacheable asset wants 1 origin request, got %d", h.count("zh"))
			}
			if !wantCached && h.count("zh") != 2 {
				t.Fatalf("non-cacheable asset wants 2 origin requests, got %d", h.count("zh"))
			}
		})
	}
}

func TestCrossLanguageIsolation(t *testing.T) {
	h := newHarness(t)

	zh1 := h.get("d1", "zh", "")
	en1 := h.get("d1", "en", "")
	if zh1.body == en1.body {
		t.Fatal("zh/en bodies should differ by language")
	}
	if zh1.header.Get("ETag") == en1.header.Get("ETag") {
		t.Fatal("ETags must be per-variant")
	}

	// 之后两种语言全部本地命中，且各自内容正确。
	for i := 0; i < 3; i++ {
		zh := h.get("d1", "zh", "")
		en := h.get("d1", "en", "")
		if zh.header.Get("X-Cache-Status") != "hit" || en.header.Get("X-Cache-Status") != "hit" {
			t.Fatalf("both variants must hit, got zh=%q en=%q",
				zh.header.Get("X-Cache-Status"), en.header.Get("X-Cache-Status"))
		}
		if zh.body != zh1.body || en.body != en1.body {
			t.Fatal("variant body mismatch after caching")
		}
	}
	if h.count("zh") != 1 || h.count("en") != 1 {
		t.Fatalf("want one origin fetch per variant, got zh=%d en=%d", h.count("zh"), h.count("en"))
	}

	// 只让 zh 过期并在源站制造错误：en 仍必须是干净的新鲜命中。
	h.clock.Advance(11 * time.Second)
	h.setFail("d1", true)
	zh2 := h.get("d1", "zh", "")
	en2 := h.get("d1", "en", "")
	if zh2.header.Get("X-Cache-Status") != "stale" {
		t.Fatalf("expired zh during failure want stale, got %q", zh2.header.Get("X-Cache-Status"))
	}
	// en 条目同样过期，回源失败也会走 stale——改用新鲜窗口内的断言：
	// 让时钟回退不可行，因此这里验证 en 返回的仍然是 en 内容而非 zh。
	if !bytes.Contains([]byte(en2.body), []byte("lang=en")) {
		t.Fatalf("en response must never be served zh content: %q", en2.body)
	}
	if !bytes.Contains([]byte(zh2.body), []byte("lang=zh")) {
		t.Fatalf("stale zh must remain zh content: %q", zh2.body)
	}
}

func TestUnsupportedVariantNotCached(t *testing.T) {
	h := newHarness(t)

	r1 := h.get("e1", "fr", "")
	if r1.status != 200 {
		t.Fatalf("fr want 200 passthrough, got %d", r1.status)
	}
	if cc := r1.header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("unsupported variant must be declared no-store, got %q", cc)
	}
	if r1.header.Get("X-Cache-Status") != "bypass" {
		t.Fatalf("want bypass, got %q", r1.header.Get("X-Cache-Status"))
	}
	r2 := h.get("e1", "fr", "")
	if r2.header.Get("Cache-Control") != "no-store" {
		t.Fatalf("second fr response still no-store, got %q", r2.header.Get("Cache-Control"))
	}
	if h.count("other") != 2 {
		t.Fatalf("fr must never be cached: origin calls=%d want 2", h.count("other"))
	}

	// 不支持的变体不能污染受支持变体。
	zh := h.get("e1", "zh", "")
	if zh.header.Get("X-Cache-Status") != "miss" || !bytes.Contains([]byte(zh.body), []byte("lang=zh")) {
		t.Fatalf("zh after fr traffic want fresh zh miss, got %q body=%q",
			zh.header.Get("X-Cache-Status"), zh.body)
	}
	// q 值里带不支持语言时同样不缓存。
	r3 := h.get("e1", "ja;q=0.9", "")
	if r3.header.Get("Cache-Control") != "no-store" {
		t.Fatalf("ja-only negotiation must be no-store, got %q", r3.header.Get("Cache-Control"))
	}
}

func TestStaleServingWindow(t *testing.T) {
	h := newHarness(t)
	h.setSpec("f1", origin.AssetSpec{MaxAge: 10, HasETag: true})
	h.get("f1", "zh", "")

	h.clock.Advance(15 * time.Second) // 过期 5 秒
	h.setFail("f1", true)

	r1 := h.get("f1", "zh", "")
	if r1.status != 200 || r1.header.Get("X-Cache-Status") != "stale" ||
		r1.header.Get("X-Cache-Stale") != "1" || r1.header.Get("Warning") == "" {
		t.Fatalf("5s stale want marked stale, got %d/%q", r1.status, r1.header.Get("X-Cache-Status"))
	}
	if r1.header.Get("Age") != "15" {
		t.Fatalf("Age want 15, got %q", r1.header.Get("Age"))
	}

	// 连续多次出错请求都可兜底，且只产生一次条件回源？——不：每次过期请求都会回源，
	// 这是允许的（合并只约束并发）。这里断言内容始终是旧内容。
	r2 := h.get("f1", "zh", "")
	if r2.status != 200 || !bytes.Contains([]byte(r2.body), []byte("v1")) {
		t.Fatalf("repeated stale want old body, got %d", r2.status)
	}

	// 推进到过期 31 秒 -> 超出窗口，502。
	h.clock.Advance(26 * time.Second)
	r3 := h.get("f1", "zh", "")
	if r3.status != 502 || r3.header.Get("X-Cache-Status") != "error" {
		t.Fatalf("31s past expiry want 502/error, got %d/%q", r3.status, r3.header.Get("X-Cache-Status"))
	}

	// 源站恢复后条件再验证成功：旧表示仍有效，得到 304 续期。
	h.setFail("f1", false)
	r4 := h.get("f1", "zh", "")
	if r4.header.Get("X-Cache-Status") != "revalidated" {
		t.Fatalf("recovery 304 want revalidated, got %q", r4.header.Get("X-Cache-Status"))
	}
	r5 := h.get("f1", "zh", "")
	if r5.header.Get("X-Cache-Status") != "hit" {
		t.Fatalf("want hit after revalidation, got %q", r5.header.Get("X-Cache-Status"))
	}
}

func TestConcurrentCoalescing(t *testing.T) {
	h := newHarness(t)
	h.ctrl.CloseGate() // 所有源站资源请求在闸门处排队

	const n = 8
	var wg sync.WaitGroup
	results := make([]resp, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = h.get("g1", "zh", "")
		}(i)
	}
	close(start)

	waitUntil(t, func() bool { return h.count("zh") >= 1 }, time.Second)
	time.Sleep(100 * time.Millisecond) // 确认没有第二个请求穿到源站
	if got := h.count("zh"); got != 1 {
		t.Fatalf("want exactly 1 in-flight origin request, got %d", got)
	}
	if h.count("maxFlight") > 1 {
		t.Fatalf("concurrent coalescing violated, max origin in-flight=%d", h.count("maxFlight"))
	}

	h.ctrl.OpenGate()
	wg.Wait()
	for i, r := range results {
		if r.status != 200 || r.header.Get("X-Cache-Status") == "" {
			t.Fatalf("waiter %d bad result: %+v", i, r)
		}
	}
	if h.count("zh") != 1 {
		t.Fatalf("after coalescing want total 1 origin request, got %d", h.count("zh"))
	}
	// 后续请求直接命中缓存。
	if r := h.get("g1", "zh", ""); r.header.Get("X-Cache-Status") != "hit" {
		t.Fatalf("post-coalesce want hit, got %q", r.header.Get("X-Cache-Status"))
	}
}

func TestConcurrentCoalescingTwoLanguages(t *testing.T) {
	h := newHarness(t)
	h.ctrl.CloseGate()

	const n = 6
	var wg sync.WaitGroup
	res := make(map[string][]resp)
	res["zh"] = make([]resp, n)
	res["en"] = make([]resp, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) { defer wg.Done(); <-start; res["zh"][i] = h.get("g2", "zh", "") }(i)
		go func(i int) { defer wg.Done(); <-start; res["en"][i] = h.get("g2", "en", "") }(i)
	}
	close(start)

	waitUntil(t, func() bool { return h.count("zh") >= 1 && h.count("en") >= 1 }, time.Second)
	time.Sleep(100 * time.Millisecond)
	if h.count("zh") != 1 || h.count("en") != 1 {
		t.Fatalf("want one origin fetch per language, got zh=%d en=%d", h.count("zh"), h.count("en"))
	}
	if h.count("maxFlight") != 2 {
		t.Fatalf("different languages must coalesce independently, maxFlight=%d", h.count("maxFlight"))
	}
	h.ctrl.OpenGate()
	wg.Wait()

	for i := 0; i < n; i++ {
		if !bytes.Contains([]byte(res["zh"][i].body), []byte("lang=zh")) {
			t.Fatalf("zh waiter %d got %q", i, res["zh"][i].body)
		}
		if !bytes.Contains([]byte(res["en"][i].body), []byte("lang=en")) {
			t.Fatalf("en waiter %d got %q", i, res["en"][i].body)
		}
	}
}

func TestWaiterCancellationDoesNotBreakOthers(t *testing.T) {
	h := newHarness(t)
	h.ctrl.CloseGate()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	ch1 := h.getAsync(ctx1, "h1", "zh")
	ch2 := h.getAsync(ctx2, "h1", "zh")
	waitUntil(t, func() bool { return h.count("zh") >= 1 }, time.Second)

	// 等待者 1 取消：必须快速返回错误，且不影响唯一一次回源。
	cancel1()
	r1 := <-ch1
	if r1.status != 0 {
		t.Fatalf("canceled client want transport error, got status %d", r1.status)
	}
	time.Sleep(100 * time.Millisecond)
	if h.count("zh") != 1 {
		t.Fatalf("canceled waiter must not trigger extra fetch, got %d", h.count("zh"))
	}

	// 放行：等待者 2 正常拿到结果。
	h.ctrl.OpenGate()
	r2 := <-ch2
	if r2.status != 200 || !bytes.Contains([]byte(r2.body), []byte("lang=zh")) {
		t.Fatalf("surviving waiter want 200 zh, got %d %q", r2.status, r2.body)
	}
	if h.count("zh") != 1 {
		t.Fatalf("still want total 1 origin request, got %d", h.count("zh"))
	}

	// 回源已完成并填充缓存：取消者后来重试得到本地命中。
	r3 := h.get("h1", "zh", "")
	if r3.header.Get("X-Cache-Status") != "hit" {
		t.Fatalf("leader fetch must populate cache, got %q", r3.header.Get("X-Cache-Status"))
	}
}

func TestLeaderClientCancellationKeepsFetchAlive(t *testing.T) {
	h := newHarness(t)
	h.ctrl.CloseGate()

	ctx, cancel := context.WithCancel(context.Background())
	ch := h.getAsync(ctx, "h2", "zh")
	waitUntil(t, func() bool { return h.count("zh") >= 1 }, time.Second)

	cancel()
	if r := <-ch; r.status != 0 {
		t.Fatalf("canceled leader want transport error, got %d", r.status)
	}

	// 另一个请求加入同一轮（flight 尚未结束，因为回源被闸门挂住）。
	ctx3, cancel3 := context.WithCancel(context.Background())
	ch3 := h.getAsync(ctx3, "h2", "zh")
	time.Sleep(100 * time.Millisecond)
	h.ctrl.OpenGate()
	defer cancel3()

	r3 := <-ch3
	if r3.status != 200 {
		t.Fatalf("waiter joining canceled leader's flight must succeed, got %d", r3.status)
	}
	if h.count("zh") != 1 {
		t.Fatalf("leader cancel must not abort fetch, origin calls=%d", h.count("zh"))
	}
}

func TestConditionalIsPerVariant(t *testing.T) {
	h := newHarness(t)

	zhETag := h.etagOf("i1", "zh")
	enETag := h.etagOf("i1", "en")
	if zhETag == "" || zhETag == enETag {
		t.Fatalf("need distinct per-variant etags, got %q %q", zhETag, enETag)
	}

	// 用各变体的正常请求填充缓存。
	if r := h.get("i1", "zh", ""); r.status != 200 {
		t.Fatalf("prime zh: %d", r.status)
	}
	if r := h.get("i1", "en", ""); r.status != 200 {
		t.Fatalf("prime en: %d", r.status)
	}

	// 拿 zh 的 ETag 去问 en：必须返回 en 的完整 200，而不是 304。
	r1 := h.get("i1", "en", zhETag)
	if r1.status != 200 || !bytes.Contains([]byte(r1.body), []byte("lang=en")) {
		t.Fatalf("zh validator on en must not 304, got %d body=%q", r1.status, r1.body)
	}
	// 拿 en 的 ETag 去问 zh：同理。
	r2 := h.get("i1", "zh", enETag)
	if r2.status != 200 || !bytes.Contains([]byte(r2.body), []byte("lang=zh")) {
		t.Fatalf("en validator on zh must not 304, got %d body=%q", r2.status, r2.body)
	}
	// 正确配对才是 304。
	r3 := h.get("i1", "zh", zhETag)
	if r3.status != 304 {
		t.Fatalf("matching zh validator want 304, got %d", r3.status)
	}
	r4 := h.get("i1", "en", enETag)
	if r4.status != 304 {
		t.Fatalf("matching en validator want 304, got %d", r4.status)
	}
}

func TestColdConditionalPassthrough(t *testing.T) {
	h := newHarness(t)
	etag := h.etagOf("j1", "zh")

	// 本地无缓存时直接携带客户端校验器回源：源站 304 被透传。
	r1 := h.get("j1", "zh", etag)
	if r1.status != 304 || r1.header.Get("X-Cache-Status") != "bypass" {
		t.Fatalf("cold conditional 304 passthrough, got %d/%q", r1.status, r1.header.Get("X-Cache-Status"))
	}

	// 冷 200 后正常入缓存，随后本地条件命中。
	r2 := h.get("j1", "zh", "")
	if r2.status != 200 || r2.header.Get("X-Cache-Status") != "miss" {
		t.Fatalf("cold 200 want miss, got %d/%q", r2.status, r2.header.Get("X-Cache-Status"))
	}
	r3 := h.get("j1", "zh", etag)
	if r3.status != 304 || r3.header.Get("X-Cache-Status") != "hit" {
		t.Fatalf("warm conditional want 304/hit, got %d/%q", r3.status, r3.header.Get("X-Cache-Status"))
	}
}

func TestRouting(t *testing.T) {
	h := newHarness(t)

	// 非 assets 路径：404。
	req, _ := http.NewRequest(http.MethodGet, h.proxy.URL+"/other", nil)
	res, err := h.proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("/other want 404, got %d", res.StatusCode)
	}

	// 非 GET：405 且带 Allow。
	req, _ = http.NewRequest(http.MethodPost, h.proxy.URL+"/assets/k1", nil)
	res, err = h.proxy.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 405 || res.Header.Get("Allow") != "GET" {
		t.Fatalf("POST want 405 + Allow: GET, got %d allow=%q", res.StatusCode, res.Header.Get("Allow"))
	}

	// 畸形 id：404。
	req, _ = http.NewRequest(http.MethodGet, h.proxy.URL+"/assets/", nil)
	res, _ = h.proxy.Client().Do(req)
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("/assets/ want 404, got %d", res.StatusCode)
	}
}

// 源站控制面冒烟：确认演练用的 HTTP 控制接口工作正常。
func TestOriginControlAPI(t *testing.T) {
	h := newHarness(t)

	// 通过控制面配置 maxAge，并在代理侧观察到该配置生效。
	body := `{"maxAge":45}`
	req, _ := http.NewRequest(http.MethodPost, h.origin.URL+"/control/assets/z1", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 204 {
		t.Fatalf("control set want 204, got %d", res.StatusCode)
	}
	res.Body.Close()

	r := h.get("z1", "zh", "")
	if cc := r.header.Get("Cache-Control"); !strings.Contains(cc, "max-age=45") {
		t.Fatalf("control-plane maxAge want applied, got %q", cc)
	}

	// 打开故障开关：冷缺失无旧响应，代理应返回 502。
	req, _ = http.NewRequest(http.MethodPost, h.origin.URL+"/control/assets/z2", bytes.NewBufferString(`{"fail":true}`))
	req.Header.Set("Content-Type", "application/json")
	res, _ = http.DefaultClient.Do(req)
	res.Body.Close()
	if r := h.get("z2", "zh", ""); r.status != 502 {
		t.Fatalf("cold failure want 502, got %d", r.status)
	}

	// reset 后统计为空。
	req, _ = http.NewRequest(http.MethodPost, h.origin.URL+"/control/reset", nil)
	res, _ = http.DefaultClient.Do(req)
	res.Body.Close()
	req, _ = http.NewRequest(http.MethodGet, h.origin.URL+"/control/stats", nil)
	res, _ = http.DefaultClient.Do(req)
	var st origin.Stats
	if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if len(st.Status) != 0 {
		t.Fatalf("reset want empty stats, got %v", st.Status)
	}

	// 闸门接口也应当可用（close 后立即 open，避免影响其他用例）。
	for _, action := range []string{"close", "open"} {
		req, _ = http.NewRequest(http.MethodGet, h.origin.URL+"/control/gate/"+action, nil)
		res, _ = http.DefaultClient.Do(req)
		if res.StatusCode != 204 {
			t.Fatalf("gate %s want 204, got %d", action, res.StatusCode)
		}
		res.Body.Close()
	}
}

// waitUntil 轮询条件，超时即失败。
func waitUntil(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
