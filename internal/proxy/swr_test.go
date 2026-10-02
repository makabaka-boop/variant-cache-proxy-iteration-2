package proxy_test

import (
	"bytes"
	"context"
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

func intp(n int) *int { return &n }

// swrSpec 构造一个带显式 stale-while-revalidate 窗口的资源配置。
func swrSpec(maxAge, swr, version int) origin.AssetSpec {
	return origin.AssetSpec{MaxAge: maxAge, HasETag: true, Version: version, SWR: intp(swr)}
}

// getAsyncINM 以独立 HTTP 连接发起带 If-None-Match 的异步请求。
func (h *harness) getAsyncINM(ctx context.Context, asset, acceptLang, inm string) <-chan resp {
	ch := make(chan resp, 1)
	go func() {
		url := h.proxy.URL + "/assets/" + asset
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		req.Header.Set("Accept-Language", acceptLang)
		if inm != "" {
			req.Header.Set("If-None-Match", inm)
		}
		r, err := h.proxy.Client().Do(req)
		if err != nil {
			ch <- resp{status: 0}
			return
		}
		defer r.Body.Close()
		data, _ := io.ReadAll(r.Body)
		ch <- resp{r.StatusCode, r.Header.Clone(), string(data)}
	}()
	return ch
}

// assertStaleMarkers 断言响应是“带明确陈旧标记的完整旧内容”。
func assertStaleMarkers(t *testing.T, r resp, wantBody string) {
	t.Helper()
	if r.status != 200 {
		t.Fatalf("SWR want 200 stale, got %d", r.status)
	}
	if r.header.Get("X-Cache-Status") != "stale" ||
		r.header.Get("X-Cache-Stale") != "1" ||
		r.header.Get("Warning") == "" {
		t.Fatalf("SWR want marked stale (status/stale/Warning), got %q stale=%q warn=%q",
			r.header.Get("X-Cache-Status"), r.header.Get("X-Cache-Stale"), r.header.Get("Warning"))
	}
	if r.body != wantBody {
		t.Fatalf("SWR must serve full old body\n want %q\n got  %q", wantBody, r.body)
	}
}

func mustGet(t *testing.T, url, lang, inm string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if lang != "" {
		req.Header.Set("Accept-Language", lang)
	}
	if inm != "" {
		req.Header.Set("If-None-Match", inm)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return r
}

func readBody(r *http.Response) string {
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	return string(b)
}

// ---------- 基本 SWR：立即陈旧 + 单轮后台刷新 + 随后命中新版本 ----------

func TestSWRStaleThenBackgroundRefresh(t *testing.T) {
	h := newHarness(t)
	h.setSpec("s1", swrSpec(10, 30, 1))

	prime := h.get("s1", "zh", "")
	if prime.header.Get("X-Cache-Status") != "miss" {
		t.Fatalf("prime want miss, got %q", prime.header.Get("X-Cache-Status"))
	}
	if cc := prime.header.Get("Cache-Control"); !strings.Contains(cc, "stale-while-revalidate=30") {
		t.Fatalf("primed response must carry origin swr window, got %q", cc)
	}
	oldBody := prime.body

	// 过期 5 秒，仍在 SWR 窗口内；闸门挂住后台刷新。
	h.clock.Advance(15 * time.Second)
	h.ctrl.CloseGate()

	r := h.get("s1", "zh", "")
	assertStaleMarkers(t, r, oldBody)
	// 即使响应已返回，后台条件回源应当已经在闸门处排队（恰好一轮）。
	waitUntil(t, func() bool { return h.count("zh") >= 2 }, time.Second)
	if got := h.count("zh"); got != 2 {
		t.Fatalf("want exactly one background revalidation, total origin=%d", got)
	}

	// 窗口内再来的请求同样立即得到旧内容，且不增加回源轮次。
	r2 := h.get("s1", "zh", "")
	assertStaleMarkers(t, r2, oldBody)
	time.Sleep(100 * time.Millisecond)
	if got := h.count("zh"); got != 2 {
		t.Fatalf("only one background round allowed, total=%d", got)
	}

	// 源站素材更新到 v2 后放行：后台条件回源拿到新 200 并替换条目。
	h.setSpec("s1", swrSpec(10, 30, 2))
	h.ctrl.OpenGate()
	waitUntil(t, func() bool {
		st := h.ctrl.SnapshotStats().Status
		return st["zh:200"] == 2
	}, time.Second)

	// 后台刷新完成后的新请求必须是新版本命中，而不是再陈旧一次旧内容。
	waitUntil(t, func() bool {
		got := h.get("s1", "zh", "")
		return got.header.Get("X-Cache-Status") == "hit" && strings.Contains(got.body, "v2")
	}, time.Second)
}

// ---------- 窗口边界：窗口内陈旧；刚出窗回落到同步再验证/兜底 ----------

func TestSWRWindowBoundary(t *testing.T) {
	h := newHarness(t)
	h.setSpec("sb", swrSpec(10, 20, 1))
	prime := h.get("sb", "en", "")

	// age == maxAge+swr 这一刻仍在窗口内（含边界），陈旧且后台刷新。
	h.clock.Advance(30 * time.Second)
	h.ctrl.CloseGate()
	r := h.get("sb", "en", "")
	assertStaleMarkers(t, r, prime.body)
	waitUntil(t, func() bool { return h.count("en") >= 2 }, time.Second)
	h.ctrl.OpenGate()
	// 源站内容未变 -> 后台 304 续期，随后请求新鲜命中。
	waitUntil(t, func() bool {
		return h.ctrl.SnapshotStats().Status["en:304"] == 1
	}, time.Second)
	waitUntil(t, func() bool {
		return h.get("sb", "en", "").header.Get("X-Cache-Status") == "hit"
	}, time.Second)

	// 新资源：走到窗口外 1 秒（age=maxAge+swr+1）不得再走 SWR。
	h.setSpec("sc", swrSpec(10, 20, 1))
	h.get("sc", "en", "")
	h.clock.Advance(31 * time.Second) // 过期 21 秒：超出 SWR(20)，但仍在 30s 兜底窗
	h.setFail("sc", true)
	r2 := h.get("sc", "en", "")
	// 同步再验证撞到 500 -> 旧的出错兜底语义（旧内容，但带 X-Cache-Error）。
	if r2.header.Get("X-Cache-Status") != "stale" || r2.header.Get("X-Cache-Error") == "" {
		t.Fatalf("beyond SWR window on 5xx want error-stale, got %q err=%q",
			r2.header.Get("X-Cache-Status"), r2.header.Get("X-Cache-Error"))
	}
	if !bytes.Contains([]byte(r2.body), []byte("v1")) {
		t.Fatalf("error-stale must keep old body, got %q", r2.body)
	}
}

// SWR 窗口比 30s 兜底窗更长：出了兜底窗、仍在 SWR 窗，后台 5xx 也不抹去旧内容。
func TestSWRWindowExtendsBeyondErrorWindow(t *testing.T) {
	h := newHarness(t)
	h.setSpec("sl", swrSpec(10, 120, 1))
	prime := h.get("sl", "zh", "")

	// 过期 35 秒：超出 30s 出错兜底窗，但仍在 120s SWR 窗内。
	h.clock.Advance(45 * time.Second)
	h.setFail("sl", true) // 放行后后台刷新拿到 500
	h.ctrl.CloseGate()

	r := h.get("sl", "zh", "")
	assertStaleMarkers(t, r, prime.body) // 主动 SWR 陈旧（无 X-Cache-Error）
	if r.header.Get("X-Cache-Error") != "" {
		t.Fatalf("proactive SWR stale must not carry error reason, got %q",
			r.header.Get("X-Cache-Error"))
	}
	h.ctrl.OpenGate()
	waitUntil(t, func() bool { return h.ctrl.SnapshotStats().Status["zh:500"] >= 1 }, time.Second)

	// 后台 500 不抹旧内容：连续多次 SWR 请求仍陈旧返回 v1（每轮后台再撞 500），
	// 绝不出现 502，也不回退成同步阻塞。
	for i := 0; i < 3; i++ {
		waitUntil(t, func() bool {
			got := h.get("sl", "zh", "")
			return got.status == 200 &&
				got.header.Get("X-Cache-Status") == "stale" &&
				bytes.Equal([]byte(got.body), []byte(prime.body))
		}, time.Second)
	}
}

// ---------- 客户端 If-None-Match：陈旧条目不得直接 304，必须同步回源 ----------

func TestSWRClientConditionalNotServedFromStale(t *testing.T) {
	h := newHarness(t)
	h.setSpec("snc", swrSpec(10, 30, 1))
	prime := h.get("snc", "zh", "")
	etag := prime.header.Get("ETag")

	h.clock.Advance(15 * time.Second) // 过期、在 SWR 窗内
	h.ctrl.CloseGate()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := h.getAsyncINM(ctx, "snc", "zh", etag)
	// 条件请求必须被同步回源阻塞，而不是立即凭陈旧条目返回 304。
	waitUntil(t, func() bool { return h.count("zh") >= 2 }, time.Second)
	select {
	case got := <-ch:
		t.Fatalf("conditional request must wait for origin, prematurely got %+v", got)
	case <-time.After(150 * time.Millisecond):
	}

	h.ctrl.OpenGate()
	got := <-ch
	if got.status != 304 {
		t.Fatalf("conditional revalidation matching etag want 304, got %d body=%q", got.status, got.body)
	}
	if got.header.Get("X-Cache-Status") != "revalidated" {
		t.Fatalf("want revalidated 304, got %q", got.header.Get("X-Cache-Status"))
	}
	if n := h.count("zh"); n != 2 {
		t.Fatalf("conditional revalidation must coalesce to the one round, total=%d", n)
	}
}

// 非条件 SWR 请求立即返回陈旧；同期加入的条件请求等待同一轮回源，不产生第二次。
func TestSWRConditionalJoinsBackgroundRound(t *testing.T) {
	h := newHarness(t)
	h.setSpec("sj", swrSpec(10, 30, 1))
	prime := h.get("sj", "zh", "")
	etag := prime.header.Get("ETag")

	h.clock.Advance(12 * time.Second)
	h.ctrl.CloseGate()

	// 非条件请求先触发后台刷新一轮。
	stale := h.get("sj", "zh", "")
	assertStaleMarkers(t, stale, prime.body)
	waitUntil(t, func() bool { return h.count("zh") >= 2 }, time.Second)

	// 条件请求加入同一轮并等待结果。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	chCond := h.getAsyncINM(ctx, "sj", "zh", etag)
	// 再来一个非条件请求，也立即陈旧且不加轮次。
	again := h.get("sj", "zh", "")
	assertStaleMarkers(t, again, prime.body)

	time.Sleep(100 * time.Millisecond)
	if n := h.count("zh"); n != 2 {
		t.Fatalf("joined conditional must not add a round, total=%d", n)
	}

	h.ctrl.OpenGate()
	got := <-chCond
	if got.status != 304 || got.header.Get("X-Cache-Status") != "revalidated" {
		t.Fatalf("joined conditional want 304/revalidated, got %d/%q", got.status,
			got.header.Get("X-Cache-Status"))
	}
	if n := h.count("zh"); n != 2 {
		t.Fatalf("still want one background round, total=%d", n)
	}
}

// ---------- 双语言隔离：SWR 刷新按语言各自一轮，内容不串 ----------

func TestSWRPerLanguageIsolation(t *testing.T) {
	h := newHarness(t)
	h.setSpec("sp", swrSpec(10, 30, 1))
	zh0 := h.get("sp", "zh", "")
	en0 := h.get("sp", "en", "")

	h.clock.Advance(15 * time.Second)
	h.ctrl.CloseGate()

	zhStale := h.get("sp", "zh", "")
	enStale := h.get("sp", "en", "")
	assertStaleMarkers(t, zhStale, zh0.body)
	assertStaleMarkers(t, enStale, en0.body)

	waitUntil(t, func() bool { return h.count("zh") >= 2 && h.count("en") >= 2 }, time.Second)
	if h.count("maxFlight") != 2 {
		t.Fatalf("two languages must refresh independently, maxFlight=%d", h.count("maxFlight"))
	}
	if !bytes.Contains([]byte(zhStale.body), []byte("lang=zh")) ||
		!bytes.Contains([]byte(enStale.body), []byte("lang=en")) {
		t.Fatalf("stale content leaked across languages: zh=%q en=%q", zhStale.body, enStale.body)
	}

	// 放行后两种语言都更新到 v2，但各自仍只含本语言内容，绝不串语言。
	h.setSpec("sp", swrSpec(10, 30, 2))
	h.ctrl.OpenGate()

	waitUntil(t, func() bool {
		zh := h.get("sp", "zh", "")
		en := h.get("sp", "en", "")
		return strings.Contains(zh.body, "v2") && strings.Contains(zh.body, "lang=zh") &&
			strings.Contains(en.body, "v2") && strings.Contains(en.body, "lang=en") &&
			!strings.Contains(zh.body, "lang=en") && !strings.Contains(en.body, "lang=zh")
	}, time.Second)

	// 每个语言各自恰好一轮后台刷新。
	if h.count("zh") != 2 || h.count("en") != 2 {
		t.Fatalf("want exactly one refresh round per language, zh=%d en=%d",
			h.count("zh"), h.count("en"))
	}
}

// ---------- 后台 4xx：按现有失效语义作废旧条目 ----------

func TestSWRBackground4xxInvalidates(t *testing.T) {
	h := newHarness(t)
	h.setSpec("s4", swrSpec(10, 30, 1))
	h.get("s4", "zh", "")

	h.clock.Advance(15 * time.Second)
	h.ctrl.CloseGate()
	// 触发后台刷新一轮（此刻源站仍健康，请求挂在闸门上）。
	r := h.get("s4", "zh", "")
	if r.header.Get("X-Cache-Status") != "stale" {
		t.Fatalf("first SWR response want stale, got %q", r.header.Get("X-Cache-Status"))
	}
	waitUntil(t, func() bool { return h.count("zh") >= 2 }, time.Second)

	// 放行前把源站切成 404：挂起的后台刷新拿到 404 并作废旧条目。
	h.setSpec("s4", origin.AssetSpec{
		MaxAge: 10, HasETag: true, Version: 1, SWR: intp(30),
		Fail: true, FailStatus: 404,
	})
	h.ctrl.OpenGate()
	waitUntil(t, func() bool { return h.ctrl.SnapshotStats().Status["zh:404"] >= 1 }, time.Second)

	// 条目已作废：下一次请求同步回源，透传 404。
	waitUntil(t, func() bool {
		got := h.get("s4", "zh", "")
		return got.status == 404
	}, time.Second)
}

// ---------- 后台 5xx / 网络错误：不抹去旧内容 ----------

func TestSWRBackground5xxKeepsStale(t *testing.T) {
	h := newHarness(t)
	h.setSpec("s5", swrSpec(10, 30, 1))
	prime := h.get("s5", "zh", "")

	h.clock.Advance(15 * time.Second)
	h.setFail("s5", true)
	// 触发后台刷新；该请求本身拿到 SWR 陈旧内容。
	r := h.get("s5", "zh", "")
	assertStaleMarkers(t, r, prime.body)
	waitUntil(t, func() bool { return h.ctrl.SnapshotStats().Status["zh:500"] >= 1 }, time.Second)

	// 后台 500 后旧内容仍在：继续陈旧服务（flight 已拆，会再触发一轮，允许）。
	waitUntil(t, func() bool {
		got := h.get("s5", "zh", "")
		return got.status == 200 && got.body == prime.body
	}, time.Second)
}

func TestSWRBackgroundNetworkErrorKeepsStale(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	ctrl := origin.New()
	osrv := httptest.NewServer(ctrl.Handler())

	p, err := proxy.New(osrv.URL, proxy.WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	psrv := httptest.NewServer(p)
	t.Cleanup(psrv.Close)

	ctrl.SetSpec("sn", swrSpec(10, 30, 1))
	// 预热（直接通过代理）。
	warm := mustGet(t, psrv.URL+"/assets/sn", "zh", "")
	oldBody := readBody(warm)

	clk.Advance(15 * time.Second)
	osrv.Close() // 之后回源必然网络错误

	// SWR 路径立即陈旧；后台刷新网络错误不得抹去旧内容。
	r := mustGet(t, psrv.URL+"/assets/sn", "zh", "")
	b := readBody(r)
	if r.StatusCode != 200 || r.Header.Get("X-Cache-Status") != "stale" || b != oldBody {
		t.Fatalf("network-error background want stale old body, got %d/%q body=%q old=%q",
			r.StatusCode, r.Header.Get("X-Cache-Status"), b, oldBody)
	}
	// 旧内容仍可用：再来一次依旧陈旧服务，而不是 502。
	r2 := mustGet(t, psrv.URL+"/assets/sn", "zh", "")
	if r2.StatusCode != 200 || readBody(r2) != oldBody {
		t.Fatalf("after network failure old content must remain servable, got %d", r2.StatusCode)
	}
}

// ---------- 取消：客户端断开不中断共享的后台刷新 ----------

func TestSWRClientCancelDoesNotStopRefresh(t *testing.T) {
	h := newHarness(t)
	h.setSpec("sx", swrSpec(10, 30, 1))
	prime := h.get("sx", "zh", "")

	h.clock.Advance(15 * time.Second)
	h.ctrl.CloseGate()

	ctx, cancel := context.WithCancel(context.Background())
	ch := h.getAsync(ctx, "sx", "zh")
	waitUntil(t, func() bool { return h.count("zh") >= 2 }, time.Second)
	// SWR 响应本就会立即返回；拿到陈旧响应后取消客户端。
	r := <-ch
	assertStaleMarkers(t, r, prime.body)
	cancel()

	// 后台刷新必须仍在闸门处排队，未因取消而消失。
	time.Sleep(100 * time.Millisecond)
	if n := h.count("zh"); n != 2 {
		t.Fatalf("cancel must not abort background refresh, total=%d", n)
	}

	h.setSpec("sx", swrSpec(10, 30, 2))
	h.ctrl.OpenGate()
	waitUntil(t, func() bool {
		got := h.get("sx", "zh", "")
		return strings.Contains(got.body, "v2")
	}, time.Second)
}

// 加入同一轮的条件等待者取消，也不影响 leader 与其他等待者。
func TestSWRConditionalWaiterCancel(t *testing.T) {
	h := newHarness(t)
	h.setSpec("swc", swrSpec(10, 30, 1))
	prime := h.get("swc", "zh", "")
	etag := prime.header.Get("ETag")

	h.clock.Advance(15 * time.Second)
	h.ctrl.CloseGate()
	h.get("swc", "zh", "") // 触发后台刷新
	waitUntil(t, func() bool { return h.count("zh") >= 2 }, time.Second)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ch1 := h.getAsyncINM(ctx1, "swc", "zh", etag)
	ch2 := h.getAsyncINM(ctx2, "swc", "zh", etag)

	cancel1()
	if got := <-ch1; got.status != 0 {
		t.Fatalf("canceled conditional waiter want transport error, got %d", got.status)
	}
	time.Sleep(100 * time.Millisecond)
	if n := h.count("zh"); n != 2 {
		t.Fatalf("waiter cancel must not add rounds, total=%d", n)
	}

	h.ctrl.OpenGate()
	got := <-ch2
	if got.status != 304 {
		t.Fatalf("surviving conditional waiter want 304, got %d", got.status)
	}
}

// ---------- 未声明指令：保持原有同步再验证/兜底行为 ----------

func TestNoSWRKeepsSynchronousRevalidation(t *testing.T) {
	h := newHarness(t)
	h.setSpec("nn", origin.AssetSpec{MaxAge: 10, HasETag: true, Version: 1})
	prime := h.get("nn", "zh", "")
	if strings.Contains(prime.header.Get("Cache-Control"), "stale-while-revalidate") {
		t.Fatalf("response must not contain swr directive: %q", prime.header.Get("Cache-Control"))
	}

	h.clock.Advance(15 * time.Second)
	h.ctrl.CloseGate()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := h.getAsync(ctx, "nn", "zh")
	waitUntil(t, func() bool { return h.count("zh") >= 2 }, time.Second)
	// 同步路径必须被闸门阻塞，而不是立即陈旧返回。
	select {
	case got := <-ch:
		t.Fatalf("without swr directive request must block synchronously, got %+v", got)
	case <-time.After(150 * time.Millisecond):
	}

	h.ctrl.OpenGate()
	got := <-ch
	if got.status != 200 || got.header.Get("X-Cache-Status") != "revalidated" {
		t.Fatalf("sync revalidation after gate want 200/revalidated, got %d/%q",
			got.status, got.header.Get("X-Cache-Status"))
	}
}

// 显式 swr=0：仅在恰好过期那一刻陈旧，1 秒后回落到同步语义。
func TestSWRExplicitZeroBoundary(t *testing.T) {
	h := newHarness(t)
	h.setSpec("s0", origin.AssetSpec{MaxAge: 10, HasETag: true, Version: 1, SWR: intp(0)})
	prime := h.get("s0", "zh", "")
	if !strings.Contains(prime.header.Get("Cache-Control"), "stale-while-revalidate=0") {
		t.Fatalf("explicit swr=0 must be emitted verbatim, got %q", prime.header.Get("Cache-Control"))
	}

	// 恰好 age==maxAge：swr=0 窗口成立，立即陈旧。
	h.clock.Advance(10 * time.Second)
	h.ctrl.CloseGate()
	r := h.get("s0", "zh", "")
	assertStaleMarkers(t, r, prime.body)
	waitUntil(t, func() bool { return h.count("zh") >= 2 }, time.Second)
	h.ctrl.OpenGate()
	waitUntil(t, func() bool {
		return h.get("s0", "zh", "").header.Get("X-Cache-Status") == "hit"
	}, time.Second)

	// 重新填充后过期 1 秒：swr=0 已出窗，回落到同步（5xx 时 error-stale）。
	h.get("s0", "zh", "")
	h.clock.Advance(11 * time.Second)
	h.setFail("s0", true)
	r2 := h.get("s0", "zh", "")
	if r2.header.Get("X-Cache-Status") != "stale" || r2.header.Get("X-Cache-Error") == "" {
		t.Fatalf("out of zero swr window want synchronous error-stale, got %q err=%q",
			r2.header.Get("X-Cache-Status"), r2.header.Get("X-Cache-Error"))
	}
}

// ---------- 并发：双语同时刷新各只一轮；单语言突发只一轮 ----------

func TestSWRConcurrentTwoLanguages(t *testing.T) {
	h := newHarness(t)
	h.setSpec("ss", swrSpec(10, 30, 1))
	h.get("ss", "zh", "")
	h.get("ss", "en", "")
	h.clock.Advance(15 * time.Second)
	h.ctrl.CloseGate()

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = h.get("ss", "zh", "") }()
		go func() { defer wg.Done(); _ = h.get("ss", "en", "") }()
	}
	wg.Wait()
	waitUntil(t, func() bool { return h.count("zh") >= 2 && h.count("en") >= 2 }, time.Second)
	time.Sleep(100 * time.Millisecond)
	if h.count("zh") != 2 || h.count("en") != 2 {
		t.Fatalf("each language must have exactly one background round, got zh=%d en=%d",
			h.count("zh"), h.count("en"))
	}
	h.ctrl.OpenGate()
}

// SWR 窗口内的并发突发必须合并成一次后台回源；即使源站一直 500，
// 突发结束后旧内容也依然可用。用闸门确定性地挂住本轮唯一回源。
func TestSWRBurstCoalescesToOneBackgroundFetch(t *testing.T) {
	h := newHarness(t)
	h.setSpec("sm", swrSpec(10, 30, 1))
	prime := h.get("sm", "zh", "")
	h.clock.Advance(15 * time.Second)
	h.setFail("sm", true)

	h.ctrl.CloseGate()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = h.get("sm", "zh", "") }()
	}
	wg.Wait() // 全部 SWR 立即陈旧返回；恰好一个后台回源挂在闸门

	waitUntil(t, func() bool { return h.count("zh") == 2 }, time.Second)
	time.Sleep(100 * time.Millisecond)
	if n := h.count("zh"); n != 2 {
		t.Fatalf("SWR burst must coalesce to one background fetch, total=%d", n)
	}
	if h.ctrl.SnapshotStats().Status["zh:500"] != 0 {
		t.Fatal("gated fetch must not have completed yet")
	}

	h.ctrl.OpenGate()
	waitUntil(t, func() bool {
		return h.ctrl.SnapshotStats().Status["zh:500"] == 1
	}, time.Second)

	// 500 不抹旧内容：仍可陈旧服务。
	if r := h.get("sm", "zh", ""); r.status != 200 || r.body != prime.body {
		t.Fatalf("old content must remain servable after 500, got %d body=%q", r.status, r.body)
	}
}
