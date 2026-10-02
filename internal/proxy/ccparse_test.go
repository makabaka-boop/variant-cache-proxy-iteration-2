package proxy

import (
	"net/http"
	"testing"
	"time"
)

func TestCacheable(t *testing.T) {
	base := func() http.Header {
		h := http.Header{}
		h.Set("ETag", `"x"`)
		h.Set("Cache-Control", "public, max-age=30")
		h.Set("Vary", "Accept-Language")
		return h
	}

	cases := []struct {
		name string
		mut  func(h http.Header)
		ok   bool
		ttl  int
		swr  int // 期望的 stale-while-revalidate 秒；-1 表示未启用
	}{
		{"happy", func(h http.Header) {}, true, 30, -1},
		{"max-age zero", func(h http.Header) { h.Set("Cache-Control", "max-age=0") }, true, 0, -1},
		{"max-age sixty", func(h http.Header) { h.Set("Cache-Control", "max-age=60") }, true, 60, -1},
		{"max-age sixty-one", func(h http.Header) { h.Set("Cache-Control", "max-age=61") }, false, 0, -1},
		{"max-age negative", func(h http.Header) { h.Set("Cache-Control", "max-age=-1") }, false, 0, -1},
		{"no max-age", func(h http.Header) { h.Set("Cache-Control", "public") }, false, 0, -1},
		{"no-store", func(h http.Header) { h.Add("Cache-Control", "no-store") }, false, 0, -1},
		{"private", func(h http.Header) { h.Add("Cache-Control", "private") }, false, 0, -1},
		{"no-cache means ttl zero", func(h http.Header) { h.Set("Cache-Control", "no-cache") }, true, 0, -1},
		{"no etag", func(h http.Header) { h.Del("ETag") }, false, 0, -1},
		{"vary missing denied", func(h http.Header) { h.Del("Vary") }, false, 0, -1},
		{"vary star denied", func(h http.Header) { h.Set("Vary", "*") }, false, 0, -1},
		{"vary cookie denied", func(h http.Header) { h.Set("Vary", "Accept-Language, Cookie") }, false, 0, -1},
		{"vary case-insensitive allowed", func(h http.Header) { h.Set("Vary", "accept-language") }, true, 30, -1},
		{"vary multi-header", func(h http.Header) {
			h.Add("Vary", "Accept-Language")
			h.Set("Vary", "Accept-Language, Accept-Encoding")
		}, false, 0, -1},
		{"swr enabled", func(h http.Header) {
			h.Set("Cache-Control", "max-age=30, stale-while-revalidate=15")
		}, true, 30, 15},
		{"swr zero is explicit opt-in", func(h http.Header) {
			h.Set("Cache-Control", "max-age=30, stale-while-revalidate=0")
		}, true, 30, 0},
		{"swr negative ignored", func(h http.Header) {
			h.Set("Cache-Control", "max-age=30, stale-while-revalidate=-5")
		}, true, 30, -1},
		{"swr garbage ignored", func(h http.Header) {
			h.Set("Cache-Control", "max-age=30, stale-while-revalidate")
		}, true, 30, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := base()
			c.mut(h)
			pol, ok := cacheable(h)
			if ok != c.ok || (ok && pol.maxAge != c.ttl) {
				t.Fatalf("cacheable = pol:%+v ok:%v, want ttl:%d ok:%v", pol, ok, c.ttl, c.ok)
			}
			if ok {
				swr := -1
				if pol.swrEnabled {
					swr = int(pol.swr / time.Second)
				}
				if swr != c.swr {
					t.Fatalf("swr = %d, want %d", swr, c.swr)
				}
			}
		})
	}
}

func TestEtagStrongMatch(t *testing.T) {
	if !etagStrongMatch(`"a"`, `"a"`) {
		t.Fatal("exact match")
	}
	if !etagStrongMatch(`"b", "a"`, `"a"`) {
		t.Fatal("match within list")
	}
	if !etagStrongMatch(`*`, `"a"`) {
		t.Fatal("wildcard match")
	}
	if etagStrongMatch(`"a"`, `"b"`) {
		t.Fatal("different must not match")
	}
	if etagStrongMatch(`"a"`, "") {
		t.Fatal("empty stored etag must not match")
	}
}

func TestFreshnessBoundaries(t *testing.T) {
	clk := newFakeClock()
	p := &Proxy{clock: clk, store: map[string]*entry{}}
	e := &entry{maxAge: 60, storedAt: clk.Now()}

	if !p.fresh(e) || p.staleUsable(e) {
		// staleUsable 要求 age >= maxAge，新鲜条目不该可“兜底”。
		t.Fatal("fresh entry state wrong")
	}
	clk.Advance(60 * time.Second)
	if p.fresh(e) {
		t.Fatal("at exactly maxAge must be stale")
	}
	if !p.staleUsable(e) {
		t.Fatal("at exactly maxAge must be within stale window")
	}
	clk.Advance(30 * time.Second)
	if !p.staleUsable(e) {
		t.Fatal("at maxAge+30s boundary must still be servable")
	}
	clk.Advance(time.Second)
	if p.staleUsable(e) {
		t.Fatal("past maxAge+30s must not be servable")
	}
}

func TestStaleWhileRevalidatingWindow(t *testing.T) {
	clk := newFakeClock()
	p := &Proxy{clock: clk, store: map[string]*entry{}}

	// 未声明指令：任何时刻都不走 SWR，保持同步再验证。
	off := &entry{maxAge: 10, storedAt: clk.Now()}
	if p.staleWhileRevalidating(off) {
		t.Fatal("without directive SWR must be off even when fresh")
	}
	clk.Advance(20 * time.Second)
	if p.staleWhileRevalidating(off) {
		t.Fatal("without directive SWR must stay off when stale")
	}

	// 声明 swr=15：新鲜期内 false；[maxAge, maxAge+15s] 含两端为 true；再远 false。
	e := &entry{maxAge: 10, swrEnabled: true, swr: 15 * time.Second, storedAt: clk.Now()}
	clk2 := newFakeClock()
	p.clock = clk2
	e.storedAt = clk2.Now()
	clk2.Advance(9 * time.Second)
	if p.staleWhileRevalidating(e) {
		t.Fatal("fresh entry must not be in SWR window")
	}
	clk2.Advance(time.Second) // age == maxAge
	if !p.staleWhileRevalidating(e) {
		t.Fatal("at exactly maxAge SWR must be available")
	}
	clk2.Advance(15 * time.Second) // age == maxAge+swr
	if !p.staleWhileRevalidating(e) {
		t.Fatal("at maxAge+swr boundary SWR must still be available")
	}
	clk2.Advance(time.Second)
	if p.staleWhileRevalidating(e) {
		t.Fatal("past maxAge+swr SWR must be unavailable")
	}

	// swr=0：只有 age == maxAge 那一瞬间在窗口内（显式零值仍属启用）。
	clk3 := newFakeClock()
	p.clock = clk3
	z := &entry{maxAge: 5, swrEnabled: true, swr: 0, storedAt: clk3.Now()}
	clk3.Advance(4 * time.Second)
	if p.staleWhileRevalidating(z) {
		t.Fatal("zero-width SWR window must not include fresh age")
	}
	clk3.Advance(time.Second) // age == maxAge
	if !p.staleWhileRevalidating(z) {
		t.Fatal("zero-width SWR window must include the expiry instant")
	}
	clk3.Advance(time.Second)
	if p.staleWhileRevalidating(z) {
		t.Fatal("zero-width SWR window must end right after expiry")
	}
}
