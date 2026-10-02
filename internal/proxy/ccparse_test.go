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
	}{
		{"happy", func(h http.Header) {}, true, 30},
		{"max-age zero", func(h http.Header) { h.Set("Cache-Control", "max-age=0") }, true, 0},
		{"max-age sixty", func(h http.Header) { h.Set("Cache-Control", "max-age=60") }, true, 60},
		{"max-age sixty-one", func(h http.Header) { h.Set("Cache-Control", "max-age=61") }, false, 0},
		{"max-age negative", func(h http.Header) { h.Set("Cache-Control", "max-age=-1") }, false, 0},
		{"no max-age", func(h http.Header) { h.Set("Cache-Control", "public") }, false, 0},
		{"no-store", func(h http.Header) { h.Add("Cache-Control", "no-store") }, false, 0},
		{"private", func(h http.Header) { h.Add("Cache-Control", "private") }, false, 0},
		{"no-cache means ttl zero", func(h http.Header) { h.Set("Cache-Control", "no-cache") }, true, 0},
		{"no etag", func(h http.Header) { h.Del("ETag") }, false, 0},
		{"vary missing denied", func(h http.Header) { h.Del("Vary") }, false, 0},
		{"vary star denied", func(h http.Header) { h.Set("Vary", "*") }, false, 0},
		{"vary cookie denied", func(h http.Header) { h.Set("Vary", "Accept-Language, Cookie") }, false, 0},
		{"vary case-insensitive allowed", func(h http.Header) { h.Set("Vary", "accept-language") }, true, 30},
		{"vary multi-header", func(h http.Header) {
			h.Add("Vary", "Accept-Language")
			h.Set("Vary", "Accept-Language, Accept-Encoding")
		}, false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := base()
			c.mut(h)
			ttl, ok := cacheable(h)
			if ok != c.ok || (ok && ttl != c.ttl) {
				t.Fatalf("cacheable = ttl:%d ok:%v, want ttl:%d ok:%v", ttl, ok, c.ttl, c.ok)
			}
		})
	}
}

func TestStaleWhileRevalidate(t *testing.T) {
	cases := []struct {
		name    string
		cc      string
		want    int
		present bool
	}{
		{"absent", "public, max-age=10", 0, false},
		{"explicit zero", "max-age=10, stale-while-revalidate=0", 0, true},
		{"five seconds", "max-age=10, stale-while-revalidate=5", 5, true},
		{"case insensitive", "max-age=10, STALE-WHILE-REVALIDATE=7", 7, true},
		{"missing value ignored", "max-age=10, stale-while-revalidate", 0, false},
		{"negative ignored", "max-age=10, stale-while-revalidate=-1", 0, false},
		{"garbage ignored", "max-age=10, stale-while-revalidate=abc", 0, false},
		{"multiple headers last wins", "stale-while-revalidate=3", 0, false}, // 仅一条头
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := http.Header{}
			h.Set("Cache-Control", c.cc)
			if c.name == "multiple headers last wins" {
				h.Add("Cache-Control", "stale-while-revalidate=9")
				got, present := staleWhileRevalidate(h)
				if !present || got != 9 {
					t.Fatalf("multi-header swr want present/9, got %d/%v", got, present)
				}
				return
			}
			got, present := staleWhileRevalidate(h)
			if present != c.present || (present && got != c.want) {
				t.Fatalf("swr(%q) = %d,%v want %d,%v", c.cc, got, present, c.want, c.present)
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

func TestSWRWindowBoundaries(t *testing.T) {
	clk := newFakeClock()
	p := &Proxy{clock: clk, store: map[string]*entry{}}

	// 未声明 SWR 的条目：任何时刻都不进入后台更新窗口。
	plain := &entry{maxAge: 10, storedAt: clk.Now(), swrSet: false, swr: 5}
	clk.Advance(10 * time.Second)
	if p.withinSWR(plain) {
		t.Fatal("entry without explicit swr must never enter SWR window")
	}

	// 显式 swr=5：maxAge、maxAge+5s 在窗内，再多 1 秒出窗。
	swr := &entry{maxAge: 10, storedAt: clk.Now(), swrSet: true, swr: 5}
	if p.withinSWR(swr) {
		t.Fatal("fresh entry must not be within SWR window")
	}
	clk.Advance(10 * time.Second)
	if !p.withinSWR(swr) {
		t.Fatal("at exactly maxAge must be within SWR window")
	}
	clk.Advance(5 * time.Second)
	if !p.withinSWR(swr) {
		t.Fatal("at exactly maxAge+swr must still be within window")
	}
	clk.Advance(time.Second)
	if p.withinSWR(swr) {
		t.Fatal("beyond maxAge+swr must leave SWR window")
	}

	// 显式 swr=0：仅 age==maxAge 一刻成立（用独立时钟，起点为零点）。
	c0 := newFakeClock()
	p0 := &Proxy{clock: c0, store: map[string]*entry{}}
	zero := &entry{maxAge: 10, storedAt: c0.Now(), swrSet: true, swr: 0}
	if p0.withinSWR(zero) {
		t.Fatal("fresh swr=0 entry must not be within window")
	}
	c0.Advance(10 * time.Second)
	if !p0.withinSWR(zero) {
		t.Fatal("at exactly maxAge swr=0 must be within window")
	}
	c0.Advance(time.Second)
	if p0.withinSWR(zero) {
		t.Fatal("one second past maxAge swr=0 must leave window")
	}
}
