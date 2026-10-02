package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cacheproxy/internal/lang"
)

// guardedHarness 构造时钟、可控源站与代理（白盒包内可直接操作 store）。
type guardedHarness struct {
	clk    *fakeClock
	origin *httptest.Server
	p      *Proxy
	hits   *atomic.Int64 // 源站已进入 handler 的请求数（与 handler 闭包共享）
}

func newGuardedHarness(t *testing.T, hf func(w http.ResponseWriter, r *http.Request, hits *atomic.Int64)) *guardedHarness {
	t.Helper()
	clk := newFakeClock()
	hits := &atomic.Int64{} // 必须在 handler 闭包与 harness 间共享同一指针
	osrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		hf(w, r, hits)
	}))
	t.Cleanup(osrv.Close)
	p, err := New(osrv.URL, WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	return &guardedHarness{clk: clk, origin: osrv, p: p, hits: hits}
}

// waitHit 等待源站收到第 n 个请求（即后台回源已进入 handler、阻塞在 gate 前）。
func (h *guardedHarness) waitHit(t *testing.T, n int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.hits.Load() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("origin did not receive %d request(s), got %d", n, h.hits.Load())
}

func mkEntry(clk *fakeClock, maxAge, swr int, etag, body string) *entry {
	h := http.Header{}
	h.Set("ETag", etag)
	h.Set("Cache-Control",
		"max-age="+strconv.Itoa(maxAge)+", stale-while-revalidate="+strconv.Itoa(swr))
	h.Set("Vary", "Accept-Language")
	h.Set("Content-Language", "zh")
	return &entry{
		header:   h,
		body:     []byte(body),
		etag:     etag,
		maxAge:   maxAge,
		storedAt: clk.Now(),
		swrSet:   true,
		swr:      swr,
	}
}

// startBackground 以指定 base 条目启动一轮后台刷新。
func (h *guardedHarness) startBackground(t *testing.T, id string, base *entry) {
	t.Helper()
	key := cacheKey(id, lang.ZH)
	f, leader, ok := h.p.flights.Do(key)
	if !ok || !leader {
		t.Fatal("must become leader")
	}
	go h.p.runBackgroundRefresh(key, id, lang.ZH, base, f)
}

func (h *guardedHarness) key(id string) string { return cacheKey(id, lang.ZH) }

func waitUntilStore(t *testing.T, p *Proxy, key string, cond func(*entry) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e := p.lookup(key); e != nil && cond(e) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("store condition not met before timeout: entry=%+v", p.lookup(key))
}

// TestGuardedCommitRejectsLate304：后台条件回源基于 v1 条目；提交前缓存已被
// 更新为 v2（指针不同）。迟到的 304 续期必须被 swapIf 拒绝，当前条目仍是 v2。
func TestGuardedCommitRejectsLate304(t *testing.T) {
	const oldETag = `"g-zh-v1"`
	const newETag = `"g-zh-v2"`
	gate := make(chan struct{})
	h := newGuardedHarness(t, func(w http.ResponseWriter, r *http.Request, _ *atomic.Int64) {
		<-gate
		if r.Header.Get("If-None-Match") == oldETag {
			w.Header().Set("Cache-Control", "max-age=10, stale-while-revalidate=30")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "unexpected")
	})

	key := h.key("g")
	old := mkEntry(h.clk, 10, 30, oldETag, "body-v1")
	h.clk.Advance(15 * time.Second) // 过期、SWR 窗内
	h.p.put(key, old)

	h.startBackground(t, "g", old)
	h.waitHit(t, 1)

	// 提交前缓存条目已被其他路径更新为 v2。
	fresh2 := mkEntry(h.clk, 10, 30, newETag, "body-v2")
	fresh2.storedAt = h.clk.Now()
	h.p.put(key, fresh2)

	close(gate) // 放行迟到的 304 刷新

	waitUntilStore(t, h.p, key, func(e *entry) bool {
		return e == fresh2 && e.etag == newETag && string(e.body) == "body-v2"
	})
}

// TestGuardedCommitRejectsLate200：迟到的新 200（v2 内容）不得覆盖更新的 v3。
func TestGuardedCommitRejectsLate200(t *testing.T) {
	const baseETag = `"g2-zh-v1"`
	const lateETag = `"g2-zh-v2"`
	const curETag = `"g2-zh-v3"`
	gate := make(chan struct{})
	h := newGuardedHarness(t, func(w http.ResponseWriter, r *http.Request, _ *atomic.Int64) {
		<-gate
		if r.Header.Get("If-None-Match") == baseETag {
			w.Header().Set("ETag", lateETag)
			w.Header().Set("Cache-Control", "max-age=10, stale-while-revalidate=30")
			w.Header().Set("Vary", "Accept-Language")
			w.Header().Set("Content-Language", "zh")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "body-v2")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "unexpected")
	})

	key := h.key("g2")
	old := mkEntry(h.clk, 10, 30, baseETag, "body-v1")
	h.clk.Advance(15 * time.Second)
	h.p.put(key, old)

	h.startBackground(t, "g2", old)
	h.waitHit(t, 1)

	v3 := mkEntry(h.clk, 10, 30, curETag, "body-v3")
	v3.storedAt = h.clk.Now()
	h.p.put(key, v3)

	close(gate)
	waitUntilStore(t, h.p, key, func(e *entry) bool {
		return e == v3 && e.etag == curETag && string(e.body) == "body-v3"
	})
}

// TestGuarded4xxDeleteDoesNotRemoveNewer：迟到 4xx 只能作废旧条目本身，
// 不能删掉已经更新的当前条目。
func TestGuarded4xxDeleteDoesNotRemoveNewer(t *testing.T) {
	const baseETag = `"g3-zh-v1"`
	gate := make(chan struct{})
	h := newGuardedHarness(t, func(w http.ResponseWriter, r *http.Request, _ *atomic.Int64) {
		<-gate
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "gone")
	})

	key := h.key("g3")
	old := mkEntry(h.clk, 10, 30, baseETag, "body-v1")
	h.clk.Advance(15 * time.Second)
	h.p.put(key, old)

	h.startBackground(t, "g3", old)
	h.waitHit(t, 1)

	newer := mkEntry(h.clk, 10, 30, `"g3-zh-v2"`, "body-v2")
	newer.storedAt = h.clk.Now()
	h.p.put(key, newer)

	close(gate)
	// 当前条目必须仍在（未被迟到 404 误删）。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if e := h.p.lookup(key); e == newer && h.hits.Load() >= 1 {
			// 再稳定观察一会儿，确认没有延迟删除。
			time.Sleep(50 * time.Millisecond)
			if e := h.p.lookup(key); e == newer {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("late 404 must not delete newer entry, got %v", h.p.lookup(key))
}

// TestGuardedCommitAcceptsWhenStillCurrent：条目未被替换时，304 正常提交续期。
func TestGuardedCommitAcceptsWhenStillCurrent(t *testing.T) {
	const etag = `"g4-zh-v1"`
	gate := make(chan struct{})
	h := newGuardedHarness(t, func(w http.ResponseWriter, r *http.Request, _ *atomic.Int64) {
		<-gate
		w.Header().Set("Cache-Control", "max-age=10, stale-while-revalidate=30")
		w.WriteHeader(http.StatusNotModified)
	})
	key := h.key("g4")
	old := mkEntry(h.clk, 10, 30, etag, "body-v1")
	h.clk.Advance(15 * time.Second)
	h.p.put(key, old)

	h.startBackground(t, "g4", old)
	h.waitHit(t, 1)
	close(gate)

	waitUntilStore(t, h.p, key, func(e *entry) bool {
		// 304 续期生成新指针，内容/ETag 沿用，storedAt 刷新到当前时钟。
		return e != nil && e != old && e.etag == etag && e.storedAt.Equal(h.clk.Now())
	})
}
