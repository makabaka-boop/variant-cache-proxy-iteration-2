// Package origin 实现一个“可控源站”：行为可通过 HTTP 控制面或 Go 方法随时调整，
// 供手工验收（Docker Compose）与代理自动化测试共同使用。
//
// 资源接口：
//
//	GET /assets/{id}
//	  - 依据 Accept-Language 选择 zh / en 变体内容，响应始终带 Vary: Accept-Language
//	  - 每个变体拥有独立 ETag，支持 If-None-Match 条件请求（304）
//
// 控制面（仅测试/演练使用）：
//
//	POST /control/reset                     清空统计
//	POST /control/assets/{id}               配置资源（maxAge/hasETag/vary/fail/delayMs）
//	GET  /control/stats                     查看分变体的请求计数与在飞请求数
//	GET  /control/gate/open|close           放行/拦截资源请求（用于并发合并测试）
package origin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cacheproxy/internal/lang"
)

// AssetSpec 描述资源的可配置行为。
type AssetSpec struct {
	MaxAge  int    `json:"maxAge"`  // Cache-Control max-age，秒；<0 表示不发送 Cache-Control
	HasETag bool   `json:"hasETag"` // 是否发送 ETag
	Vary    string `json:"vary"`    // Vary 头内容；空串表示 Accept-Language
	Fail    bool   `json:"fail"`    // 为 true 时对该资源返回 500
	DelayMs int    `json:"delayMs"` // 处理前固定延迟，毫秒
}

// Stats 是源站观测计数，全部按变体统计（unsupported 归入 other）。
type Stats struct {
	Total       map[string]int64 `json:"total"`       // 到达源站的资源请求数
	Status      map[string]int64 `json:"status"`      // 按 HTTP 状态码统计
	InFlight    int64            `json:"inFlight"`    // 正在处理的请求数
	MaxInFlight int64            `json:"maxInFlight"` // 历史峰值在飞数
}

// Controller 是可控源站。
type Controller struct {
	mu     sync.Mutex
	assets map[string]*assetState

	total       map[string]*atomic.Int64 // key: 变体（zh/en/other）
	status      map[string]*atomic.Int64 // key: "variant:code"
	inFlight    atomic.Int64
	maxInFlight atomic.Int64

	gateCh chan struct{} // 非 nil 且未关闭时，资源请求阻塞等待
}

type assetState struct {
	mu   sync.RWMutex
	spec AssetSpec
}

// New 创建可控源站，并写入一组默认资源。
func New() *Controller {
	c := &Controller{
		assets: map[string]*assetState{},
		total:  map[string]*atomic.Int64{},
		status: map[string]*atomic.Int64{},
	}
	return c
}

// DefaultSpec 返回资源的默认配置：max-age=10、带 ETag、Vary: Accept-Language。
func DefaultSpec() AssetSpec {
	return AssetSpec{MaxAge: 10, HasETag: true, Vary: "Accept-Language"}
}

// SetSpec 直接以 Go 代码配置资源（测试使用）。
func (c *Controller) SetSpec(id string, spec AssetSpec) {
	c.mu.Lock()
	c.assets[id] = &assetState{spec: spec}
	c.mu.Unlock()
}

// SetFail 切换资源的 500 故障开关（测试使用）。
func (c *Controller) SetFail(id string, fail bool) {
	c.withAsset(id, func(s *AssetSpec) { s.Fail = fail })
}

// SetMaxAge 调整资源 max-age（测试使用）。
func (c *Controller) SetMaxAge(id string, maxAge int) {
	c.withAsset(id, func(s *AssetSpec) { s.MaxAge = maxAge })
}

// ResetStats 清空全部计数（测试使用）。
func (c *Controller) ResetStats() {
	c.mu.Lock()
	c.total = map[string]*atomic.Int64{}
	c.status = map[string]*atomic.Int64{}
	c.inFlight.Store(0)
	c.maxInFlight.Store(0)
	c.mu.Unlock()
}

// SnapshotStats 返回计数快照（测试使用）。
func (c *Controller) SnapshotStats() Stats {
	c.mu.Lock()
	total := map[string]int64{}
	for k, v := range c.total {
		total[k] = v.Load()
	}
	status := map[string]int64{}
	for k, v := range c.status {
		status[k] = v.Load()
	}
	st := Stats{
		Total:       total,
		Status:      status,
		InFlight:    c.inFlight.Load(),
		MaxInFlight: c.maxInFlight.Load(),
	}
	c.mu.Unlock()
	return st
}

// CloseGate 拦截所有资源请求，直到 OpenGate 放行（测试使用）。
func (c *Controller) CloseGate() {
	c.mu.Lock()
	if c.gateCh == nil {
		c.gateCh = make(chan struct{})
	}
	c.mu.Unlock()
}

// OpenGate 放行了被拦截的资源请求（测试使用）。
func (c *Controller) OpenGate() {
	c.mu.Lock()
	ch := c.gateCh
	c.gateCh = nil
	c.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (c *Controller) withAsset(id string, fn func(*AssetSpec)) {
	c.mu.Lock()
	st, ok := c.assets[id]
	if !ok {
		st = &assetState{spec: DefaultSpec()}
		c.assets[id] = st
	}
	c.mu.Unlock()
	st.mu.Lock()
	fn(&st.spec)
	st.mu.Unlock()
}

// Handler 返回源站的 http.Handler。
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/assets/", c.handleAsset)
	mux.HandleFunc("/control/reset", c.handleReset)
	mux.HandleFunc("/control/assets/", c.handleSetAsset)
	mux.HandleFunc("/control/stats", c.handleStats)
	mux.HandleFunc("/control/gate/", c.handleGate)
	return mux
}

func (c *Controller) handleAsset(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/assets/")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	c.trackFlight(1) // 整个请求期间统计在飞数，供并发合并测试断言
	defer c.trackFlight(-1)

	c.mu.Lock()
	st, ok := c.assets[id]
	if !ok {
		st = &assetState{spec: DefaultSpec()}
		c.assets[id] = st
	}
	c.mu.Unlock()
	spec := st.snapshot()

	variant, supported := lang.Parse(r.Header.Get("Accept-Language"))
	bucket := "other"
	if supported {
		bucket = string(variant)
	} else {
		variant = lang.EN // 不支持的变体回退英文内容，但响应声明不得被缓存
	}
	// 请求已到达源站即计数——即便它正阻塞在测试闸门上。
	c.countStatus(bucket+":total", 1)

	c.enterGate(r) // 测试闸门：关闭时在此阻塞（客户端断开也会解除）
	if r.Context().Err() != nil {
		return
	}

	if spec.DelayMs > 0 {
		select {
		case <-time.After(time.Duration(spec.DelayMs) * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}

	if spec.Fail {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		c.countStatus(bucket+":500", 1)
		_, _ = w.Write([]byte("origin injected failure\n"))
		return
	}

	etag := ""
	if spec.HasETag {
		etag = fmt.Sprintf("%q", id+"-"+string(variant)+"-v1")
	}
	vary := spec.Vary
	if vary == "" {
		vary = "Accept-Language" // 空值即默认：响应随语言变化
	}
	h := w.Header()
	h.Set("Vary", vary)
	h.Set("Content-Language", string(variant))
	h.Set("Content-Type", "text/plain; charset=utf-8")
	if spec.MaxAge >= 0 {
		h.Set("Cache-Control", "public, max-age="+strconv.Itoa(spec.MaxAge))
	}
	if etag != "" {
		h.Set("ETag", etag)
	}
	if !supported {
		// 不支持的语言变体：源站显式禁止缓存。
		h.Set("Cache-Control", "no-store")
	}

	// 条件请求：ETag 按变体独立，因此只有变体匹配时才可能 304。
	if etag != "" && etagIn(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		c.countStatus(bucket+":304", 1)
		return
	}

	w.WriteHeader(http.StatusOK)
	c.countStatus(bucket+":200", 1)
	body := fmt.Sprintf("asset=%s lang=%s v1\n", id, variant)
	_, _ = w.Write([]byte(body))
}

func (st *assetState) snapshot() AssetSpec {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.spec
}

// enterGate 在测试闸门关闭时阻塞；客户端断开时立即返回。
// 在飞计数由 handleAsset 统一负责。
func (c *Controller) enterGate(r *http.Request) {
	c.mu.Lock()
	ch := c.gateCh
	c.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case <-ch:
	case <-r.Context().Done():
	}
}

func (c *Controller) trackFlight(delta int64) {
	cur := c.inFlight.Add(delta)
	if delta > 0 {
		for {
			max := c.maxInFlight.Load()
			if cur <= max || c.maxInFlight.CompareAndSwap(max, cur) {
				break
			}
		}
	}
}

func (c *Controller) countStatus(key string, n int64) {
	c.mu.Lock()
	ctr, ok := c.status[key]
	if !ok {
		ctr = &atomic.Int64{}
		c.status[key] = ctr
	}
	c.mu.Unlock()
	ctr.Add(n)
}

// etagIn 判断 If-None-Match（可能含多个 ETag 或 *）是否命中。
func etagIn(header, etag string) bool {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if part == "*" {
			return true
		}
		if part == etag {
			return true
		}
	}
	return false
}

// ---------- 控制面 ----------

func (c *Controller) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	c.ResetStats()
	w.WriteHeader(http.StatusNoContent)
}

type setAssetReq struct {
	MaxAge  *int    `json:"maxAge"`
	HasETag *bool   `json:"hasETag"`
	Vary    *string `json:"vary"`
	Fail    *bool   `json:"fail"`
	DelayMs *int    `json:"delayMs"`
}

func (c *Controller) handleSetAsset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/control/assets/")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "bad asset id", http.StatusBadRequest)
		return
	}
	var req setAssetReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	st, ok := c.assets[id]
	if !ok {
		st = &assetState{spec: DefaultSpec()}
		c.assets[id] = st
	}
	c.mu.Unlock()

	st.mu.Lock()
	spec := st.spec
	if req.MaxAge != nil {
		spec.MaxAge = *req.MaxAge
	}
	if req.HasETag != nil {
		spec.HasETag = *req.HasETag
	}
	if req.Vary != nil {
		if *req.Vary == "" {
			spec.Vary = "Accept-Language"
		} else {
			spec.Vary = *req.Vary
		}
	}
	if req.Fail != nil {
		spec.Fail = *req.Fail
	}
	if req.DelayMs != nil {
		spec.DelayMs = *req.DelayMs
	}
	st.spec = spec
	st.mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

func (c *Controller) handleStats(w http.ResponseWriter, r *http.Request) {
	st := c.SnapshotStats()
	// total 直接从 status 里的 ":total" 派生，便于外部阅读。
	for k, v := range st.Status {
		if strings.HasSuffix(k, ":total") {
			st.Total[strings.TrimSuffix(k, ":total")] = v
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

func (c *Controller) handleGate(w http.ResponseWriter, r *http.Request) {
	switch strings.TrimPrefix(r.URL.Path, "/control/gate/") {
	case "close":
		c.CloseGate()
		w.WriteHeader(http.StatusNoContent)
	case "open":
		c.OpenGate()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unknown gate action", http.StatusNotFound)
	}
}
