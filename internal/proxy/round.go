package proxy

import "sync"

// roundRole 标识一轮回源的服务方式。
type roundRole int

const (
	roundAsync roundRole = iota // 后台 stale-while-revalidate 条件回源
	roundSync                   // 同步再验证（等待者需要可应答的 outcome）
)

// round 是某个缓存键“正在进行中的唯一一轮回源”。
// 同一 (资源, 变体) 任意时刻至多一轮：后台异步轮与同步轮在 roundGroup
// 的同一把锁下原子互斥，因此两者不可能并发地重复回源。
type round struct {
	role roundRole
	done chan struct{} // 提交结果（异步轮）或发布 outcome（同步轮）后关闭
	res  outcome       // 仅同步轮：leader 在 close(done) 前写入
}

func (r *round) setOutcome(out outcome) {
	r.res = out
	close(r.done)
}

// wait 等待本轮结束；等待方放弃（ctx 关闭）时返回 false，不影响轮次本身。
func (r *round) wait(ctx <-chan struct{}) bool {
	select {
	case <-r.done:
		return true
	case <-ctx:
		return false
	}
}

// roundGroup 维护“每键至多一轮在飞回源”。
type roundGroup struct {
	mu sync.Mutex
	m  map[string]*round
}

func newRoundGroup() *roundGroup {
	return &roundGroup{m: map[string]*round{}}
}

// startAsyncIfFree 原子地尝试登记一轮后台刷新。
// 该键已有任何一轮（异步或同步）时返回 false，调用方不得再发起回源。
func (g *roundGroup) startAsyncIfFree(key string) (*round, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, exists := g.m[key]; exists {
		return nil, false
	}
	r := &round{role: roundAsync, done: make(chan struct{})}
	g.m[key] = r
	return r, true
}

// joinSync 加入或开启该键的同步再验证。
//   - 返回 leader 轮：调用方负责回源、setOutcome，并在之后 finish；
//   - 返回已在飞的同步轮：调用方作为等待者阻塞在 wait 上，随后读 res；
//   - 返回一个“刚好结束”的异步轮：调用方等待结束后重新 joinSync，
//     本方法会在同一把锁内把它原子替换为新的同步轮（leader）——
//     替换瞬间没有空窗，SWR 请求不可能在两者之间再插一轮后台刷新。
func (g *roundGroup) joinSync(key string) (*round, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	cur, exists := g.m[key]
	if !exists {
		r := &round{role: roundSync, done: make(chan struct{})}
		g.m[key] = r
		return r, true
	}
	if cur.role == roundSync {
		return cur, false
	}
	// 异步轮：仅当它确实已结束，才原子接管为同步轮。
	select {
	case <-cur.done:
		r := &round{role: roundSync, done: make(chan struct{})}
		g.m[key] = r
		return r, true
	default:
		return cur, false
	}
}

// current 返回该键当前在飞的轮次（无则 nil），不改变状态。
func (g *roundGroup) current(key string) *round {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.m[key]
}

// finish 拆除键；指针比较防止误拆/误删后来接管的轮次。
func (g *roundGroup) finish(key string, r *round) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m[key] == r {
		delete(g.m, key)
	}
}
