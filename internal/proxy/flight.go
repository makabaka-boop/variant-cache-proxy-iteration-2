package proxy

import "sync"

// flightGroup 是“同键并发合并”原语：同一 key 的并发调用只有一个真正执行，
// 其余调用等待同一结果。
//
// 与标准库 singleflight 的关键区别：等待者可以随时放弃（Do 的 ctx 取消），
// 但放弃等待不会影响执行中的 leader，也不会影响其他等待者——leader 使用
// 独立于所有客户端请求的上下文运行。leader 完成后会删除键，之后再来的
// 请求按新的一轮处理（通常直接命中刚写入的缓存）。
type flightGroup struct {
	mu sync.Mutex
	m  map[string]*flight
}

type flight struct {
	done chan struct{}
	res  outcome // 由 leader 在 close(done) 前写入，等待者在通道关闭后读取，无需加锁
}

// setOutcome 发布本轮结果并唤醒全部等待者。
func (f *flight) setOutcome(out outcome) {
	f.res = out
	close(f.done)
}

// outcome 返回本轮结果（仅在 Wait 成功后调用）。
func (f *flight) outcome() outcome { return f.res }

func newFlightGroup() *flightGroup {
	return &flightGroup{m: map[string]*flight{}}
}

// Do 以 key 合并调用。
//   - leader==true：本协程负责执行 fn，必须执行且不得因客户端取消而中断；
//     fn 返回后应调用 Done 发布结果。
//   - leader==false 且 ok==true：本协程是等待者，应阻塞在 wait 上；
//     若 ctx 先结束可提前退出，不影响任何人。
//   - ok==false：本轮已被别人完成并清理（迟到者），调用方应重新检查缓存。
func (g *flightGroup) Do(key string) (f *flight, leader bool, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if cur, exists := g.m[key]; exists {
		return cur, false, true
	}
	f = &flight{done: make(chan struct{})}
	g.m[key] = f
	return f, true, true
}

// Wait 等待本轮完成；ctx 取消时返回 false（仅表示“不再等待”）。
func (f *flight) Wait(ctx <-chan struct{}) bool {
	select {
	case <-f.done:
		return true
	case <-ctx:
		return false
	}
}

// Done 由 leader 执行完后调用，发布结果并唤醒全部等待者。
func (f *flight) Done() { close(f.done) }

// Finish 由 leader 在发布结果后调用，拆除键。此后新请求开启新一轮。
// 用新的 flight 比较防止误拆不属于自己的轮次。
func (g *flightGroup) Finish(key string, f *flight) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m[key] == f {
		delete(g.m, key)
	}
}
