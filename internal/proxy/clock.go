package proxy

import "time"

// Clock 抽象“当前时间”，生产环境用系统时钟，测试注入可控时钟。
// 缓存所有新鲜期/陈旧期判断都只依赖该接口，测试无需真实 sleep。
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// fakeClock 是测试用时钟，可手动推进。
type fakeClock struct {
	t time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
