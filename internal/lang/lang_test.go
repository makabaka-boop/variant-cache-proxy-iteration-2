package lang

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		header string
		want   Variant
		ok     bool
	}{
		{"zh", ZH, true},
		{"en", EN, true},
		{"zh-CN", ZH, true},
		{"en-US,en;q=0.9", EN, true},
		{"fr, en;q=0.8, zh;q=0.5", EN, true}, // 按 q 值选最高的受支持语言
		{"fr;q=0.9, zh;q=0.8", ZH, true},
		{"en;q=0, zh;q=0.1", ZH, true}, // q=0 表示拒绝
		{"fr", "", false},
		{"ja, ko", "", false},
		{"", "", false},
		{"*", "", false},           // 通配符不能隐式选定缓存变体
		{"en;q=bad, zh", EN, true}, // 畸形 q 值被忽略，按默认 q=1 与出现顺序
	}
	for _, c := range cases {
		got, ok := Parse(c.header)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("Parse(%q) = %q,%v want %q,%v", c.header, got, ok, c.want, c.ok)
		}
	}
}
