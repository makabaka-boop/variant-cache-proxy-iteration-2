// Package lang 负责把请求方的 Accept-Language 收敛到源站/代理共同支持的两个变体。
//
// 业务只支持两种内容语言变体：zh 与 en。代理缓存以 (资源, 变体) 为键，
// 因此源站与代理必须用同一套规则解释 Accept-Language，避免键的解释不一致。
package lang

import (
	"sort"
	"strconv"
	"strings"
)

// Variant 是收敛后的语言变体。
type Variant string

const (
	ZH Variant = "zh"
	EN Variant = "en"
)

// Parse 按 RFC 7231 解析 Accept-Language（支持 q 值排序），
// 返回首个受支持的变体（zh / en）；没有任何受支持变体时 ok 为 false。
func Parse(acceptLanguage string) (Variant, bool) {
	type tag struct {
		name string
		q    float64
		idx  int
	}

	var tags []tag
	for i, part := range strings.Split(acceptLanguage, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name := part
		q := 1.0
		if semi := strings.IndexByte(part, ';'); semi >= 0 {
			name = strings.TrimSpace(part[:semi])
			for _, p := range strings.Split(part[semi+1:], ";") {
				p = strings.TrimSpace(p)
				if val, ok := cutPrefix(p, "q="); ok {
					if f, err := strconv.ParseFloat(strings.TrimSpace(val), 64); err == nil {
						q = f
					}
				}
			}
		}
		if name != "" {
			tags = append(tags, tag{name: strings.ToLower(name), q: q, idx: i})
		}
	}

	// q 值降序；q 相同保持出现顺序。
	sort.SliceStable(tags, func(i, j int) bool {
		return tags[i].q > tags[j].q
	})

	for _, t := range tags {
		if t.q <= 0 {
			continue
		}
		switch primary(t.name) {
		case "zh":
			return ZH, true
		case "en":
			return EN, true
		case "*":
			// 通配符不允许隐式选定受支持变体：缓存键必须由明确语言决定。
			return "", false
		}
	}
	return "", false
}

func cutPrefix(s, prefix string) (string, bool) {
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):], true
	}
	return s, false
}

// primary 返回语言标签的主语言子串（"-" 之前的部分）。
func primary(tag string) string {
	if i := strings.IndexByte(tag, '-'); i >= 0 {
		return tag[:i]
	}
	return tag
}
