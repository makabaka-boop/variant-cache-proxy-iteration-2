package proxy

import (
	"net/http"
	"strconv"
	"strings"
)

// cacheDirectives 是从 Cache-Control 解析出的、与缓存决策相关的指令。
type cacheDirectives struct {
	noStore bool
	private bool
	noCache bool
	maxAge  *int
}

// parseCacheControl 解析响应的 Cache-Control 头（多条逗号合并）。
// 只识别本代理支持的指令，其余忽略。
func parseCacheControl(header http.Header) cacheDirectives {
	var d cacheDirectives
	for _, h := range header.Values("Cache-Control") {
		for _, part := range strings.Split(h, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			token := part
			value := ""
			if i := strings.IndexByte(part, '='); i >= 0 {
				token = strings.TrimSpace(part[:i])
				value = strings.TrimSpace(part[i+1:])
			}
			switch strings.ToLower(token) {
			case "no-store":
				d.noStore = true
			case "private":
				d.private = true
			case "no-cache":
				d.noCache = true
			case "max-age":
				if n, err := strconv.Atoi(value); err == nil {
					d.maxAge = &n
				}
			}
		}
	}
	return d
}

// varyAllowsCaching 判断 Vary 头是否恰好声明 Accept-Language。
// 规则（宁漏勿错：缓存键含语言，未显式声明语言差异的响应不进缓存）：
//   - Vary 缺失：禁止（语言中性的内容不值得冒串语言的风险，逐请求透传）；
//   - Vary: *：禁止；
//   - 含任意非 Accept-Language 字段：禁止；
//   - 至少出现一次 Accept-Language，且没有其他字段：允许。
func varyAllowsCaching(header http.Header) bool {
	values := header.Values("Vary")
	if len(values) == 0 {
		return false
	}
	sawLanguage := false
	for _, v := range values {
		for _, field := range strings.Split(v, ",") {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if strings.EqualFold(field, "Accept-Language") {
				sawLanguage = true
				continue
			}
			return false
		}
	}
	return sawLanguage
}

// cacheable 判定一个 200 响应能否被存入缓存。
// 要求：带 ETag；有明确 max-age 且 0 <= max-age <= 60；
// 无 no-store/private；Vary 只允许 Accept-Language。
// 通过时返回的 ttl 为新鲜期秒数（no-cache 等价于 max-age=0）。
func cacheable(h http.Header) (ttl int, ok bool) {
	d := parseCacheControl(h)
	if d.noStore || d.private {
		return 0, false
	}
	if h.Get("ETag") == "" {
		return 0, false
	}
	if !varyAllowsCaching(h) {
		return 0, false
	}
	if d.noCache {
		return 0, true
	}
	if d.maxAge == nil {
		return 0, false
	}
	if *d.maxAge < 0 || *d.maxAge > 60 {
		return 0, false
	}
	return *d.maxAge, true
}

// etagStrongMatch 判断客户端的 If-None-Match 是否命中某个 ETag。
// 支持多个 ETag 与 *。源站/缓存内的 ETag 均为强校验器，直接精确比较。
func etagStrongMatch(ifNoneMatch, etag string) bool {
	if etag == "" {
		return false
	}
	for _, part := range strings.Split(ifNoneMatch, ",") {
		part = strings.TrimSpace(part)
		if part == "*" || part == etag {
			return true
		}
	}
	return false
}
