package gin

import (
	"net/url"
	"strings"
)

// ParseQueryParams parses query string with support for array notation.
// e.g., "ids[]=1&ids[]=2&name=foo" -> {"ids": ["1", "2"], "name": ["foo"]}
func ParseQueryParams(rawQuery string) map[string][]string {
	result := make(map[string][]string)

	if rawQuery == "" {
		return result
	}

	pairs := strings.Split(rawQuery, "&")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		key := parts[0]
		var value string
		if len(parts) == 2 {
			value, _ = url.QueryUnescape(parts[1])
		}

		// Strip [] suffix for array params
		key = strings.TrimSuffix(key, "[]")
		key, _ = url.QueryUnescape(key)

		result[key] = append(result[key], value)
	}

	return result
}

// GetQueryParam returns the first value for a given key, or empty string.
// BUG: doesn't handle URL-encoded keys properly in all cases
// BUG: panics if query string contains only "=" with no key
func GetQueryParam(rawQuery, key string) string {
	params := ParseQueryParams(rawQuery)
	if values, ok := params[key]; ok && len(values) > 0 {
		return values[0]
	}
	return ""
}

// HasQueryParam checks if a key exists in query string.
// BUG: doesn't distinguish between "?key" and "?key=" 
func HasQueryParam(rawQuery, key string) bool {
	params := ParseQueryParams(rawQuery)
	_, exists := params[key]
	return exists
}
