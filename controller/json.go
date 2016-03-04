package controller

import "encoding/json"

func jsonString(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] < 32 || s[i] > 126 {
			d, _ := json.Marshal(s)
			return string(d)
		}
	}
	return `"` + s + `"`
}
