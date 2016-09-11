package controller

import (
	"encoding/json"

	"github.com/tidwall/cast"
)

func jsonString(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] == '\\' || s[i] == '"' || s[i] > 126 {
			d, _ := json.Marshal(s)
			return string(d)
		}
	}
	b := make([]byte, len(s)+2)
	b[0] = '"'
	copy(b[1:], cast.ToBytes(s))
	b[len(b)-1] = '"'
	return cast.ToString(b)
}
