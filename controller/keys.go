package controller

import (
	"bytes"
	"io"
	"strings"
	"time"

	"github.com/google/btree"
)

func (c *Controller) cmdKeys(line string, w io.Writer) error {
	var pattern string
	if line, pattern = token(line); pattern == "" {
		return errInvalidNumberOfArguments
	}
	if line != "" {
		return errInvalidNumberOfArguments
	}
	var start = time.Now()
	var wr = &bytes.Buffer{}
	var once bool
	wr.WriteString(`{"ok":true,"keys":[`)

	var everything bool
	var greater bool
	var greaterPivot string

	iterator := func(item btree.Item) bool {

		key := item.(*collectionT).Key
		var match bool
		if everything {
			match = true
		} else if greater {
			if !strings.HasPrefix(key, greaterPivot) {
				return false
			}
			match = true
		} else {
			match, _ = globMatch(pattern, key)
		}
		if match {
			if once {
				wr.WriteByte(',')
			} else {
				once = true
			}
			s := jsonString(key)
			wr.WriteString(s)
		}
		return true
	}
	if pattern == "*" {
		everything = true
		c.cols.Ascend(iterator)
	} else {
		if strings.HasSuffix(pattern, "*") {
			greaterPivot = pattern[:len(pattern)-1]
			if globIsGlob(greaterPivot) {
				greater = false
				c.cols.Ascend(iterator)
			} else {
				greater = true
				c.cols.AscendGreaterOrEqual(&collectionT{Key: greaterPivot}, iterator)
			}
		} else if globIsGlob(pattern) {
			greater = false
			c.cols.Ascend(iterator)
		} else {
			greater = true
			greaterPivot = pattern
			c.cols.AscendGreaterOrEqual(&collectionT{Key: greaterPivot}, iterator)
		}
	}
	wr.WriteString(`],"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	w.Write(wr.Bytes())
	return nil
}
