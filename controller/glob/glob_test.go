package glob

import "testing"

func TestGlob(t *testing.T) {
	test := func(pattern string, desc bool, limitsExpect []string, isGlobExpect bool) {
		g := Parse(pattern, desc)
		if g.IsGlob != isGlobExpect {
			t.Fatalf("pattern[%v] desc[%v] (isGlob=%v, expected=%v)", pattern, desc, g.IsGlob, isGlobExpect)
		}
		if g.Limits[0] != limitsExpect[0] || g.Limits[1] != limitsExpect[1] {
			t.Fatalf("pattern[%v] desc[%v] (limits=%v, expected=%v)", pattern, desc, g.Limits, limitsExpect)
		}
		if g.Pattern != pattern {
			t.Fatalf("pattern[%v] desc[%v] (pattern=%v, expected=%v)", pattern, desc, g.Pattern, pattern)
		}
		if g.Desc != desc {
			t.Fatalf("pattern[%v] desc[%v] (desc=%v, expected=%v)", pattern, desc, g.Desc, desc)
		}
	}
	test("*", false, []string{"", ""}, true)
	test("", false, []string{"", ""}, false)
	test("hello*", false, []string{"hello", "hellp"}, true)
	test("hello", false, []string{"hello", "hellp"}, false)
	test("\xff*", false, []string{"\xff", "\xff\x00"}, true)
	test("\x00*", false, []string{"\x00", "\x01"}, true)
	test("\xff", false, []string{"\xff", "\xff\x00"}, false)

	test("*", true, []string{"", ""}, true)
	test("", true, []string{"", ""}, false)
	test("hello*", true, []string{"hellp", "helln"}, true)
	test("hello", true, []string{"hellp", "helln"}, false)
	test("a\xff*", true, []string{"a\xff\x00", "a\xfe"}, true)
	test("\x00*", true, []string{"\x01", ""}, true)
	test("\x01*", true, []string{"\x02", "\x00"}, true)
	test("b\x00*", true, []string{"b\x01", "a\xff"}, true)
	test("\x00\x00*", true, []string{"\x00\x01", ""}, true)
	test("\x00\x01\x00*", true, []string{"\x00\x01\x01", "\x00\x00\xff"}, true)
}
