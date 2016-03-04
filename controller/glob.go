package controller

import "path"

func globMatch(pattern, name string) (matched bool, err error) {
	return path.Match(pattern, name)
}

func globIsGlob(pattern string) bool {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '[', '*', '?':
			_, err := globMatch(pattern, "whatever")
			return err == nil
		}
	}
	return false
}
