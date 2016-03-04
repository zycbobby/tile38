package controller

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const defaultSearchOutput = outputObjects

var errInvalidNumberOfArguments = errors.New("invalid number of arguments")
var errKeyNotFound = errors.New("key not found")
var errIDNotFound = errors.New("id not found")

func errInvalidArgument(arg string) error {
	return fmt.Errorf("invalid argument '%s'", arg)
}
func errDuplicateArgument(arg string) error {
	return fmt.Errorf("duplicate argument '%s'", arg)
}
func token(line string) (newLine, token string) {
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' {
			return line[i+1:], line[:i]
		}
	}
	return "", line
}

func tokenlc(line string) (newLine, token string) {
	for i := 0; i < len(line); i++ {
		ch := line[i]
		if ch == ' ' {
			return line[i+1:], line[:i]
		}
		if ch >= 'A' && ch <= 'Z' {
			lc := make([]byte, 0, 16)
			if i > 0 {
				lc = append(lc, []byte(line[:i])...)
			}
			lc = append(lc, ch+32)
			i++
			for ; i < len(line); i++ {
				ch = line[i]
				if ch == ' ' {
					return line[i+1:], string(lc)
				}
				if ch >= 'A' && ch <= 'Z' {
					lc = append(lc, ch+32)
				} else {
					lc = append(lc, ch)
				}
			}
			return "", string(lc)
		}
	}
	return "", line
}

func lc(s1, s2 string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		ch := s1[i]
		if ch >= 'A' && ch <= 'Z' {
			if ch+32 != s2[i] {
				return false
			}
		} else if ch != s2[i] {
			return false
		}
	}
	return true
}

type whereT struct {
	field string
	minx  bool
	min   float64
	maxx  bool
	max   float64
}

func (where whereT) match(value float64) bool {
	if !where.minx {
		if value < where.min {
			return false
		}
	} else {
		if value <= where.min {
			return false
		}
	}
	if !where.maxx {
		if value > where.max {
			return false
		}
	} else {
		if value >= where.max {
			return false
		}
	}
	return true
}

type searchScanBaseTokens struct {
	key       string
	cursor    uint64
	output    outputT
	precision uint64
	lineout   string
	fence     bool
	glob      string
	wheres    []whereT
	nofields  bool
	limit     uint64
	sparse    uint8
}

func parseSearchScanBaseTokens(cmd, line string) (lineout string, t searchScanBaseTokens, err error) {
	if line, t.key = token(line); t.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	var slimit string
	var ssparse string
	var scursor string
	for {
		nline, wtok := token(line)
		if len(wtok) > 0 {
			if (wtok[0] == 'C' || wtok[0] == 'c') && strings.ToLower(wtok) == "cursor" {
				line = nline
				if scursor != "" {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				if line, scursor = token(line); scursor == "" {
					err = errInvalidNumberOfArguments
					return
				}
				continue
			} else if (wtok[0] == 'W' || wtok[0] == 'w') && strings.ToLower(wtok) == "where" {
				line = nline
				var field, smin, smax string
				if line, field = token(line); field == "" {
					err = errInvalidNumberOfArguments
					return
				}
				if line, smin = token(line); smin == "" {
					err = errInvalidNumberOfArguments
					return
				}
				if line, smax = token(line); smax == "" {
					err = errInvalidNumberOfArguments
					return
				}
				var minx, maxx bool
				var min, max float64
				if strings.ToLower(smin) == "-inf" {
					min = math.Inf(-1)
				} else {
					if strings.HasPrefix(smin, "(") {
						minx = true
						smin = smin[1:]
					}
					min, err = strconv.ParseFloat(smin, 64)
					if err != nil {
						err = errInvalidArgument(smin)
						return
					}
				}
				if strings.ToLower(smax) == "+inf" {
					max = math.Inf(+1)
				} else {
					if strings.HasPrefix(smax, "(") {
						maxx = true
						smax = smax[1:]
					}
					max, err = strconv.ParseFloat(smax, 64)
					if err != nil {
						err = errInvalidArgument(smax)
						return
					}
				}
				t.wheres = append(t.wheres, whereT{field, minx, min, maxx, max})
				continue
			} else if (wtok[0] == 'N' || wtok[0] == 'n') && strings.ToLower(wtok) == "nofields" {
				line = nline
				if t.nofields {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.nofields = true
				continue
			} else if (wtok[0] == 'L' || wtok[0] == 'l') && strings.ToLower(wtok) == "limit" {
				line = nline
				if slimit != "" {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				if line, slimit = token(line); slimit == "" {
					err = errInvalidNumberOfArguments
					return
				}
				continue
			} else if (wtok[0] == 'S' || wtok[0] == 's') && strings.ToLower(wtok) == "sparse" {
				line = nline
				if ssparse != "" {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				if line, ssparse = token(line); ssparse == "" {
					err = errInvalidNumberOfArguments
					return
				}
				continue
			} else if (wtok[0] == 'F' || wtok[0] == 'f') && strings.ToLower(wtok) == "fence" {
				line = nline
				if t.fence {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.fence = true
				continue
			} else if (wtok[0] == 'M' || wtok[0] == 'm') && strings.ToLower(wtok) == "match" {
				line = nline
				if t.glob != "" {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				if line, t.glob = token(line); t.glob == "" {
					err = errInvalidNumberOfArguments
					return
				}
				continue
			}
		}
		break
	}

	// check to make sure that there aren't any conflicts
	if cmd == "scan" {
		if ssparse != "" {
			err = errors.New("SPARSE is not allowed for SCAN")
			return
		}
		if t.fence {
			err = errors.New("FENCE is not allowed for SCAN")
			return
		}
	}
	if ssparse != "" && slimit != "" {
		err = errors.New("LIMIT is not allowed when SPARSE is specified")
		return
	}
	if scursor != "" && ssparse != "" {
		err = errors.New("CURSOR is not allowed when SPARSE is specified")
		return
	}
	if scursor != "" && t.fence {
		err = errors.New("CURSOR is not allowed when FENCE is specified")
		return
	}

	t.output = defaultSearchOutput
	var nline string
	var sprecision string
	var which string
	if nline, which = token(line); which != "" {
		updline := true
		switch strings.ToLower(which) {
		default:
			if cmd == "scan" {
				err = errInvalidArgument(which)
				return
			}
			updline = false
		case "count":
			t.output = outputCount
		case "objects":
			t.output = outputObjects
		case "points":
			t.output = outputPoints
		case "hashes":
			t.output = outputHashes
			if nline, sprecision = token(nline); sprecision == "" {
				err = errInvalidNumberOfArguments
				return
			}
		case "bounds":
			t.output = outputBounds
		case "ids":
			t.output = outputIDs
		}
		if updline {
			line = nline
		}
	}

	if scursor != "" {
		if t.cursor, err = strconv.ParseUint(scursor, 10, 64); err != nil {
			err = errInvalidArgument(scursor)
			return
		}
	}
	if sprecision != "" {
		if t.precision, err = strconv.ParseUint(sprecision, 10, 64); err != nil || t.precision == 0 || t.precision > 64 {
			err = errInvalidArgument(sprecision)
			return
		}
	}
	if slimit != "" {
		if t.limit, err = strconv.ParseUint(slimit, 10, 64); err != nil || t.limit == 0 {
			err = errInvalidArgument(slimit)
			return
		}
	}
	if ssparse != "" {
		var sparse uint64
		if sparse, err = strconv.ParseUint(ssparse, 10, 8); err != nil || sparse == 0 || sparse > 8 {
			err = errInvalidArgument(ssparse)
			return
		}
		t.sparse = uint8(sparse)
		t.limit = math.MaxUint64
	}
	lineout = line
	return
}
