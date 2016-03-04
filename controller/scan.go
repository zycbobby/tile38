package controller

import (
	"bytes"
	"io"
	"strings"
	"time"

	"github.com/tidwall/tile38/geojson"
)

func cmdScanArgs(line string) (
	s liveFenceSwitches, err error,
) {
	if line, s.searchScanBaseTokens, err = parseSearchScanBaseTokens("scan", line); err != nil {
		return
	}
	if line != "" {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func (c *Controller) cmdScan(line string, w io.Writer) error {
	start := time.Now()
	wr := &bytes.Buffer{}
	s, err := cmdScanArgs(line)
	if err != nil {
		return err
	}
	sw, err := c.newScanWriter(wr, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
	if err != nil {
		return err
	}
	if s.sparse > 0 && sw.col != nil {
		return c.cmdWithinOrIntersects("within", line+" BOUNDS -90 -180 90 180", w)
	}
	wr.WriteString(`{"ok":true`)
	sw.writeHead()
	if sw.col != nil {
		if sw.output == outputCount && len(sw.wheres) == 0 && sw.globEverything == true {
			count := sw.col.Count() - int(s.cursor)
			if count < 0 {
				count = 0
			}
			sw.count = uint64(count)
		} else {
			if strings.HasSuffix(sw.glob, "*") {
				greaterGlob := sw.glob[:len(sw.glob)-1]
				if globIsGlob(greaterGlob) {
					s.cursor = sw.col.Scan(s.cursor, func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(id, o, fields)
					})
				} else {
					s.cursor = sw.col.ScanGreaterOrEqual(sw.glob, s.cursor, func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(id, o, fields)
					})
				}
			} else {
				s.cursor = sw.col.Scan(s.cursor, func(id string, o geojson.Object, fields []float64) bool {
					return sw.writeObject(id, o, fields)
				})
			}
		}
	}
	sw.writeFoot(s.cursor)
	wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	w.Write(wr.Bytes())
	return nil
}
