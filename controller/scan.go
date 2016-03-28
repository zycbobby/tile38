package controller

import (
	"bytes"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
)

func cmdScanArgs(vs []resp.Value) (s liveFenceSwitches, err error) {
	if vs, s.searchScanBaseTokens, err = parseSearchScanBaseTokens("scan", vs); err != nil {
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func (c *Controller) cmdScan(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	wr := &bytes.Buffer{}
	s, err := cmdScanArgs(vs)
	if err != nil {
		return "", err
	}
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
	if err != nil {
		return "", err
	}
	if s.sparse > 0 && sw.col != nil {
		msg.Values = append(msg.Values,
			resp.StringValue("BOUNDS"),
			resp.StringValue("-90"),
			resp.StringValue("-180"),
			resp.StringValue("180"),
		)
		return c.cmdWithinOrIntersects("within", msg)
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true`)
	}
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
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	}
	return string(wr.Bytes()), nil
}
