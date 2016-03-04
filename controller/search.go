package controller

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/tile38/bing"
	"github.com/tidwall/tile38/geojson"
	"github.com/tidwall/tile38/geojson/geohash"
)

type liveFenceSwitches struct {
	searchScanBaseTokens
	lat, lon, meters float64
	o                geojson.Object
	minLat, minLon   float64
	maxLat, maxLon   float64
	cmd              string
}

func (s liveFenceSwitches) Error() string {
	return "going live"
}

func (c *Controller) cmdSearchArgs(cmd, line string, types []string) (s liveFenceSwitches, err error) {
	if line, s.searchScanBaseTokens, err = parseSearchScanBaseTokens(cmd, line); err != nil {
		return
	}
	var typ string
	if line, typ = token(line); typ == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if s.searchScanBaseTokens.output == outputBounds {
		if cmd == "within" || cmd == "intersects" {
			if _, err := strconv.ParseFloat(typ, 64); err == nil {
				// It's likely that the output was not specified, but rather the search bounds.
				s.searchScanBaseTokens.output = defaultSearchOutput
				line = typ + " " + line
				typ = "BOUNDS"
			}
		}
	}
	var found bool
	for _, t := range types {
		if strings.ToLower(typ) == t {
			found = true
			break
		}
	}
	if !found {
		err = errInvalidArgument(typ)
		return
	}
	switch strings.ToLower(typ) {
	case "point":
		var slat, slon, smeters string
		if line, slat = token(line); slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, slon = token(line); slon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, smeters = token(line); smeters == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.lat, err = strconv.ParseFloat(slat, 64); err != nil {
			err = errInvalidArgument(slat)
			return
		}
		if s.lon, err = strconv.ParseFloat(slon, 64); err != nil {
			err = errInvalidArgument(slon)
			return
		}
		if s.meters, err = strconv.ParseFloat(smeters, 64); err != nil {
			err = errInvalidArgument(smeters)
			return
		}
	case "object":
		if line == "" {
			err = errInvalidNumberOfArguments
			return
		}
		s.o, err = geojson.ObjectJSON(line)
		if err != nil {
			return
		}
		line = "" // since we read the remaining bytes
	case "bounds":
		var sminLat, sminLon, smaxlat, smaxlon string
		if line, sminLat = token(line); sminLat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, sminLon = token(line); sminLon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, smaxlat = token(line); smaxlat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, smaxlon = token(line); smaxlon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.minLat, err = strconv.ParseFloat(sminLat, 64); err != nil {
			err = errInvalidArgument(sminLat)
			return
		}
		if s.minLon, err = strconv.ParseFloat(sminLon, 64); err != nil {
			err = errInvalidArgument(sminLon)
			return
		}
		if s.maxLat, err = strconv.ParseFloat(smaxlat, 64); err != nil {
			err = errInvalidArgument(smaxlat)
			return
		}
		if s.maxLon, err = strconv.ParseFloat(smaxlon, 64); err != nil {
			err = errInvalidArgument(smaxlon)
			return
		}
	case "hash":
		var hash string
		if line, hash = token(line); hash == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.minLat, s.minLon, s.maxLat, s.maxLon, err = geohash.Bounds(hash); err != nil {
			err = errInvalidArgument(hash)
			return
		}
	case "quadkey":
		var key string
		if line, key = token(line); key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.minLat, s.minLon, s.maxLat, s.maxLon, err = bing.QuadKeyToBounds(key); err != nil {
			err = errInvalidArgument(key)
			return
		}
	case "tile":
		var sx, sy, sz string
		if line, sx = token(line); sx == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, sy = token(line); sy == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, sz = token(line); sz == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var x, y int64
		var z uint64
		if x, err = strconv.ParseInt(sx, 10, 64); err != nil {
			err = errInvalidArgument(sx)
			return
		}
		if y, err = strconv.ParseInt(sy, 10, 64); err != nil {
			err = errInvalidArgument(sy)
			return
		}
		if z, err = strconv.ParseUint(sz, 10, 64); err != nil {
			err = errInvalidArgument(sz)
			return
		}
		s.minLat, s.minLon, s.maxLat, s.maxLon = bing.TileXYToBounds(x, y, z)
	case "get":
		var key, id string
		if line, key = token(line); key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, id = token(line); id == "" {
			err = errInvalidNumberOfArguments
			return
		}
		col := c.getCol(key)
		if col == nil {
			err = errKeyNotFound
			return
		}
		o, _, ok := col.Get(id)
		if !ok {
			err = errIDNotFound
			return
		}
		if o.IsBBoxDefined() {
			bbox := o.CalculatedBBox()
			s.minLat = bbox.Min.Y
			s.minLon = bbox.Min.X
			s.maxLat = bbox.Max.Y
			s.maxLon = bbox.Max.X
		} else {
			s.o = o
		}
	}
	if line != "" {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func (c *Controller) cmdNearby(line string, w io.Writer) error {
	start := time.Now()
	wr := &bytes.Buffer{}
	s, err := c.cmdSearchArgs("nearby", line, []string{"point"})
	if err != nil {
		return err
	}
	s.cmd = "nearby"
	if s.fence {
		return s
	}
	sw, err := c.newScanWriter(wr, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
	if err != nil {
		return err
	}
	wr.WriteString(`{"ok":true`)
	sw.writeHead()
	if sw.col != nil {
		s.cursor = sw.col.Nearby(s.cursor, s.sparse, s.lat, s.lon, s.meters, func(id string, o geojson.Object, fields []float64) bool {
			return sw.writeObject(id, o, fields)
		})
	}
	sw.writeFoot(s.cursor)
	wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	w.Write(wr.Bytes())
	return nil
}

func (c *Controller) cmdWithin(line string, w io.Writer) error {
	return c.cmdWithinOrIntersects("within", line, w)
}

func (c *Controller) cmdIntersects(line string, w io.Writer) error {
	return c.cmdWithinOrIntersects("intersects", line, w)
}

func (c *Controller) cmdWithinOrIntersects(cmd string, line string, w io.Writer) error {
	start := time.Now()
	wr := &bytes.Buffer{}
	s, err := c.cmdSearchArgs(cmd, line, []string{"geo", "bounds", "hash", "tile", "quadkey", "get"})
	if err != nil {
		return err
	}
	s.cmd = cmd
	if s.fence {
		return s
	}
	sw, err := c.newScanWriter(wr, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
	if err != nil {
		return err
	}
	wr.WriteString(`{"ok":true`)
	sw.writeHead()
	if cmd == "within" {
		s.cursor = sw.col.Within(s.cursor, s.sparse, s.o, s.minLat, s.minLon, s.maxLat, s.maxLon,
			func(id string, o geojson.Object, fields []float64) bool {
				return sw.writeObject(id, o, fields)
			},
		)
	} else if cmd == "intersects" {
		s.cursor = sw.col.Intersects(s.cursor, s.sparse, s.o, s.minLat, s.minLon, s.maxLat, s.maxLon,
			func(id string, o geojson.Object, fields []float64) bool {
				return sw.writeObject(id, o, fields)
			},
		)
	}
	sw.writeFoot(s.cursor)
	wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	w.Write(wr.Bytes())
	return nil
}
