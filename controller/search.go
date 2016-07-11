package controller

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/bing"
	"github.com/tidwall/tile38/controller/server"
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
	roam             roamSwitches
}

type roamSwitches struct {
	on      bool
	key     string
	id      string
	pattern bool
	meters  float64
}

func (s liveFenceSwitches) Error() string {
	return "going live"
}

func (c *Controller) cmdSearchArgs(cmd string, vs []resp.Value, types []string) (s liveFenceSwitches, err error) {
	if vs, s.searchScanBaseTokens, err = parseSearchScanBaseTokens(cmd, vs); err != nil {
		return
	}
	var typ string
	var ok bool
	if vs, typ, ok = tokenval(vs); !ok || typ == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if s.searchScanBaseTokens.output == outputBounds {
		if cmd == "within" || cmd == "intersects" {
			if _, err := strconv.ParseFloat(typ, 64); err == nil {
				// It's likely that the output was not specified, but rather the search bounds.
				s.searchScanBaseTokens.output = defaultSearchOutput
				vs = append([]resp.Value{resp.StringValue(typ)}, vs...)
				typ = "BOUNDS"
			}
		}
	}
	ltyp := strings.ToLower(typ)
	var found bool
	for _, t := range types {
		if ltyp == t {
			found = true
			break
		}
	}
	if !found && s.searchScanBaseTokens.fence && ltyp == "roam" && cmd == "nearby" {
		// allow roaming for nearby fence searches.
		found = true
	}
	if !found {
		err = errInvalidArgument(typ)
		return
	}
	switch ltyp {
	case "point":
		var slat, slon, smeters string
		if vs, slat, ok = tokenval(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = tokenval(vs); !ok || slon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smeters, ok = tokenval(vs); !ok || smeters == "" {
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
		var obj string
		if vs, obj, ok = tokenval(vs); !ok || obj == "" {
			err = errInvalidNumberOfArguments
			return
		}
		s.o, err = geojson.ObjectJSON(obj)
		if err != nil {
			return
		}
	case "bounds":
		var sminLat, sminLon, smaxlat, smaxlon string
		if vs, sminLat, ok = tokenval(vs); !ok || sminLat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sminLon, ok = tokenval(vs); !ok || sminLon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smaxlat, ok = tokenval(vs); !ok || smaxlat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smaxlon, ok = tokenval(vs); !ok || smaxlon == "" {
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
		if vs, hash, ok = tokenval(vs); !ok || hash == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.minLat, s.minLon, s.maxLat, s.maxLon, err = geohash.Bounds(hash); err != nil {
			err = errInvalidArgument(hash)
			return
		}
	case "quadkey":
		var key string
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.minLat, s.minLon, s.maxLat, s.maxLon, err = bing.QuadKeyToBounds(key); err != nil {
			err = errInvalidArgument(key)
			return
		}
	case "tile":
		var sx, sy, sz string
		if vs, sx, ok = tokenval(vs); !ok || sx == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sy, ok = tokenval(vs); !ok || sy == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sz, ok = tokenval(vs); !ok || sz == "" {
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
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, id, ok = tokenval(vs); !ok || id == "" {
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
	case "roam":
		s.roam.on = true
		if vs, s.roam.key, ok = tokenval(vs); !ok || s.roam.key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, s.roam.id, ok = tokenval(vs); !ok || s.roam.id == "" {
			err = errInvalidNumberOfArguments
			return
		}
		s.roam.pattern = globIsGlob(s.roam.id)
		var smeters string
		if vs, smeters, ok = tokenval(vs); !ok || smeters == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.roam.meters, err = strconv.ParseFloat(smeters, 64); err != nil {
			err = errInvalidArgument(smeters)
			return
		}
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

var nearbyTypes = []string{"point"}
var withinOrIntersectsTypes = []string{"geo", "bounds", "hash", "tile", "quadkey", "get", "object"}

func (c *Controller) cmdNearby(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	wr := &bytes.Buffer{}
	s, err := c.cmdSearchArgs("nearby", vs, nearbyTypes)
	if err != nil {
		return "", err
	}
	s.cmd = "nearby"
	if s.fence {
		return "", s
	}
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
	if err != nil {
		return "", err
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		s.cursor = sw.col.Nearby(s.cursor, s.sparse, s.lat, s.lon, s.meters, func(id string, o geojson.Object, fields []float64) bool {
			return sw.writeObject(id, o, fields, false)
		})
	}
	sw.writeFoot(s.cursor)
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	}
	return string(wr.Bytes()), nil
}

func (c *Controller) cmdWithin(msg *server.Message) (res string, err error) {
	return c.cmdWithinOrIntersects("within", msg)
}

func (c *Controller) cmdIntersects(msg *server.Message) (res string, err error) {
	return c.cmdWithinOrIntersects("intersects", msg)
}

func (c *Controller) cmdWithinOrIntersects(cmd string, msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	wr := &bytes.Buffer{}
	s, err := c.cmdSearchArgs(cmd, vs, withinOrIntersectsTypes)
	if err != nil {
		return "", err
	}
	s.cmd = cmd
	if s.fence {
		return "", s
	}
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
	if err != nil {
		return "", err
	}
	if sw.col == nil {
		return "", errKeyNotFound
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if cmd == "within" {
		s.cursor = sw.col.Within(s.cursor, s.sparse, s.o, s.minLat, s.minLon, s.maxLat, s.maxLon,
			func(id string, o geojson.Object, fields []float64) bool {
				return sw.writeObject(id, o, fields, false)
			},
		)
	} else if cmd == "intersects" {
		s.cursor = sw.col.Intersects(s.cursor, s.sparse, s.o, s.minLat, s.minLon, s.maxLat, s.maxLon,
			func(id string, o geojson.Object, fields []float64) bool {
				return sw.writeObject(id, o, fields, false)
			},
		)
	}
	sw.writeFoot(s.cursor)
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	}
	return string(wr.Bytes()), nil
}

func (c *Controller) cmdSearch(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	var key string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	col := c.getCol(key)
	if col == nil {
		err = errKeyNotFound
		return
	}
	var tok string
	var pivot string
	var pivoton bool
	var limiton bool
	var limit int
	var descon bool
	var desc bool
	for {
		if vs, tok, ok = tokenval(vs); !ok || tok == "" {
			break
		}
		switch strings.ToLower(tok) {
		default:
			err = errInvalidArgument(tok)
			return
		case "pivot":
			if pivoton {
				err = errInvalidArgument(tok)
				return
			}
			pivoton = true
			if vs, pivot, ok = tokenval(vs); !ok || pivot == "" {
				err = errInvalidNumberOfArguments
				return
			}
		case "limit":
			if limiton {
				err = errInvalidArgument(tok)
				return
			}
			limiton = true
			if vs, tok, ok = tokenval(vs); !ok || tok == "" {
				err = errInvalidNumberOfArguments
				return
			}
			n, err2 := strconv.ParseUint(tok, 10, 64)
			if err2 != nil {
				err = errInvalidArgument(tok)
				return
			}
			limit = int(n)
		case "asc", "desc":
			if descon {
				err = errInvalidArgument(tok)
				return
			}
			descon = true
			switch strings.ToLower(tok) {
			case "asc":
				desc = false
			case "desc":
				desc = true
			}
		}
	}
	println(pivoton, pivot)
	println(limiton, limit)
	println(descon, desc)
	wr := &bytes.Buffer{}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true,"objects":[`)
	}
	n := 0
	col.SearchValues(pivot, desc, func(id string, obj geojson.Object, fields []float64) bool {
		if msg.OutputType == server.JSON {
			if n > 0 {
				wr.WriteString(`,`)
			}
			wr.WriteString(`{"id":` + jsonString(id) + `,"object":` + obj.JSON() + `}`)
			n++
		}
		return true
	})
	if msg.OutputType == server.JSON {
		wr.WriteString(`],"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	}
	return string(wr.Bytes()), nil
}
