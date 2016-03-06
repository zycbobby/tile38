package controller

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/btree"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/geojson"
	"github.com/tidwall/tile38/geojson/geohash"
)

func (c *Controller) cmdGet(line string) (string, error) {
	start := time.Now()
	var key, id, typ, sprecision string
	if line, key = token(line); key == "" {
		return "", errInvalidNumberOfArguments
	}
	if line, id = token(line); id == "" {
		return "", errInvalidNumberOfArguments
	}
	col := c.getCol(key)
	if col == nil {
		return "", errKeyNotFound
	}
	o, fields, ok := col.Get(id)
	if !ok {
		return "", errIDNotFound
	}
	var buf bytes.Buffer
	buf.WriteString(`{"ok":true`)
	if line, typ = token(line); typ == "" || strings.ToLower(typ) == "object" {
		buf.WriteString(`,"object":`)
		buf.WriteString(o.JSON())
	} else {
		ltyp := strings.ToLower(typ)
		switch ltyp {
		default:
			return "", errInvalidArgument(typ)
		case "point":
			buf.WriteString(`,"point":`)
			buf.WriteString(o.CalculatedPoint().ExternalJSON())
		case "hash":
			if line, sprecision = token(line); sprecision == "" {
				return "", errInvalidNumberOfArguments
			}
			buf.WriteString(`,"hash":`)
			precision, err := strconv.ParseInt(sprecision, 10, 64)
			if err != nil || precision < 1 || precision > 64 {
				return "", errInvalidArgument(sprecision)
			}
			p, err := o.Geohash(int(precision))
			if err != nil {
				return "", err
			}
			buf.WriteString(`"` + p + `"`)
		case "bounds":
			buf.WriteString(`,"bounds":`)
			buf.WriteString(o.CalculatedBBox().ExternalJSON())
		}
	}
	if line != "" {
		return "", errInvalidNumberOfArguments
	}
	fmap := col.FieldMap()
	if len(fmap) > 0 {
		buf.WriteString(`,"fields":{`)
		var i int
		for field, idx := range fmap {
			if len(fields) > idx {
				if !math.IsNaN(fields[idx]) {
					if i > 0 {
						buf.WriteString(`,`)
					}
					buf.WriteString(jsonString(field) + ":" + strconv.FormatFloat(fields[idx], 'f', -1, 64))
					i++
				}
			}
		}
		buf.WriteString(`}`)
	}
	buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	return buf.String(), nil
}

func (c *Controller) cmdDel(line string) (d commandDetailsT, err error) {
	if line, d.key = token(line); d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if line, d.id = token(line); d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if line != "" {
		err = errInvalidNumberOfArguments
		return
	}
	col := c.getCol(d.key)
	if col != nil {
		d.obj, d.fields, _ = col.Remove(d.id)
		if col.Count() == 0 {
			c.deleteCol(d.key)
		}
	}
	d.command = "del"
	return
}

func (c *Controller) cmdDrop(line string) (d commandDetailsT, err error) {
	if line, d.key = token(line); d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if line != "" {
		err = errInvalidNumberOfArguments
		return
	}
	col := c.getCol(d.key)
	if col != nil {
		c.deleteCol(d.key)
	} else {
		d.key = "" // ignore the details
	}
	d.command = "drop"
	return
}

func (c *Controller) cmdFlushDB(line string) (d commandDetailsT, err error) {
	if line != "" {
		err = errInvalidNumberOfArguments
		return
	}
	c.cols = btree.New(16)
	c.colsm = make(map[string]*collection.Collection)
	d.command = "flushdb"
	return
}

func (c *Controller) parseSetArgs(line string) (d commandDetailsT, fields []string, values []float64, etype, eline string, err error) {

	var typ string
	if line, d.key = token(line); d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if line, d.id = token(line); d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	var arg string
	var nline string
	fields = make([]string, 0, 8)
	values = make([]float64, 0, 8)
	for {
		if nline, arg = token(line); arg == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if lc(arg, "field") {
			line = nline
			var name string
			var svalue string
			var value float64
			if line, name = token(line); name == "" {
				err = errInvalidNumberOfArguments
				return
			}
			if isReservedFieldName(name) {
				err = errInvalidArgument(name)
				return
			}
			if line, svalue = token(line); svalue == "" {
				err = errInvalidNumberOfArguments
				return
			}
			value, err = strconv.ParseFloat(svalue, 64)
			if err != nil {
				err = errInvalidArgument(svalue)
				return
			}
			fields = append(fields, name)
			values = append(values, value)
			continue
		}
		break
	}
	if line, typ = token(line); typ == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if line == "" {
		err = errInvalidNumberOfArguments
		return
	}
	etype = typ
	eline = line

	switch {
	default:
		err = errInvalidArgument(typ)
		return
	case lc(typ, "point"):
		var slat, slon, sz string
		if line, slat = token(line); slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, slon = token(line); slon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		line, sz = token(line)
		if sz == "" {
			var sp geojson.SimplePoint
			sp.Y, err = strconv.ParseFloat(slat, 64)
			if err != nil {
				err = errInvalidArgument(slat)
				return
			}
			sp.X, err = strconv.ParseFloat(slon, 64)
			if err != nil {
				err = errInvalidArgument(slon)
				return
			}
			d.obj = sp
		} else {
			var sp geojson.Point
			sp.Coordinates.Y, err = strconv.ParseFloat(slat, 64)
			if err != nil {
				err = errInvalidArgument(slat)
				return
			}
			sp.Coordinates.X, err = strconv.ParseFloat(slon, 64)
			if err != nil {
				err = errInvalidArgument(slon)
				return
			}
			sp.Coordinates.Z, err = strconv.ParseFloat(sz, 64)
			if err != nil {
				err = errInvalidArgument(sz)
				return
			}
			d.obj = sp
		}
	case lc(typ, "bounds"):
		var sminlat, sminlon, smaxlat, smaxlon string
		if line, sminlat = token(line); sminlat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if line, sminlon = token(line); sminlon == "" {
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
		var minlat, minlon, maxlat, maxlon float64
		minlat, err = strconv.ParseFloat(sminlat, 64)
		if err != nil {
			err = errInvalidArgument(sminlat)
			return
		}
		minlon, err = strconv.ParseFloat(sminlon, 64)
		if err != nil {
			err = errInvalidArgument(sminlon)
			return
		}
		maxlat, err = strconv.ParseFloat(smaxlat, 64)
		if err != nil {
			err = errInvalidArgument(smaxlat)
			return
		}
		maxlon, err = strconv.ParseFloat(smaxlon, 64)
		if err != nil {
			err = errInvalidArgument(smaxlon)
			return
		}
		g := geojson.Polygon{
			Coordinates: [][]geojson.Position{
				[]geojson.Position{
					geojson.Position{X: minlon, Y: minlat, Z: 0},
					geojson.Position{X: minlon, Y: maxlat, Z: 0},
					geojson.Position{X: maxlon, Y: maxlat, Z: 0},
					geojson.Position{X: maxlon, Y: minlat, Z: 0},
					geojson.Position{X: minlon, Y: minlat, Z: 0},
				},
			},
		}
		d.obj = g
	case lc(typ, "hash"):
		var sp geojson.SimplePoint
		var shash string
		if line, shash = token(line); shash == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var lat, lon float64
		lat, lon, err = geohash.Decode(shash)
		if err != nil {
			return
		}
		sp.X = lon
		sp.Y = lat
		d.obj = sp
	case lc(typ, "object"):
		d.obj, err = geojson.ObjectJSON(line)
		if err != nil {
			return
		}
	}
	return
}

func (c *Controller) cmdSet(line string) (d commandDetailsT, err error) {
	var fields []string
	var values []float64
	d, fields, values, _, _, err = c.parseSetArgs(line)
	if err != nil {
		return
	}
	col := c.getCol(d.key)
	if col == nil {
		col = collection.New()
		c.setCol(d.key, col)
	}
	d.oldObj, d.oldFields, d.fields = col.ReplaceOrInsert(d.id, d.obj, fields, values)
	d.command = "set"
	return
}

func (c *Controller) parseFSetArgs(line string) (d commandDetailsT, err error) {
	var svalue string
	if line, d.key = token(line); d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if line, d.id = token(line); d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if line, d.field = token(line); d.field == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if isReservedFieldName(d.field) {
		err = errInvalidNumberOfArguments
		return
	}
	if line, svalue = token(line); svalue == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if line != "" {
		err = errInvalidNumberOfArguments
		return
	}
	d.value, err = strconv.ParseFloat(svalue, 64)
	if err != nil {
		err = errInvalidArgument(svalue)
		return
	}
	return
}

func (c *Controller) cmdFset(line string) (d commandDetailsT, err error) {
	d, err = c.parseFSetArgs(line)
	col := c.getCol(d.key)
	if col == nil {
		err = errKeyNotFound
		return
	}
	var ok bool
	d.obj, d.fields, ok = col.SetField(d.id, d.field, d.value)
	if !ok {
		err = errIDNotFound
		return
	}
	d.command = "fset"
	return
}
