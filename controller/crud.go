package controller

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/btree"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
	"github.com/tidwall/tile38/geojson/geohash"
)

type fvt struct {
	field string
	value float64
}

type byField []fvt

func (a byField) Len() int {
	return len(a)
}
func (a byField) Less(i, j int) bool {
	return a[i].field < a[j].field
}
func (a byField) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func orderFields(fmap map[string]int, fields []float64) []fvt {
	var fv fvt
	fvs := make([]fvt, 0, len(fmap))
	for field, idx := range fmap {
		if idx < len(fields) {
			fv.field = field
			fv.value = fields[idx]
			if fv.value != 0 {
				fvs = append(fvs, fv)
			}
		}
	}
	sort.Sort(byField(fvs))
	return fvs
}

func (c *Controller) cmdGet(msg *server.Message) (string, error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var key, id, typ, sprecision string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		return "", errInvalidNumberOfArguments
	}
	if vs, id, ok = tokenval(vs); !ok || id == "" {
		return "", errInvalidNumberOfArguments
	}

	withfields := false
	if _, peek, ok := tokenval(vs); ok && strings.ToLower(peek) == "withfields" {
		withfields = true
		vs = vs[1:]
	}

	col := c.getCol(key)
	if col == nil {
		if msg.OutputType == server.RESP {
			return "$-1\r\n", nil
		}
		return "", errKeyNotFound
	}
	o, fields, ok := col.Get(id)
	if !ok {
		if msg.OutputType == server.RESP {
			return "$-1\r\n", nil
		}
		return "", errIDNotFound
	}

	vals := make([]resp.Value, 0, 2)
	var buf bytes.Buffer
	if msg.OutputType == server.JSON {
		buf.WriteString(`{"ok":true`)
	}
	if vs, typ, ok = tokenval(vs); !ok || strings.ToLower(typ) == "object" {
		if msg.OutputType == server.JSON {
			buf.WriteString(`,"object":`)
			buf.WriteString(o.JSON())
		} else {
			vals = append(vals, resp.StringValue(o.JSON()))
		}
	} else {
		switch strings.ToLower(typ) {
		default:
			return "", errInvalidArgument(typ)
		case "point":
			point := o.CalculatedPoint()
			if msg.OutputType == server.JSON {
				buf.WriteString(`,"point":`)
				buf.WriteString(point.ExternalJSON())
			} else {
				if point.Z != 0 {
					vals = append(vals, resp.ArrayValue([]resp.Value{
						resp.StringValue(strconv.FormatFloat(point.Y, 'f', -1, 64)),
						resp.StringValue(strconv.FormatFloat(point.X, 'f', -1, 64)),
						resp.StringValue(strconv.FormatFloat(point.Z, 'f', -1, 64)),
					}))
				} else {
					vals = append(vals, resp.ArrayValue([]resp.Value{
						resp.StringValue(strconv.FormatFloat(point.Y, 'f', -1, 64)),
						resp.StringValue(strconv.FormatFloat(point.X, 'f', -1, 64)),
					}))
				}
			}
		case "hash":
			if vs, sprecision, ok = tokenval(vs); !ok || sprecision == "" {
				return "", errInvalidNumberOfArguments
			}
			if msg.OutputType == server.JSON {
				buf.WriteString(`,"hash":`)
			}
			precision, err := strconv.ParseInt(sprecision, 10, 64)
			if err != nil || precision < 1 || precision > 64 {
				return "", errInvalidArgument(sprecision)
			}
			p, err := o.Geohash(int(precision))
			if err != nil {
				return "", err
			}
			if msg.OutputType == server.JSON {
				buf.WriteString(`"` + p + `"`)
			} else {
				vals = append(vals, resp.StringValue(p))
			}
		case "bounds":
			bbox := o.CalculatedBBox()
			if msg.OutputType == server.JSON {
				buf.WriteString(`,"bounds":`)
				buf.WriteString(bbox.ExternalJSON())
			} else {
				vals = append(vals, resp.ArrayValue([]resp.Value{
					resp.ArrayValue([]resp.Value{
						resp.FloatValue(bbox.Min.Y),
						resp.FloatValue(bbox.Min.X),
					}),
					resp.ArrayValue([]resp.Value{
						resp.FloatValue(bbox.Max.Y),
						resp.FloatValue(bbox.Max.X),
					}),
				}))
			}
		}
	}
	if len(vs) != 0 {
		return "", errInvalidNumberOfArguments
	}
	if withfields {
		fvs := orderFields(col.FieldMap(), fields)
		if len(fvs) > 0 {
			fvals := make([]resp.Value, 0, len(fvs)*2)
			if msg.OutputType == server.JSON {
				buf.WriteString(`,"fields":{`)
			}
			for i, fv := range fvs {
				if msg.OutputType == server.JSON {
					if i > 0 {
						buf.WriteString(`,`)
					}
					buf.WriteString(jsonString(fv.field) + ":" + strconv.FormatFloat(fv.value, 'f', -1, 64))
				} else {
					fvals = append(fvals, resp.StringValue(fv.field), resp.StringValue(strconv.FormatFloat(fv.value, 'f', -1, 64)))
				}
				i++
			}
			if msg.OutputType == server.JSON {
				buf.WriteString(`}`)
			} else {
				vals = append(vals, resp.ArrayValue(fvals))
			}
		}
	}
	switch msg.OutputType {
	case server.JSON:
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return buf.String(), nil
	case server.RESP:
		var oval resp.Value
		if withfields {
			oval = resp.ArrayValue(vals)
		} else {
			oval = vals[0]
		}
		data, err := oval.MarshalRESP()
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	return "", nil
}

func (c *Controller) cmdDel(msg *server.Message) (res string, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	found := false
	col := c.getCol(d.key)
	if col != nil {
		d.obj, d.fields, ok = col.Remove(d.id)
		if ok {
			if col.Count() == 0 {
				c.deleteCol(d.key)
				d.revert = func() {
					c.setCol(d.key, col)
					col.ReplaceOrInsert(d.id, d.obj, nil, d.fields)
				}
			} else {
				d.revert = func() {
					col.ReplaceOrInsert(d.id, d.obj, nil, d.fields)
				}
			}
			found = true
		}
	}
	d.command = "del"
	d.updated = found
	d.timestamp = time.Now()
	switch msg.OutputType {
	case server.JSON:
		res = `{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
	case server.RESP:
		if d.updated {
			res = ":1\r\n"
		} else {
			res = ":0\r\n"
		}
	}
	return
}

func (c *Controller) cmdDrop(msg *server.Message) (res string, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	col := c.getCol(d.key)
	if col != nil {
		c.deleteCol(d.key)
		d.revert = func() {
			c.setCol(d.key, col)
		}
		d.updated = true
	} else {
		d.key = "" // ignore the details
		d.updated = false
	}
	d.command = "drop"
	d.timestamp = time.Now()
	switch msg.OutputType {
	case server.JSON:
		res = `{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
	case server.RESP:
		if d.updated {
			res = ":1\r\n"
		} else {
			res = ":0\r\n"
		}
	}
	return
}

func (c *Controller) cmdFlushDB(msg *server.Message) (res string, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	c.cols = btree.New(16)
	c.hooks = make(map[string]*Hook)
	c.hookcols = make(map[string]map[string]*Hook)
	d.command = "flushdb"
	d.updated = true
	d.timestamp = time.Now()
	switch msg.OutputType {
	case server.JSON:
		res = `{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
	case server.RESP:
		res = "+OK\r\n"
	}
	return
}

func (c *Controller) parseSetArgs(vs []resp.Value) (d commandDetailsT, fields []string, values []float64, etype string, evs []resp.Value, err error) {
	var ok bool
	var typ string
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}

	var arg string
	var nvs []resp.Value
	fields = make([]string, 0, 8)
	values = make([]float64, 0, 8)
	for {
		if nvs, arg, ok = tokenval(vs); !ok || arg == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if lc(arg, "field") {
			vs = nvs
			var name string
			var svalue string
			var value float64
			if vs, name, ok = tokenval(vs); !ok || name == "" {
				err = errInvalidNumberOfArguments
				return
			}
			if isReservedFieldName(name) {
				err = errInvalidArgument(name)
				return
			}
			if vs, svalue, ok = tokenval(vs); !ok || svalue == "" {
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
	if vs, typ, ok = tokenval(vs); !ok || typ == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) == 0 {
		err = errInvalidNumberOfArguments
		return
	}
	etype = typ
	evs = vs

	switch {
	default:
		err = errInvalidArgument(typ)
		return
	case lc(typ, "point"):
		var slat, slon, sz string
		if vs, slat, ok = tokenval(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = tokenval(vs); !ok || slon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		vs, sz, ok = tokenval(vs)
		if !ok || sz == "" {
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
		if vs, sminlat, ok = tokenval(vs); !ok || sminlat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sminlon, ok = tokenval(vs); !ok || sminlon == "" {
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
		if vs, shash, ok = tokenval(vs); !ok || shash == "" {
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
		var object string
		if vs, object, ok = tokenval(vs); !ok || object == "" {
			err = errInvalidNumberOfArguments
			return
		}
		d.obj, err = geojson.ObjectJSON(object)
		if err != nil {
			return
		}
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
	}
	return
}

func (c *Controller) cmdSet(msg *server.Message) (res string, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var fields []string
	var values []float64
	d, fields, values, _, _, err = c.parseSetArgs(vs)
	if err != nil {
		return
	}
	addedcol := false
	col := c.getCol(d.key)
	if col == nil {
		col = collection.New()
		c.setCol(d.key, col)
		addedcol = true
	}
	d.oldObj, d.oldFields, d.fields = col.ReplaceOrInsert(d.id, d.obj, fields, values)
	d.revert = func() {
		if addedcol {
			c.deleteCol(d.key)
		} else if d.oldObj != nil {
			col.ReplaceOrInsert(d.id, d.oldObj, nil, d.oldFields)
		} else {
			col.Remove(d.id)
		}
	}
	d.command = "set"
	d.updated = true // perhaps we should do a diff on the previous object?
	fmap := col.FieldMap()
	d.fmap = make(map[string]int)
	for key, idx := range fmap {
		d.fmap[key] = idx
	}
	d.timestamp = time.Now()
	switch msg.OutputType {
	case server.JSON:
		res = `{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
	case server.RESP:
		res = "+OK\r\n"
	}
	return
}

func (c *Controller) parseFSetArgs(vs []resp.Value) (d commandDetailsT, err error) {
	var svalue string
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.field, ok = tokenval(vs); !ok || d.field == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if isReservedFieldName(d.field) {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, svalue, ok = tokenval(vs); !ok || svalue == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
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

func (c *Controller) cmdFset(msg *server.Message) (res string, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	d, err = c.parseFSetArgs(vs)
	col := c.getCol(d.key)
	if col == nil {
		err = errKeyNotFound
		return
	}
	var ok bool
	d.obj, d.fields, d.updated, ok = col.SetField(d.id, d.field, d.value)
	if !ok {
		err = errIDNotFound
		return
	}
	d.command = "fset"
	d.timestamp = time.Now()
	fmap := col.FieldMap()
	d.fmap = make(map[string]int)
	for key, idx := range fmap {
		d.fmap[key] = idx
	}

	switch msg.OutputType {
	case server.JSON:
		res = `{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
	case server.RESP:
		if d.updated {
			res = ":1\r\n"
		} else {
			res = ":0\r\n"
		}
	}
	return
}
