package controller

import (
	"strings"
	"time"

	"github.com/tidwall/tile38/geojson"
)

func (c *Controller) FenceMatch(hookName string, sw *scanWriter, fence *liveFenceSwitches, details *commandDetailsT, mustLock bool) [][]byte {
	glob := fence.glob
	if details.command == "drop" {
		return [][]byte{[]byte(`{"cmd":"drop"}`)}
	}
	match := true
	if glob != "" && glob != "*" {
		match, _ = globMatch(glob, details.id)
	}
	if !match {
		return nil
	}

	if details.obj == nil || (details.command == "fset" && sw.nofields) {
		return nil
	}
	match = false
	detect := "outside"
	if fence != nil {
		match1 := fenceMatchObject(fence, details.oldObj)
		match2 := fenceMatchObject(fence, details.obj)
		if match1 && match2 {
			match = true
			detect = "inside"
		} else if match1 && !match2 {
			match = true
			detect = "exit"
		} else if !match1 && match2 {
			match = true
			detect = "enter"
		} else {
			// Maybe the old object and new object create a line that crosses the fence.
			// Must detect for that possibility.
			if details.oldObj != nil {
				ls := geojson.LineString{
					Coordinates: []geojson.Position{
						details.oldObj.CalculatedPoint(),
						details.obj.CalculatedPoint(),
					},
				}
				temp := false
				if fence.cmd == "within" {
					// because we are testing if the line croses the area we need to use
					// "intersects" instead of "within".
					fence.cmd = "intersects"
					temp = true
				}
				if fenceMatchObject(fence, ls) {
					match = true
					detect = "cross"
				}
				if temp {
					fence.cmd = "within"
				}
			}
		}
	}
	if details.command == "del" {
		return [][]byte{[]byte(`{"command":"del","id":` + jsonString(details.id) + `}`)}
	}
	var fmap map[string]int
	if mustLock {
		c.mu.RLock()
	}
	col := c.getCol(details.key)
	if col != nil {
		fmap = col.FieldMap()
	}
	if mustLock {
		c.mu.RUnlock()
	}
	if fmap == nil {
		return nil
	}
	sw.fmap = fmap
	sw.fullFields = true
	sw.writeObject(details.id, details.obj, details.fields)
	if sw.wr.Len() == 0 {
		return nil
	}
	res := sw.wr.String()
	sw.wr.Reset()
	if strings.HasPrefix(res, ",") {
		res = res[1:]
	}
	if sw.output == outputIDs {
		res = `{"id":` + res + `}`
	}
	jskey := jsonString(details.key)
	jstime := time.Now().Format("2006-01-02T15:04:05.999999999Z07:00")
	jshookName := jsonString(hookName)
	if strings.HasPrefix(res, "{") {
		res = `{"command":"` + details.command + `","detect":"` + detect + `","hook":` + jshookName + `,"time":"` + jstime + `","key":` + jskey + `,` + res[1:]
	}
	msgs := [][]byte{[]byte(res)}
	switch detect {
	case "enter":
		msgs = append(msgs, []byte(`{"command":"`+details.command+`","detect":"inside","hook":`+jshookName+`,"time":"`+jstime+`","key":`+jskey+`,`+res[1:]))
	case "exit", "cross":
		msgs = append(msgs, []byte(`{"command":"`+details.command+`","detect":"outside","hook":`+jshookName+`,"time":"`+jstime+`","key":`+jskey+`,`+res[1:]))
	}
	return msgs
}

func fenceMatchObject(fence *liveFenceSwitches, obj geojson.Object) bool {
	if obj == nil {
		return false
	}
	if fence.cmd == "nearby" {
		return obj.Nearby(geojson.Position{X: fence.lon, Y: fence.lat, Z: 0}, fence.meters)
	} else if fence.cmd == "within" {
		if fence.o != nil {
			return obj.Within(fence.o)
		} else {
			return obj.WithinBBox(geojson.BBox{
				Min: geojson.Position{X: fence.minLon, Y: fence.minLat, Z: 0},
				Max: geojson.Position{X: fence.maxLon, Y: fence.maxLat, Z: 0},
			})
		}
	} else if fence.cmd == "intersects" {
		if fence.o != nil {
			return obj.Intersects(fence.o)
		} else {
			return obj.IntersectsBBox(geojson.BBox{
				Min: geojson.Position{X: fence.minLon, Y: fence.minLat, Z: 0},
				Max: geojson.Position{X: fence.maxLon, Y: fence.maxLat, Z: 0},
			})
		}
	}
	return false
}
