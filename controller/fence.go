package controller

import (
	"strings"

	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
)

var tmfmt = "2006-01-02T15:04:05.999999999Z07:00"

// FenceMatch executes a fence match returns back json messages for fence detection.
func FenceMatch(hookName string, sw *scanWriter, fence *liveFenceSwitches, details *commandDetailsT) []string {
	jshookName := jsonString(hookName)
	jstime := jsonString(details.timestamp.Format(tmfmt))
	glob := fence.glob
	if details.command == "drop" {
		return []string{`{"cmd":"drop","hook":` + jshookName + `,"time":` + jstime + `}`}
	}
	match := true
	if glob != "" && glob != "*" {
		match, _ = globMatch(glob, details.id)
	}
	if !match {
		return nil
	}

	sw.mu.Lock()
	nofields := sw.nofields
	sw.mu.Unlock()

	if details.obj == nil || (details.command == "fset" && nofields) {
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
			if details.command == "fset" {
				detect = "inside"
			}
		} else {
			if details.command != "fset" {
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
						//match = true
						detect = "cross"
					}
					if temp {
						fence.cmd = "within"
					}
				}
			}
		}
	}
	if details.command == "del" {
		return []string{`{"command":"del","hook":` + jshookName + `,"id":` + jsonString(details.id) + `,"time":` + jstime + `}`}
	}
	if details.fmap == nil {
		return nil
	}
	sw.mu.Lock()
	sw.fmap = details.fmap
	sw.fullFields = true
	sw.msg.OutputType = server.JSON
	sw.writeObject(details.id, details.obj, details.fields, true)
	if sw.wr.Len() == 0 {
		sw.mu.Unlock()
		return nil
	}
	res := sw.wr.String()
	resb := make([]byte, len(res))
	copy(resb, res)
	res = string(resb)
	sw.wr.Reset()
	if sw.output == outputIDs {
		res = `{"id":` + res + `}`
	}
	sw.mu.Unlock()

	if strings.HasPrefix(res, ",") {
		res = res[1:]
	}

	jskey := jsonString(details.key)
	ores := res
	msgs := make([]string, 0, 2)
	if fence.detect == nil || fence.detect[detect] {
		if strings.HasPrefix(ores, "{") {
			res = `{"command":"` + details.command + `","detect":"` + detect + `","hook":` + jshookName + `,"key":` + jskey + `,"time":` + jstime + `,` + ores[1:]
		}
		msgs = append(msgs, res)
	}
	switch detect {
	case "enter":
		if fence.detect == nil || fence.detect["inside"] {
			msgs = append(msgs, `{"command":"`+details.command+`","detect":"inside","hook":`+jshookName+`,"key":`+jskey+`,"time":`+jstime+`,`+ores[1:])
		}
	case "exit", "cross":
		if fence.detect == nil || fence.detect["outside"] {
			msgs = append(msgs, `{"command":"`+details.command+`","detect":"outside","hook":`+jshookName+`,"key":`+jskey+`,"time":`+jstime+`,`+ores[1:])
		}
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
		}
		return obj.WithinBBox(geojson.BBox{
			Min: geojson.Position{X: fence.minLon, Y: fence.minLat, Z: 0},
			Max: geojson.Position{X: fence.maxLon, Y: fence.maxLat, Z: 0},
		})
	} else if fence.cmd == "intersects" {
		if fence.o != nil {
			return obj.Intersects(fence.o)
		}
		return obj.IntersectsBBox(geojson.BBox{
			Min: geojson.Position{X: fence.minLon, Y: fence.minLat, Z: 0},
			Max: geojson.Position{X: fence.maxLon, Y: fence.maxLat, Z: 0},
		})
	}
	return false
}
