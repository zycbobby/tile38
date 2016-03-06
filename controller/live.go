package controller

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/tidwall/tile38/client"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/geojson"
)

type liveBuffer struct {
	key     string
	glob    string
	fence   *liveFenceSwitches
	details []*commandDetailsT
	cond    *sync.Cond
}

func (c *Controller) processLives() {
	for {
		c.lcond.L.Lock()
		for len(c.lstack) > 0 {
			item := c.lstack[0]
			c.lstack = c.lstack[1:]
			if len(c.lstack) == 0 {
				c.lstack = nil
			}
			for lb := range c.lives {
				lb.cond.L.Lock()
				if lb.key != "" && lb.key == item.key {
					lb.details = append(lb.details, item)
					lb.cond.Broadcast()
				}
				lb.cond.L.Unlock()
			}
		}
		c.lcond.Wait()
		c.lcond.L.Unlock()
	}
}

func writeMessage(conn net.Conn, message []byte, websocket bool) error {
	if websocket {
		return client.WriteWebSocket(conn, message)
	}
	return client.WriteMessage(conn, message)
}

func (c *Controller) goLive(inerr error, conn net.Conn, rd *bufio.Reader, websocket bool) error {
	addr := conn.RemoteAddr().String()
	log.Info("live " + addr)
	defer func() {
		log.Info("not live " + addr)
	}()
	if s, ok := inerr.(liveAOFSwitches); ok {
		return c.liveAOF(s.pos, conn, rd)
	}
	lb := &liveBuffer{
		cond: sync.NewCond(&sync.Mutex{}),
	}
	var err error
	var sw *scanWriter
	var wr bytes.Buffer
	switch s := inerr.(type) {
	default:
		return errors.New("invalid switch")
	case liveFenceSwitches:
		lb.glob = s.glob
		lb.key = s.key
		lb.fence = &s
		c.mu.RLock()
		sw, err = c.newScanWriter(&wr, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
		c.mu.RUnlock()
	}
	// everything below if for live SCAN, NEARBY, WITHIN, INTERSECTS
	if err != nil {
		return err
	}
	c.lcond.L.Lock()
	c.lives[lb] = true
	c.lcond.L.Unlock()
	defer func() {
		c.lcond.L.Lock()
		delete(c.lives, lb)
		c.lcond.L.Unlock()
		conn.Close()
	}()

	var mustQuit bool
	go func() {
		defer func() {
			lb.cond.L.Lock()
			mustQuit = true
			lb.cond.Broadcast()
			lb.cond.L.Unlock()
			conn.Close()
		}()
		for {
			command, _, err := client.ReadMessage(rd, nil)
			if err != nil {
				if err != io.EOF {
					log.Error(err)
				}
				return
			}
			switch strings.ToLower(string(command)) {
			default:
				log.Error("received a live command that was not QUIT")
				return
			case "quit", "":
				return
			}
		}
	}()
	if err := writeMessage(conn, []byte(client.LiveJSON), websocket); err != nil {
		return nil // nil return is fine here
	}
	var col *collection.Collection
	for {
		lb.cond.L.Lock()
		if mustQuit {
			lb.cond.L.Unlock()
			return nil
		}
		for len(lb.details) > 0 {
			details := lb.details[0]
			lb.details = lb.details[1:]
			if len(lb.details) == 0 {
				lb.details = nil
			}
			fence := lb.fence
			glob := lb.glob
			lb.cond.L.Unlock()
			if details.command == "drop" {
				col = nil
				if err := writeMessage(conn, []byte(`{"cmd":"drop"}`), websocket); err != nil {
					return nil
				}
			} else {
				match := true
				if glob != "" && glob != "*" {
					match, _ = globMatch(glob, details.id)
				}
				if match {
					if details.obj != nil && !(details.command == "fset" && sw.nofields) {
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
						if match {
							if details.command == "del" {
								if err := writeMessage(conn, []byte(`{"command":"del","id":`+jsonString(details.id)+`}`), websocket); err != nil {
									return nil
								}
							} else {
								var fmap map[string]int
								c.mu.RLock()
								if col == nil {
									col = c.getCol(details.key)
								}
								if col != nil {
									fmap = col.FieldMap()
								}
								c.mu.RUnlock()
								if fmap != nil {
									sw.fmap = fmap
									sw.fullFields = true
									sw.writeObject(details.id, details.obj, details.fields)
									if wr.Len() > 0 {
										res := wr.String()
										wr.Reset()
										if strings.HasPrefix(res, ",") {
											res = res[1:]
										}
										if sw.output == outputIDs {
											res = `{"id":` + res + `}`
										}
										if strings.HasPrefix(res, "{") {
											res = `{"command":"` + details.command + `","detect":"` + detect + `",` + res[1:]
										}
										if err := writeMessage(conn, []byte(res), websocket); err != nil {
											return nil
										}
									}
								}
							}
						}
					}
				}
			}
			lb.cond.L.Lock()
		}
		lb.cond.Wait()
		lb.cond.L.Unlock()
	}
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
