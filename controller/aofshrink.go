package controller

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/geojson"
)

type objFields struct {
	obj    geojson.Object
	fields []float64
}

const maxKeyGroup = 10
const maxIDGroup = 10

// aofshrink shrinks the aof file to it's minimum size.
// There are some pauses but each pause should not take more that 100ms on a busy server.
func (c *Controller) aofshrink() {
	start := time.Now()
	c.mu.Lock()
	c.f.Sync()
	if c.shrinking {
		c.mu.Unlock()
		return
	}
	f, err := os.Create(path.Join(c.dir, "shrink"))
	if err != nil {
		log.Errorf("aof shrink failed: %s\n", err.Error())
		return
	}
	defer func() {
		f.Close()
		//os.RemoveAll(rewritePath)
	}()
	var ferr error // stores the final error
	c.shrinking = true
	endpos := int64(c.aofsz) // 1) Log the aofsize at start. Locked
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.shrinking = false
		defer func() {
			if ferr != nil {
				log.Errorf("aof shrink failed: %s\n", ferr.Error())
			} else {
				log.Printf("aof shrink completed in %s", time.Now().Sub(start))
			}
		}()
		if ferr != nil {
			return
		}

		of, err := os.Open(c.f.Name())
		if err != nil {
			ferr = err
			return
		}
		defer of.Close()
		if _, err := of.Seek(endpos, 0); err != nil {
			ferr = err
			return
		}
		rd := resp.NewReader(of)
		for {
			v, telnet, _, err := rd.ReadMultiBulk()
			if err != nil {
				if err == io.EOF {
					break
				}
				ferr = err
				return
			}
			if telnet {
				ferr = errors.New("invalid RESP message")
				return
			}
			data, err := v.MarshalRESP()
			if err != nil {
				ferr = err
				return
			}
			if _, err := f.Write(data); err != nil {
				ferr = err
				return
			}
			break
		}
		of.Close()
		// swap files
		f.Close()
		c.f.Close()
		err = os.Rename(path.Join(c.dir, "shrink"), path.Join(c.dir, "appendonly.aof"))
		if err != nil {
			log.Fatal("shink rename fatal operation")
		}
		c.f, err = os.OpenFile(path.Join(c.dir, "appendonly.aof"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			log.Fatal("shink openfile fatal operation")
		}
		var n int64
		n, err = c.f.Seek(0, 2)
		if err != nil {
			log.Fatal("shink seek end fatal operation")
		}
		c.aofsz = int(n)
		// kill all followers connections
		for conn, _ := range c.aofconnM {
			conn.Close()
		}
	}()
	log.Infof("aof shrink started at pos %d", endpos)

	// Ascend collections. Load maxKeyGroup at a time.
	nextKey := ""
	for {
		cols := make(map[string]*collection.Collection)
		c.mu.Lock()
		c.scanGreaterOrEqual(nextKey, func(key string, col *collection.Collection) bool {
			if key != nextKey {
				cols[key] = col
				nextKey = key
			}
			return len(cols) < maxKeyGroup
		})
		c.mu.Unlock()

		keys := make([]string, 0, maxKeyGroup)
		for key := range cols {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			col := cols[key]
			// Ascend objects. Load maxIDGroup at a time.
			nextID := ""
			for {
				objs := make(map[string]objFields)
				c.mu.Lock()
				fnames := col.FieldArr() // reload an array of field names to match each object
				col.ScanGreaterOrEqual(nextID, 0, func(id string, obj geojson.Object, fields []float64) bool {
					if id != nextID {
						objs[id] = objFields{obj, fields}
						nextID = id
					}
					return len(objs) < maxIDGroup
				})
				c.mu.Unlock()

				ids := make([]string, 0, maxIDGroup)
				for id := range objs {
					ids = append(ids, id)
				}
				sort.Strings(ids)

				linebuf := &bytes.Buffer{}
				for _, id := range ids {
					obj := objs[id]
					values := make([]resp.Value, 0, len(obj.fields)*3+16)
					values = append(values, resp.StringValue("set"), resp.StringValue(key), resp.StringValue(id))
					for i, fvalue := range obj.fields {
						if fvalue != 0 {
							values = append(values, resp.StringValue("field"), resp.StringValue(fnames[i]), resp.FloatValue(fvalue))
						}
					}
					switch obj := obj.obj.(type) {
					default:
						values = append(values, resp.StringValue("object"), resp.StringValue(obj.JSON()))
					case geojson.SimplePoint:
						values = append(values, resp.StringValue("point"), resp.FloatValue(obj.Y), resp.FloatValue(obj.X))
					case geojson.Point:
						if obj.Coordinates.Z == 0 {
							values = append(values, resp.StringValue("point"), resp.FloatValue(obj.Coordinates.Y), resp.FloatValue(obj.Coordinates.X))
						} else {
							values = append(values, resp.StringValue("point"), resp.FloatValue(obj.Coordinates.Y), resp.FloatValue(obj.Coordinates.X), resp.FloatValue(obj.Coordinates.Z))
						}
					}
					data, err := resp.ArrayValue(values).MarshalRESP()
					if err != nil {
						ferr = err
						return
					}
					linebuf.Write(data)
				}
				if _, err := f.Write(linebuf.Bytes()); err != nil {
					ferr = err
					return
				}
				if len(objs) < maxIDGroup {
					break
				}
			}
		}
		if len(cols) < maxKeyGroup {
			break
		}
	}

	// load hooks
	c.mu.Lock()
	for name, hook := range c.hooks {
		values := make([]resp.Value, 0, 3+len(hook.Message.Values))
		endpoints := make([]string, len(hook.Endpoints))
		for i, endpoint := range hook.Endpoints {
			endpoints[i] = endpoint.Original
		}
		values = append(values, resp.StringValue("sethook"), resp.StringValue(name), resp.StringValue(strings.Join(endpoints, ",")))
		values = append(values, hook.Message.Values...)
		data, err := resp.ArrayValue(values).MarshalRESP()
		if err != nil {
			c.mu.Unlock()
			ferr = err
			return
		}
		if _, err := f.Write(data); err != nil {
			c.mu.Unlock()
			ferr = err
			return
		}
	}
	c.mu.Unlock()

}
