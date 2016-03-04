package controller

import (
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/google/btree"
)

func (c *Controller) cmdStats(line string) (string, error) {
	start := time.Now()
	var key string
	var ms = []map[string]interface{}{}
	if line == "" {
		return "", errInvalidNumberOfArguments
	}
	for len(line) > 0 {
		line, key = token(line)
		col := c.getCol(key)
		if col != nil {
			m := make(map[string]interface{})
			points := col.PointCount()
			m["num_points"] = points
			m["in_memory_size"] = col.TotalWeight()
			m["num_objects"] = col.Count()
			ms = append(ms, m)
		} else {
			ms = append(ms, nil)
		}
	}
	data, err := json.Marshal(ms)
	if err != nil {
		return "", err
	}
	return `{"ok":true,"stats":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}", nil
}
func (c *Controller) cmdServer(line string) (string, error) {
	start := time.Now()
	if line != "" {
		return "", errInvalidNumberOfArguments
	}
	m := make(map[string]interface{})
	m["id"] = c.config.ServerID
	if c.config.FollowHost != "" {
		m["following"] = fmt.Sprintf("%s:%d", c.config.FollowHost, c.config.FollowPort)
		m["caught_up"] = c.fcup
	}
	m["aof_size"] = c.aofsz
	m["num_collections"] = c.cols.Len()
	sz := 0
	c.cols.Ascend(func(item btree.Item) bool {
		col := item.(*collectionT).Collection
		sz += col.TotalWeight()
		return true
	})
	m["in_memory_size"] = sz
	points := 0
	objects := 0
	c.cols.Ascend(func(item btree.Item) bool {
		col := item.(*collectionT).Collection
		points += col.PointCount()
		objects += col.Count()
		return true
	})
	m["num_points"] = points
	m["num_objects"] = objects
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	avgsz := 0
	if points != 0 {
		avgsz = int(mem.HeapAlloc) / points
	}
	m["heap_size"] = mem.HeapAlloc
	m["avg_item_size"] = avgsz
	m["pointer_size"] = (32 << uintptr(uint64(^uintptr(0))>>63)) / 8
	m["read_only"] = c.config.ReadOnly

	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return `{"ok":true,"stats":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}", nil
}
func (c *Controller) statsCollections(line string) (string, error) {
	start := time.Now()
	var key string
	var ms = []map[string]interface{}{}
	for len(line) > 0 {
		line, key = token(line)
		col := c.getCol(key)
		if col != nil {
			m := make(map[string]interface{})
			points := col.PointCount()
			m["num_points"] = points
			m["in_memory_size"] = col.TotalWeight()
			m["num_objects"] = col.Count()
			ms = append(ms, m)
		} else {
			ms = append(ms, nil)
		}
	}
	data, err := json.Marshal(ms)
	if err != nil {
		return "", err
	}
	return `{"ok":true,"stats":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}", nil
}
