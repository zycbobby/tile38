package client

import (
	"encoding/json"
	"errors"
)

// Standard represents a standard tile38 message.
type Standard struct {
	OK      bool   `json:"ok"`
	Err     string `json:"err"`
	Elapsed string `json:"elapsed"`
}

// Stats represents tile38 server statistics.
type Stats struct {
	Standard
	Stats struct {
		ServerID       string `json:"id"`
		Following      string `json:"following"`
		AOFSize        int    `json:"aof_size"`
		NumCollections int    `json:"num_collections"`
		InMemorySize   int    `json:"in_memory_size"`
		NumPoints      int    `json:"num_points"`
		NumObjects     int    `json:"num_objects"`
		HeapSize       int    `json:"heap_size"`
		AvgItemSize    int    `json:"avg_item_size"`
		PointerSize    int    `json:"pointer_size"`
	} `json:"stats"`
}

// Stats returns tile38 server statistics.
func (conn *Conn) Stats() (Stats, error) {
	var stats Stats
	msg, err := conn.Do("stats")
	if err != nil {
		return stats, err
	}
	if err := json.Unmarshal(msg, &stats); err != nil {
		return stats, err
	}
	if !stats.OK {
		if stats.Err == "" {
			return stats, errors.New(stats.Err)
		}
		return stats, errors.New("not ok")
	}
	return stats, nil
}
