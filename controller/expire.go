package controller

import (
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

// clearAllExpires removes all items that are marked at expires.
func (c *Controller) clearAllExpires() {
	c.expires = make(map[string]map[string]time.Time)
}

// clearIDExpires will clear a single item from the expires list.
func (c *Controller) clearIDExpires(key, id string) {
	m := c.expires[key]
	if m == nil {
		return
	}
	delete(m, id)
	if len(m) == 0 {
		delete(c.expires, key)
	}
}

// clearKeyExpires will clear all items that are marked as expires from a single key.
func (c *Controller) clearKeyExpires(key string) {
	delete(c.expires, key)
}

// expireAt will mark an item as expires at a specific time.
func (c *Controller) expireAt(key, id string, at time.Time) {
	m := c.expires[key]
	if m == nil {
		m = make(map[string]time.Time)
		c.expires[key] = m
	}
	m[id] = at
}

// getExpires will return the when the item expires.
func (c *Controller) getExpires(key, id string) (at time.Time, ok bool) {
	m := c.expires[key]
	if m == nil {
		ok = false
		return
	}
	at, ok = m[id]
	return
}

// backgroundExpiring watches for when items must expire from the database.
// It's runs through every item that has been marked as expires five times
// per second.
func (c *Controller) backgroundExpiring() {
	for {
		c.mu.Lock()
		if c.stopBackgroundExpiring {
			c.mu.Unlock()
			return
		}
		// Only excute for leaders. Followers should ignore.
		if c.config.FollowHost == "" {
			now := time.Now()
			for key, m := range c.expires {
				for id, at := range m {
					if now.After(at) {
						// issue a DEL command
						c.mu.Lock()
						c.statsExpired++
						c.mu.Unlock()
						msg := &server.Message{}
						msg.Values = resp.MultiBulkValue("del", key, id).Array()
						msg.Command = "del"
						_, d, err := c.cmdDel(msg)
						if err != nil {
							log.Fatal(err)
							continue
						}
						if err := c.writeAOF(resp.ArrayValue(msg.Values), &d); err != nil {
							log.Fatal(err)
							continue
						}
					}
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second / 5)
	}
}
