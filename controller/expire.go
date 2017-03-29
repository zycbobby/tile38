package controller

import (
	"log"
	"math/rand"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/server"
)

type exitem struct {
	key, id string
	at      time.Time
}

func (a *exitem) Less(v btree.Item, ctx interface{}) bool {
	b := v.(*exitem)
	if a.at.Before(b.at) {
		return true
	}
	if a.at.After(b.at) {
		return false
	}
	if a.key < b.key {
		return true
	}
	if a.key > b.key {
		return false
	}
	return a.id < b.id
}

// clearAllExpires removes all items that are marked at expires.
func (c *Controller) clearAllExpires() {
	c.expires = make(map[string]map[string]time.Time)
}

// clearIDExpires clears a single item from the expires list.
func (c *Controller) clearIDExpires(key, id string) (cleared bool) {
	if len(c.expires) == 0 {
		return false
	}
	m, ok := c.expires[key]
	if !ok {
		return false
	}
	_, ok = m[id]
	if !ok {
		return false
	}
	delete(m, id)
	return true
}

// clearKeyExpires clears all items that are marked as expires from a single key.
func (c *Controller) clearKeyExpires(key string) {
	delete(c.expires, key)
}

// expireAt marks an item as expires at a specific time.
func (c *Controller) expireAt(key, id string, at time.Time) {
	m := c.expires[key]
	if m == nil {
		m = make(map[string]time.Time)
		c.expires[key] = m
	}
	m[id] = at
	if c.exlist != nil {
		c.exlist = append(c.exlist, exitem{key, id, at})
	}
}

// getExpires returns the when an item expires.
func (c *Controller) getExpires(key, id string) (at time.Time, ok bool) {
	if len(c.expires) == 0 {
		return at, false
	}
	m, ok := c.expires[key]
	if !ok {
		return at, false
	}
	at, ok = m[id]
	return at, ok
}

// hasExpired returns true if an item has expired.
func (c *Controller) hasExpired(key, id string) bool {
	at, ok := c.getExpires(key, id)
	if !ok {
		return false
	}
	return time.Now().After(at)
}

func (c *Controller) fillExpiresList() {
	c.exlist = make([]exitem, 0)
	for key, m := range c.expires {
		for id, at := range m {
			c.exlist = append(c.exlist, exitem{key, id, at})
		}
	}
}

// backgroundExpiring watches for when items that have expired must be purged
// from the database. It's executes 10 times a seconds.
func (c *Controller) backgroundExpiring() {
	rand.Seed(time.Now().UnixNano())
	for {
		c.mu.Lock()
		if c.stopBackgroundExpiring {
			c.mu.Unlock()
			return
		}
		now := time.Now()
		var purged int
		for i := 0; i < 20 && len(c.exlist) > 0; i++ {
			ix := rand.Int() % len(c.exlist)
			if now.After(c.exlist[ix].at) {
				if c.hasExpired(c.exlist[ix].key, c.exlist[ix].id) {
					msg := &server.Message{}
					msg.Values = resp.MultiBulkValue("del", c.exlist[ix].key, c.exlist[ix].id).Array()
					msg.Command = "del"
					_, d, err := c.cmdDel(msg)
					if err != nil {
						c.mu.Unlock()
						log.Fatal(err)
						continue
					}
					if err := c.writeAOF(resp.ArrayValue(msg.Values), &d); err != nil {
						c.mu.Unlock()
						log.Fatal(err)
						continue
					}
					purged++
				}
				c.exlist[ix] = c.exlist[len(c.exlist)-1]
				c.exlist = c.exlist[:len(c.exlist)-1]
			}
		}
		c.mu.Unlock()
		if purged > 5 {
			continue
		}
		time.Sleep(time.Second / 10)
	}
}
