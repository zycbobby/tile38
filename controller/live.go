package controller

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/tidwall/tile38/client"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
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

func (c *Controller) goLive(inerr error, conn net.Conn, rd *server.AnyReaderWriter, msg *server.Message, websocket bool) error {
	addr := conn.RemoteAddr().String()
	log.Info("live " + addr)
	defer func() {
		log.Info("not live " + addr)
	}()
	if s, ok := inerr.(liveAOFSwitches); ok {
		return c.liveAOF(s.pos, conn, rd, msg)
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
		var msg *server.Message
		panic("todo: goLive message must be defined")
		sw, err = c.newScanWriter(&wr, msg, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
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
			v, err := rd.ReadMessage()
			if err != nil {
				if err != io.EOF {
					log.Error(err)
				}
				return
			}
			switch v.Command {
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
			lb.cond.L.Unlock()
			msgs := c.FenceMatch("", sw, fence, details, true)
			for _, msg := range msgs {
				if err := writeMessage(conn, msg, websocket); err != nil {
					return nil // nil return is fine here
				}
			}
			lb.cond.L.Lock()
		}
		lb.cond.Wait()
		lb.cond.L.Unlock()
	}
}
