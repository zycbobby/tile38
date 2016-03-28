package controller

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/geojson"
)

type collectionT struct {
	Key        string
	Collection *collection.Collection
}

type commandDetailsT struct {
	command   string
	key, id   string
	field     string
	value     float64
	obj       geojson.Object
	fields    []float64
	oldObj    geojson.Object
	oldFields []float64
	updated   bool
}

func (col *collectionT) Less(item btree.Item) bool {
	return col.Key < item.(*collectionT).Key
}

// Controller is a tile38 controller
type Controller struct {
	mu        sync.RWMutex
	host      string
	port      int
	f         *os.File
	cols      *btree.BTree                      // use both tree and map. provide ordering.
	colsm     map[string]*collection.Collection // use both tree and map. provide performance.
	aofsz     int
	dir       string
	config    Config
	followc   uint64 // counter increases when follow property changes
	follows   map[*bytes.Buffer]bool
	fcond     *sync.Cond
	lstack    []*commandDetailsT
	lives     map[*liveBuffer]bool
	lcond     *sync.Cond
	fcup      bool                        // follow caught up
	shrinking bool                        // aof shrinking flag
	hooks     map[string]*Hook            // hook name
	hookcols  map[string]map[string]*Hook // col key
}

// ListenAndServe starts a new tile38 server
func ListenAndServe(host string, port int, dir string) error {
	log.Infof("Server started, Tile38 version %s, git %s", core.Version, core.GitSHA)
	c := &Controller{
		host:     host,
		port:     port,
		dir:      dir,
		cols:     btree.New(16),
		colsm:    make(map[string]*collection.Collection),
		follows:  make(map[*bytes.Buffer]bool),
		fcond:    sync.NewCond(&sync.Mutex{}),
		lives:    make(map[*liveBuffer]bool),
		lcond:    sync.NewCond(&sync.Mutex{}),
		hooks:    make(map[string]*Hook),
		hookcols: make(map[string]map[string]*Hook),
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := c.loadConfig(); err != nil {
		return err
	}
	f, err := os.OpenFile(dir+"/aof", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	c.f = f
	if err := c.loadAOF(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.config.FollowHost != "" {
		go c.follow(c.config.FollowHost, c.config.FollowPort, c.followc)
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.followc++ // this will force any follow communication to die
		c.mu.Unlock()
	}()
	go c.processLives()
	handler := func(conn *server.Conn, msg *server.Message, rd *bufio.Reader, w io.Writer, websocket bool) error {
		err := c.handleInputCommand(conn, msg, w)
		if err != nil {
			if err.Error() == "going live" {
				return c.goLive(err, conn, rd, websocket)
			}
			return err
		}
		return nil
	}
	protected := func() bool {
		if core.ProtectedMode == "no" {
			// --protected-mode no
			return false
		}
		if host != "" && host != "127.0.0.1" && host != "::1" && host != "localhost" {
			// -h address
			return false
		}
		c.mu.RLock()
		is := c.config.ProtectedMode != "no" && c.config.RequirePass == ""
		c.mu.RUnlock()
		return is
	}
	return server.ListenAndServe(host, port, protected, handler)
}

func (c *Controller) setCol(key string, col *collection.Collection) {
	c.cols.ReplaceOrInsert(&collectionT{Key: key, Collection: col})
	c.colsm[key] = col
}

func (c *Controller) getCol(key string) *collection.Collection {
	col, ok := c.colsm[key]
	if !ok {
		return nil
	}
	return col
}

func (c *Controller) deleteCol(key string) *collection.Collection {
	delete(c.colsm, key)
	i := c.cols.Delete(&collectionT{Key: key})
	if i == nil {
		return nil
	}
	return i.(*collectionT).Collection
}

func isReservedFieldName(field string) bool {
	switch field {
	case "z", "lat", "lon":
		return true
	}
	return false
}

func (c *Controller) handleInputCommand(conn *server.Conn, msg *server.Message, w io.Writer) error {
	var words []string
	for _, v := range msg.Values {
		words = append(words, v.String())
	}
	// line := strings.Join(words, " ")

	// if core.ShowDebugMessages && line != "pInG" {
	// 	log.Debug(line)
	// }
	start := time.Now()

	// Ping. Just send back the response. No need to put through the pipeline.
	if msg.Command == "ping" {
		switch msg.OutputType {
		case server.JSON:
			w.Write([]byte(`{"ok":true,"ping":"pong","elapsed":"` + time.Now().Sub(start).String() + `"}`))
		case server.RESP:
			io.WriteString(w, "+PONG\r\n")
		}
		return nil
	}

	writeErr := func(err error) error {
		switch msg.OutputType {
		case server.JSON:
			io.WriteString(w, `{"ok":false,"err":`+jsonString(err.Error())+`,"elapsed":"`+time.Now().Sub(start).String()+"\"}")
		case server.RESP:
			if err == errInvalidNumberOfArguments {
				io.WriteString(w, "-ERR wrong number of arguments for '"+msg.Command+"' command\r\n")
			} else {
				v, _ := resp.ErrorValue(errors.New("ERR " + err.Error())).MarshalRESP()
				io.WriteString(w, string(v))
			}
		}
		return nil
	}

	var write bool

	if !conn.Authenticated || msg.Command == "auth" {
		c.mu.RLock()
		requirePass := c.config.RequirePass
		c.mu.RUnlock()
		if requirePass != "" {
			// This better be an AUTH command.
			if msg.Command != "auth" {
				// Just shut down the pipeline now. The less the client connection knows the better.
				return writeErr(errors.New("authentication required"))
			}
			password := ""
			if len(msg.Values) > 1 {
				password = msg.Values[1].String()
			}
			if requirePass != strings.TrimSpace(password) {
				return writeErr(errors.New("invalid password"))
			}
			conn.Authenticated = true
			w.Write([]byte(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}"))
			return nil
		} else if msg.Command == "auth" {
			return writeErr(errors.New("invalid password"))
		}
	}

	// choose the locking strategy
	switch msg.Command {
	default:
		c.mu.RLock()
		defer c.mu.RUnlock()
	case "set", "del", "drop", "fset", "flushdb", "sethook", "delhook":
		// write operations
		write = true
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.config.FollowHost != "" {
			return writeErr(errors.New("not the leader"))
		}
		if c.config.ReadOnly {
			return writeErr(errors.New("read only"))
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks":
		// read operations
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.config.FollowHost != "" && !c.fcup {
			return writeErr(errors.New("catching up to leader"))
		}
	case "follow", "readonly", "config":
		// system operations
		// does not write to aof, but requires a write lock.
		c.mu.Lock()
		defer c.mu.Unlock()
	case "massinsert":
		// dev operation
		// ** danger zone **
		// no locks! DEV MODE ONLY
	}

	res, d, err := c.command(msg, w)
	if err != nil {
		if err.Error() == "going live" {
			return err
		}
		return writeErr(err)
	}
	if write {
		if err := c.writeAOF(resp.ArrayValue(msg.Values), &d); err != nil {
			if _, ok := err.(errAOFHook); ok {
				return writeErr(err)
			}
			log.Fatal(err)
			return err
		}
	}
	if res != "" {
		if _, err := io.WriteString(w, res); err != nil {
			return err
		}
	}
	return nil
}

func randomKey(n int) string {
	b := make([]byte, n)
	nn, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	if nn != n {
		panic("random failed")
	}
	return fmt.Sprintf("%x", b)
}

func (c *Controller) reset() {
	c.aofsz = 0
	c.cols = btree.New(16)
}

func (c *Controller) command(msg *server.Message, w io.Writer) (res string, d commandDetailsT, err error) {
	start := time.Now()
	okResp := func() string {
		if w != nil {
			switch msg.OutputType {
			case server.JSON:
				return `{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
			case server.RESP:
				return "+OK\r\n"
			}
		}
		return ""
	}
	okResp = okResp
	switch msg.Command {
	default:
		err = fmt.Errorf("unknown command '%s'", msg.Values[0])
		return
	// lock
	case "set":
		res, d, err = c.cmdSet(msg)
	case "fset":
		res, d, err = c.cmdFset(msg)
	case "del":
		res, d, err = c.cmdDel(msg)
	case "drop":
		res, d, err = c.cmdDrop(msg)
	case "flushdb":
		res, d, err = c.cmdFlushDB(msg)
	// case "sethook":
	// 	err = c.cmdSetHook(nline)
	// 	resp = okResp()
	// case "delhook":
	// 	err = c.cmdDelHook(nline)
	// 	resp = okResp()
	// case "hooks":
	// 	err = c.cmdHooks(nline, w)
	// case "massinsert":
	// 	if !core.DevMode {
	// 		err = fmt.Errorf("unknown command '%s'", cmd)
	// 		return
	// 	}
	// 	err = c.cmdMassInsert(nline)
	// 	resp = okResp()
	// case "follow":
	// 	err = c.cmdFollow(nline)
	// 	resp = okResp()
	// case "config":
	// 	resp, err = c.cmdConfig(nline)
	// case "readonly":
	// 	err = c.cmdReadOnly(nline)
	// 	resp = okResp()
	// case "stats":
	// 	resp, err = c.cmdStats(nline)
	// case "server":
	// 	resp, err = c.cmdServer(nline)
	case "scan":
		res, err = c.cmdScan(msg)
	case "nearby":
		res, err = c.cmdNearby(msg)
	case "within":
		res, err = c.cmdWithin(msg)
	case "intersects":
		res, err = c.cmdIntersects(msg)
	case "get":
		res, err = c.cmdGet(msg)
		// case "keys":
		// 	err = c.cmdKeys(nline, w)
		// case "aof":
		// 	err = c.cmdAOF(nline, w)
		// case "aofmd5":
		// 	resp, err = c.cmdAOFMD5(nline)
		// case "gc":
		// 	go runtime.GC()
		// 	resp = okResp()
		// case "aofshrink":
		// 	go c.aofshrink()
		// 	resp = okResp()
	}
	return
}
