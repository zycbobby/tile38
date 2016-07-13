package controller

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/geojson"
)

var errOOM = errors.New("OOM command not allowed when used memory > 'maxmemory'")

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
	fmap      map[string]int
	oldObj    geojson.Object
	oldFields []float64
	updated   bool
	revert    func()
	timestamp time.Time
}

func (col *collectionT) Less(item btree.Item, ctx int) bool {
	return col.Key < item.(*collectionT).Key
}

// Controller is a tile38 controller
type Controller struct {
	mu        sync.RWMutex
	host      string
	port      int
	f         *os.File
	cols      *btree.BTree
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
	aofconnM  map[net.Conn]bool

	stopWatchingMemory bool
	outOfMemory        bool
}

// ListenAndServe starts a new tile38 server
func ListenAndServe(host string, port int, dir string) error {
	log.Infof("Server started, Tile38 version %s, git %s", core.Version, core.GitSHA)
	c := &Controller{
		host:     host,
		port:     port,
		dir:      dir,
		cols:     btree.New(16, 0),
		follows:  make(map[*bytes.Buffer]bool),
		fcond:    sync.NewCond(&sync.Mutex{}),
		lives:    make(map[*liveBuffer]bool),
		lcond:    sync.NewCond(&sync.Mutex{}),
		hooks:    make(map[string]*Hook),
		hookcols: make(map[string]map[string]*Hook),
		aofconnM: make(map[net.Conn]bool),
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := c.loadConfig(); err != nil {
		return err
	}
	if err := c.migrateAOF(); err != nil {
		return err
	}
	f, err := os.OpenFile(path.Join(dir, "appendonly.aof"), os.O_CREATE|os.O_RDWR, 0600)
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
	go c.watchMemory()
	defer func() {
		c.mu.Lock()
		c.stopWatchingMemory = true
		c.mu.Unlock()
	}()
	handler := func(conn *server.Conn, msg *server.Message, rd *server.AnyReaderWriter, w io.Writer, websocket bool) error {
		err := c.handleInputCommand(conn, msg, w)
		if err != nil {
			if err.Error() == "going live" {
				return c.goLive(err, conn, rd, msg, websocket)
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

func (c *Controller) watchMemory() {
	t := time.NewTicker(time.Second * 2)
	defer t.Stop()
	var mem runtime.MemStats
	for range t.C {
		func() {
			c.mu.RLock()
			if c.stopWatchingMemory {
				c.mu.RUnlock()
				return
			}
			maxmem := c.config.MaxMemory
			oom := c.outOfMemory
			c.mu.RUnlock()
			if maxmem == 0 {
				if oom {
					c.mu.Lock()
					c.outOfMemory = false
					c.mu.Unlock()
				}
				return
			}
			if oom {
				runtime.GC()
			}
			runtime.ReadMemStats(&mem)
			c.mu.Lock()
			c.outOfMemory = int(mem.HeapAlloc) > maxmem
			c.mu.Unlock()
		}()
	}
}

func (c *Controller) setCol(key string, col *collection.Collection) {
	c.cols.ReplaceOrInsert(&collectionT{Key: key, Collection: col})
}

func (c *Controller) getCol(key string) *collection.Collection {
	item := c.cols.Get(&collectionT{Key: key})
	if item == nil {
		return nil
	}
	return item.(*collectionT).Collection
}

func (c *Controller) scanGreaterOrEqual(key string, iterator func(key string, col *collection.Collection) bool) {
	c.cols.AscendGreaterOrEqual(&collectionT{Key: key}, func(item btree.Item) bool {
		col := item.(*collectionT)
		return iterator(col.Key, col.Collection)
	})
}

func (c *Controller) deleteCol(key string) *collection.Collection {
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
	start := time.Now()
	writeOutput := func(res string) error {
		switch msg.ConnType {
		default:
			err := fmt.Errorf("unsupported conn type: %v", msg.ConnType)
			log.Error(err)
			return err
		case server.WebSocket:
			return server.WriteWebSocketMessage(w, []byte(res))
		case server.HTTP:
			_, err := fmt.Fprintf(w, "HTTP/1.1 200 OK\r\n"+
				"Connection: close\r\n"+
				"Content-Length: %d\r\n"+
				"Content-Type: application/json charset=utf-8\r\n"+
				"\r\n", len(res)+2)
			if err != nil {
				return err
			}
			_, err = io.WriteString(w, res+"\r\n")
			return err
		case server.RESP:
			var err error
			if msg.OutputType == server.JSON {
				_, err = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(res), res)
			} else {
				_, err = io.WriteString(w, res)
			}
			return err
		case server.Native:
			_, err := fmt.Fprintf(w, "$%d %s\r\n", len(res), res)
			return err
		}
	}
	// Ping. Just send back the response. No need to put through the pipeline.
	if msg.Command == "ping" {
		switch msg.OutputType {
		case server.JSON:
			return writeOutput(`{"ok":true,"ping":"pong","elapsed":"` + time.Now().Sub(start).String() + `"}`)
		case server.RESP:
			return writeOutput("+PONG\r\n")
		}
		return nil
	}

	writeErr := func(err error) error {
		switch msg.OutputType {
		case server.JSON:
			return writeOutput(`{"ok":false,"err":` + jsonString(err.Error()) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		case server.RESP:
			if err == errInvalidNumberOfArguments {
				return writeOutput("-ERR wrong number of arguments for '" + msg.Command + "' command\r\n")
			}
			v, _ := resp.ErrorValue(errors.New("ERR " + err.Error())).MarshalRESP()
			return writeOutput(string(v))
		}
		return nil
	}

	var write bool

	if !conn.Authenticated || msg.Command == "auth" {
		c.mu.RLock()
		requirePass := c.config.RequirePass
		c.mu.RUnlock()
		if requirePass != "" {
			password := ""
			// This better be an AUTH command or the Message should contain an Auth
			if msg.Command != "auth" && msg.Auth == "" {
				// Just shut down the pipeline now. The less the client connection knows the better.
				return writeErr(errors.New("authentication required"))
			}
			if msg.Auth != "" {
				password = msg.Auth
			} else {
				if len(msg.Values) > 1 {
					password = msg.Values[1].String()
				}
			}
			if requirePass != strings.TrimSpace(password) {
				return writeErr(errors.New("invalid password"))
			}
			conn.Authenticated = true
			return writeOutput(server.OKMessage(msg, start))
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
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search":
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
	case "output":
		// this is local connection operation. Locks not needed.
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
		if err := writeOutput(res); err != nil {
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
	c.cols = btree.New(16, 0)
}

func (c *Controller) command(msg *server.Message, w io.Writer) (res string, d commandDetailsT, err error) {

	switch msg.Command {
	default:
		err = fmt.Errorf("unknown command '%s'", msg.Values[0])
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
	case "sethook":
		res, d, err = c.cmdSetHook(msg)
	case "delhook":
		res, d, err = c.cmdDelHook(msg)
	case "hooks":
		res, err = c.cmdHooks(msg)
	case "massinsert":
		if !core.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Values[0])
			return
		}
		res, err = c.cmdMassInsert(msg)
	case "follow":
		res, err = c.cmdFollow(msg)
	case "readonly":
		res, err = c.cmdReadOnly(msg)
	case "stats":
		res, err = c.cmdStats(msg)
	case "server":
		res, err = c.cmdServer(msg)
	case "scan":
		res, err = c.cmdScan(msg)
	case "nearby":
		res, err = c.cmdNearby(msg)
	case "within":
		res, err = c.cmdWithin(msg)
	case "intersects":
		res, err = c.cmdIntersects(msg)
	case "search":
		res, err = c.cmdSearch(msg)
	case "get":
		res, err = c.cmdGet(msg)
	case "keys":
		res, err = c.cmdKeys(msg)
	case "output":
		res, err = c.cmdOutput(msg)
	case "aof":
		res, err = c.cmdAOF(msg)
	case "aofmd5":
		res, err = c.cmdAOFMD5(msg)
	case "gc":
		go runtime.GC()
		res = server.OKMessage(msg, time.Now())
	case "aofshrink":
		go c.aofshrink()
		res = server.OKMessage(msg, time.Now())
	case "config get":
		res, err = c.cmdConfigGet(msg)
	case "config set":
		res, err = c.cmdConfigSet(msg)
	case "config rewrite":
		res, err = c.cmdConfigRewrite(msg)
	case "config":
		err = fmt.Errorf("unknown command '%s'", msg.Values[0])
		if len(msg.Values) > 1 {
			command := msg.Values[0].String() + " " + msg.Values[1].String()
			msg.Values[1] = resp.StringValue(command)
			msg.Values = msg.Values[1:]
			msg.Command = strings.ToLower(command)
			return c.command(msg, w)
		}
	}
	return
}
