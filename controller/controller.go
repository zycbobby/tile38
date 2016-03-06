package controller

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/google/btree"
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
	fcup      bool // follow caught up
	shrinking bool // aof shrinking flag
}

// Config is a tile38 config
type Config struct {
	FollowHost string `json:"follow_host,omitempty"`
	FollowPort int    `json:"follow_port,omitempty"`
	FollowID   string `json:"follow_id,omitempty"`
	FollowPos  int    `json:"follow_pos,omitempty"`
	ServerID   string `json:"server_id,omitempty"`
	ReadOnly   bool   `json:"read_only,omitempty"`
}

// ListenAndServe starts a new tile38 server
func ListenAndServe(host string, port int, dir string) error {
	log.Infof("Server started, Tile38 version %s, git %s", core.Version, core.GitSHA)
	c := &Controller{
		host:    host,
		port:    port,
		dir:     dir,
		cols:    btree.New(16),
		colsm:   make(map[string]*collection.Collection),
		follows: make(map[*bytes.Buffer]bool),
		fcond:   sync.NewCond(&sync.Mutex{}),
		lives:   make(map[*liveBuffer]bool),
		lcond:   sync.NewCond(&sync.Mutex{}),
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
	handler := func(command []byte, conn net.Conn, rd *bufio.Reader, w io.Writer, websocket bool) error {
		err := c.handleInputCommand(string(command), w)
		if err != nil {
			if err.Error() == "going live" {
				return c.goLive(err, conn, rd, websocket)
			}
			return err
		}
		return nil
	}
	return server.ListenAndServe(host, port, handler)
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

func (c *Controller) handleInputCommand(line string, w io.Writer) error {
	if core.ShowDebugMessages && line != "pInG" {
		log.Debug(line)
	}
	start := time.Now()
	// Ping and Help. Just send back the response. No need to put through the pipeline.
	if len(line) == 4 && (line[0] == 'p' || line[0] == 'P') && lc(line, "ping") {
		w.Write([]byte(`{"ok":true,"ping":"pong","elapsed":"` + time.Now().Sub(start).String() + `"}`))
		return nil
	}

	writeErr := func(err error) error {
		js := `{"ok":false,"err":` + jsonString(err.Error()) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
		if _, err := w.Write([]byte(js)); err != nil {
			return err
		}
		return nil
	}

	var write bool
	_, cmd := tokenlc(line)
	if cmd == "" {
		return writeErr(errors.New("empty command"))
	}
	// choose the locking strategy
	switch cmd {
	default:
		c.mu.RLock()
		defer c.mu.RUnlock()
	case "set", "del", "drop", "fset", "flushdb":
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
	case "get", "keys", "scan", "nearby", "within", "intersects":
		// read operations
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.config.FollowHost != "" && !c.fcup {
			return writeErr(errors.New("catching up to leader"))
		}
	case "follow", "readonly":
		// system operations
		// does not write to aof, but requires a write lock.
		c.mu.Lock()
		defer c.mu.Unlock()
	case "massinsert":
		// dev operation
		// ** danger zone **
		// no locks! DEV MODE ONLY
	}

	resp, d, err := c.command(line, w)
	if err != nil {
		if err.Error() == "going live" {
			return err
		}
		return writeErr(err)
	}
	if write {
		if err := c.writeAOF(line, &d); err != nil {
			log.Fatal(err)
			return err
		}
	}
	if resp != "" {
		if _, err := io.WriteString(w, resp); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) loadConfig() error {
	data, err := ioutil.ReadFile(c.dir + "/config")
	if err != nil {
		if os.IsNotExist(err) {
			return c.initConfig()
		}
		return err
	}
	err = json.Unmarshal(data, &c.config)
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) initConfig() error {
	c.config = Config{ServerID: randomKey(16)}
	return c.writeConfig()
}

func (c *Controller) writeConfig() error {
	data, err := json.MarshalIndent(c.config, "", "\t")
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(c.dir+"/config", data, 0600); err != nil {
		return err
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

func (c *Controller) command(line string, w io.Writer) (resp string, d commandDetailsT, err error) {
	start := time.Now()
	okResp := func() string {
		if w == nil {
			return ""
		}
		return `{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
	}
	nline, cmd := tokenlc(line)
	switch cmd {
	default:
		err = fmt.Errorf("unknown command '%s'", cmd)
		return
	// lock
	case "set":
		d, err = c.cmdSet(nline)
		resp = okResp()
	case "fset":
		d, err = c.cmdFset(nline)
		resp = okResp()
	case "del":
		d, err = c.cmdDel(nline)
		resp = okResp()
	case "drop":
		d, err = c.cmdDrop(nline)
		resp = okResp()
	case "flushdb":
		d, err = c.cmdFlushDB(nline)
		resp = okResp()
	case "massinsert":
		if !core.DevMode {
			err = fmt.Errorf("unknown command '%s'", cmd)
			return
		}
		err = c.cmdMassInsert(nline)
		resp = okResp()
	case "follow":
		err = c.cmdFollow(nline)
		resp = okResp()
	case "readonly":
		err = c.cmdReadOnly(nline)
		resp = okResp()
	case "stats":
		resp, err = c.cmdServer(nline)
	case "server":
		resp, err = c.cmdStats(nline)
	case "scan":
		err = c.cmdScan(nline, w)
	case "nearby":
		err = c.cmdNearby(nline, w)
	case "within":
		err = c.cmdWithin(nline, w)
	case "intersects":
		err = c.cmdIntersects(nline, w)
	case "get":
		resp, err = c.cmdGet(nline)
	case "keys":
		err = c.cmdKeys(nline, w)
	case "aof":
		err = c.cmdAOF(nline, w)
	case "aofmd5":
		resp, err = c.cmdAOFMD5(nline)
	case "gc":
		go runtime.GC()
		resp = okResp()
	case "aofshrink":
		go c.aofshrink()
		resp = okResp()
	}
	return
}
