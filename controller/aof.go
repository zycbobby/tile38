package controller

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/google/btree"
	"github.com/tidwall/tile38/client"
	"github.com/tidwall/tile38/controller/log"
)

const backwardsBufferSize = 50000

var errCorruptedAOF = errors.New("corrupted aof file")

type AOFReader struct {
	r     io.Reader // reader
	rerr  error     // read error
	chunk []byte    // chunk buffer
	buf   []byte    // main buffer
	l     int       // length of valid data in buffer
	p     int       // pointer
}

func (rd *AOFReader) ReadCommand() ([]byte, error) {
	if rd.l >= 4 {
		sz1 := int(binary.LittleEndian.Uint32(rd.buf[rd.p:]))
		if rd.l >= sz1+9 {
			// we have enough data for a record
			sz2 := int(binary.LittleEndian.Uint32(rd.buf[rd.p+4+sz1:]))
			if sz2 != sz1 || rd.buf[rd.p+4+sz1+4] != 0 {
				return nil, errCorruptedAOF
			}
			buf := rd.buf[rd.p+4 : rd.p+4+sz1]
			rd.p += sz1 + 9
			rd.l -= sz1 + 9
			return buf, nil
		}
	}
	// need more data
	if rd.rerr != nil {
		if rd.rerr == io.EOF {
			rd.rerr = nil // we want to return EOF, but we want to be able to try again
			if rd.l != 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, io.EOF
		}
		return nil, rd.rerr
	}
	if rd.p != 0 {
		// move p to the beginning
		copy(rd.buf, rd.buf[rd.p:rd.p+rd.l])
		rd.p = 0
	}
	var n int
	n, rd.rerr = rd.r.Read(rd.chunk)
	if n > 0 {
		cbuf := rd.chunk[:n]
		if len(rd.buf)-rd.l < n {
			if len(rd.buf) == 0 {
				rd.buf = make([]byte, len(cbuf))
				copy(rd.buf, cbuf)
			} else {
				copy(rd.buf[rd.l:], cbuf[:len(rd.buf)-rd.l])
				rd.buf = append(rd.buf, cbuf[len(rd.buf)-rd.l:]...)
			}
		} else {
			copy(rd.buf[rd.l:], cbuf)
		}
		rd.l += n
	}
	return rd.ReadCommand()
}

func NewAOFReader(r io.Reader) *AOFReader {
	rd := &AOFReader{r: r, chunk: make([]byte, 0xFFFF)}
	return rd
}

func (c *Controller) loadAOF() error {
	start := time.Now()
	var count int
	defer func() {
		d := time.Now().Sub(start)
		ps := float64(count) / (float64(d) / float64(time.Second))
		log.Infof("AOF loaded %d commands: %s: %.0f/sec", count, d, ps)
	}()
	rd := NewAOFReader(c.f)
	for {
		buf, err := rd.ReadCommand()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			if err == io.ErrUnexpectedEOF || err == errCorruptedAOF {
				log.Warnf("aof is corrupted, likely data loss. Truncating to %d", c.aofsz)
				fname := c.f.Name()
				c.f.Close()
				if err := os.Truncate(c.f.Name(), int64(c.aofsz)); err != nil {
					log.Fatalf("could not truncate aof, possible data loss. %s", err.Error())
					return err
				}
				c.f, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0600)
				if err != nil {
					log.Fatalf("could not create aof, possible data loss. %s", err.Error())
					return err
				}
				if _, err := c.f.Seek(int64(c.aofsz), 0); err != nil {
					log.Fatalf("could not seek aof, possible data loss. %s", err.Error())
					return err
				}
			}
			return err
		}

		if _, _, err := c.command(string(buf), nil); err != nil {
			return err
		}
		c.aofsz += 9 + len(buf)
		count++
	}
}

func writeCommand(w io.Writer, line []byte) (n int, err error) {
	b := make([]byte, len(line)+9)
	binary.LittleEndian.PutUint32(b, uint32(len(line)))
	copy(b[4:], line)
	binary.LittleEndian.PutUint32(b[len(b)-5:], uint32(len(line)))
	return w.Write(b)
}

func (c *Controller) writeAOF(line string, d *commandDetailsT) error {
	n, err := writeCommand(c.f, []byte(line))
	if err != nil {
		return err
	}
	c.aofsz += n

	// notify aof live connections that we have new data
	c.fcond.L.Lock()
	c.fcond.Broadcast()
	c.fcond.L.Unlock()

	// write to live connection streams
	if d != nil {
		c.lcond.L.Lock()
		c.lstack = append(c.lstack, d)
		c.lcond.Broadcast()
		c.lcond.L.Unlock()
	}
	return nil
}

type liveAOFSwitches struct {
	pos int64
}

func (s liveAOFSwitches) Error() string {
	return "going live"
}

func (c *Controller) cmdAOFMD5(line string) (string, error) {
	start := time.Now()
	var spos, ssize string
	if line, spos = token(line); spos == "" {
		return "", errInvalidNumberOfArguments
	}
	if line, ssize = token(line); ssize == "" {
		return "", errInvalidNumberOfArguments
	}
	if line != "" {
		return "", errInvalidNumberOfArguments
	}
	pos, err := strconv.ParseInt(spos, 10, 64)
	if err != nil || pos < 0 {
		return "", errInvalidArgument(spos)
	}
	size, err := strconv.ParseInt(ssize, 10, 64)
	if err != nil || size < 0 {
		return "", errInvalidArgument(ssize)
	}
	sum, err := c.checksum(pos, size)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"ok":true,"md5":"%s","elapsed":"%s"}`, sum, time.Now().Sub(start)), nil
}

func (c *Controller) cmdAOF(line string, w io.Writer) error {
	var spos string
	if line, spos = token(line); spos == "" {
		return errInvalidNumberOfArguments
	}
	if line != "" {
		return errInvalidNumberOfArguments
	}
	pos, err := strconv.ParseInt(spos, 10, 64)
	if err != nil || pos < 0 {
		return errInvalidArgument(spos)
	}
	f, err := os.Open(c.f.Name())
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := f.Seek(0, 2)
	if err != nil {
		return err
	}
	if n < pos {
		return errors.New("pos is too big, must be less that the aof_size of leader")
	}
	var s liveAOFSwitches
	s.pos = pos
	return s
}

func (c *Controller) liveAOF(pos int64, conn net.Conn, rd *bufio.Reader) error {
	defer conn.Close()
	if err := client.WriteMessage(conn, []byte(client.LiveJSON)); err != nil {
		return nil // nil return is fine here
	}
	c.mu.RLock()
	f, err := os.Open(c.f.Name())
	c.mu.RUnlock()
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(pos, 0); err != nil {
		return err
	}
	cond := sync.NewCond(&sync.Mutex{})
	var mustQuit bool
	go func() {
		defer func() {
			cond.L.Lock()
			mustQuit = true
			cond.Broadcast()
			cond.L.Unlock()
		}()
		for {
			command, _, err := client.ReadMessage(rd, nil)
			if err != nil {
				if err != io.EOF {
					log.Error(err)
				}
				return
			}
			cmd := string(command)
			if cmd != "" && strings.ToLower(cmd) != "quit" {
				log.Error("received a live command that was not QUIT")
				return
			}
		}
	}()
	go func() {
		defer func() {
			cond.L.Lock()
			mustQuit = true
			cond.Broadcast()
			cond.L.Unlock()
		}()
		err := func() error {
			_, err := io.Copy(conn, f)
			if err != nil {
				return err
			}
			rd := NewAOFReader(f)
			for {
				cmd, err := rd.ReadCommand()
				if err != io.EOF {
					if err != nil {
						return err
					}
					if _, err := writeCommand(conn, cmd); err != nil {
						return err
					}
					continue
				}
				c.fcond.L.Lock()
				c.fcond.Wait()
				c.fcond.L.Unlock()
			}
		}()
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") &&
				!strings.Contains(err.Error(), "bad file descriptor") {
				log.Error(err)
			}
			return
		}
	}()
	for {
		cond.L.Lock()
		if mustQuit {
			cond.L.Unlock()
			return nil
		}
		cond.Wait()
		cond.L.Unlock()
	}
}

type treeKeyBoolT struct {
	key string
}

func (k *treeKeyBoolT) Less(item btree.Item) bool {
	return k.key < item.(*treeKeyBoolT).key
}

// aofshink shinks the aof file in the background
// When completed the only command that should exist in a shrunken aof is SET.
// We will read the commands backwards from last known position of the live aof
// and use an ondisk key value store for state. For now we use BoltDB, in the future
// we should use a store that is better performant.
// The following commands may exist in the aof.
// 'SET'
//	  - Has this key been marked 'ignore'?
//      - Yes, then ignore
//      - No, Has this id been marked 'soft-ignore' or 'hard-ignore'?
//        - Yes, then ignore
//        - No
//          - Add command to key bucket
//          - Mark id as 'soft-ignore'
// 'FSET'
//	  - Has this key been marked 'ignore'?
//      - Yes, then ignore
//      - No, Has this id been marked 'hard-ignore'?
//        - Yes, then ignore
//        - No
//          - Add command to key bucket
// 'DEL'
//	  - Has this key been marked 'ignore'?
//      - Yes, then ignore
//      - No, Mark id as 'ignore'?
// 'DROP'
//    - Has this key been marked 'ignore'?
//      - Yes, then ignore
//      - No, Mark key as 'ignore'?
// 'FLUSHDB'
//    - Stop shrinking, nothing left to do

func (c *Controller) aofshrink() {
	c.mu.Lock()
	c.f.Sync()
	if c.shrinking {
		c.mu.Unlock()
		return
	}
	c.shrinking = true
	endpos := int64(c.aofsz)
	start := time.Now()
	log.Infof("aof shrink started at pos %d", endpos)
	c.mu.Unlock()
	var err error
	defer func() {
		c.mu.Lock()
		c.shrinking = false
		c.mu.Unlock()
		os.RemoveAll(c.dir + "/shrink.db")
		os.RemoveAll(c.dir + "/shrink")
		if err != nil {
			log.Error("aof shrink failed: " + err.Error())
		} else {
			log.Info("aof shrink completed: " + time.Now().Sub(start).String())
		}
	}()
	var db *bolt.DB
	db, err = bolt.Open(c.dir+"/shrink.db", 0600, nil)
	if err != nil {
		return
	}
	defer db.Close()
	var nf *os.File
	nf, err = os.Create(c.dir + "/shrink")
	if err != nil {
		return
	}
	defer nf.Close()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if err == nil {
			c.f.Sync()
			_, err = nf.Seek(0, 2)
			if err == nil {
				var f *os.File
				f, err = os.Open(c.dir + "/aof")
				if err != nil {
					return
				}
				defer f.Close()
				_, err = f.Seek(endpos, 0)
				if err == nil {
					_, err = io.Copy(nf, f)
					if err == nil {
						f.Close()
						nf.Close()
						// At this stage we need to kill all aof followers. To do so we will
						// write a KILLAOF command to the stream. KILLAOF isn't really a command.
						// This will cause the followers will close their connection and then
						// automatically reconnect. The reconnection will force a sync of the aof.
						err = c.writeAOF("KILLAOF", nil)
						if err == nil {
							c.f.Close()
							err = os.Rename(c.dir+"/shrink", c.dir+"/aof")
							if err != nil {
								log.Fatal("shink rename fatal operation")
							}
							c.f, err = os.OpenFile(c.dir+"/aof", os.O_CREATE|os.O_RDWR, 0600)
							if err != nil {
								log.Fatal("shink openfile fatal operation")
							}
							var n int64
							n, err = c.f.Seek(0, 2)
							if err != nil {
								log.Fatal("shink seek end fatal operation")
							}
							c.aofsz = int(n)
						}
					}
				}
			}
		}
	}()
	var f *os.File
	f, err = os.Open(c.dir + "/aof")
	if err != nil {
		return
	}
	defer f.Close()

	var buf []byte
	var pos int64
	pos, err = f.Seek(endpos, 0)
	if err != nil {
		return
	}
	var readPreviousCommand func() ([]byte, error)
	readPreviousCommand = func() ([]byte, error) {
		if len(buf) >= 5 {
			if buf[len(buf)-1] != 0 {
				return nil, errCorruptedAOF
			}
			sz2 := int(binary.LittleEndian.Uint32(buf[len(buf)-5:]))
			if len(buf) >= sz2+9 {
				sz1 := int(binary.LittleEndian.Uint32(buf[len(buf)-(sz2+9):]))
				if sz1 != sz2 {
					return nil, errCorruptedAOF
				}
				command := buf[len(buf)-(sz2+5) : len(buf)-5]
				buf = buf[:len(buf)-(sz2+9)]
				return command, nil
			}
		}
		if pos == 0 {
			if len(buf) > 0 {
				return nil, io.ErrUnexpectedEOF
			} else {
				return nil, io.EOF
			}
		}
		sz := int64(backwardsBufferSize)
		offset := pos - sz
		if offset < 0 {
			sz = pos
			offset = 0
		}
		pos, err = f.Seek(offset, 0)
		if err != nil {
			return nil, err
		}
		nbuf := make([]byte, int(sz))
		_, err = io.ReadFull(f, nbuf)
		if err != nil {
			return nil, err
		}
		if len(buf) > 0 {
			nbuf = append(nbuf, buf...)
		}
		buf = nbuf
		return readPreviousCommand()
	}
	var tx *bolt.Tx
	tx, err = db.Begin(true)
	if err != nil {
		return
	}
	defer func() {
		tx.Rollback()
	}()
	var keyIgnoreM = map[string]bool{}
	var keyBucketM = btree.New(16)
	var cmd, key, id, field string
	var line string
	var command []byte
	var val []byte
	var b *bolt.Bucket
reading:
	for i := 0; ; i++ {
		if i%500 == 0 {
			if err = tx.Commit(); err != nil {
				return
			}
			tx, err = db.Begin(true)
			if err != nil {
				return
			}
		}
		command, err = readPreviousCommand()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return
		}
		// quick path
		if len(command) == 0 {
			continue // ignore blank commands
		}
		line, cmd = token(string(command))
		cmd = strings.ToLower(cmd)
		switch cmd {
		case "flushdb":
			break reading // all done
		case "drop":
			if line, key = token(line); key == "" {
				err = errors.New("drop is missing key")
				return
			}
			if !keyIgnoreM[key] {
				keyIgnoreM[key] = true
			}
		case "del":
			if line, key = token(line); key == "" {
				err = errors.New("del is missing key")
				return
			}
			if keyIgnoreM[key] {
				continue // ignore
			}
			if line, id = token(line); id == "" {
				err = errors.New("del is missing id")
				return
			}
			if keyBucketM.Get(&treeKeyBoolT{key}) == nil {
				if _, err = tx.CreateBucket([]byte(key + ".ids")); err != nil {
					return
				}
				if _, err = tx.CreateBucket([]byte(key + ".ignore_ids")); err != nil {
					return
				}
				keyBucketM.ReplaceOrInsert(&treeKeyBoolT{key})
			}
			b = tx.Bucket([]byte(key + ".ignore_ids"))
			err = b.Put([]byte(id), []byte("2")) // 2 for hard ignore
			if err != nil {
				return
			}

		case "set":
			if line, key = token(line); key == "" {
				err = errors.New("SET is missing key")
				return
			}
			if keyIgnoreM[key] {
				continue // ignore
			}
			if line, id = token(line); id == "" {
				err = errors.New("SET is missing id")
				return
			}
			if keyBucketM.Get(&treeKeyBoolT{key}) == nil {
				if _, err = tx.CreateBucket([]byte(key + ".ids")); err != nil {
					return
				}
				if _, err = tx.CreateBucket([]byte(key + ".ignore_ids")); err != nil {
					return
				}
				keyBucketM.ReplaceOrInsert(&treeKeyBoolT{key})
			}
			b = tx.Bucket([]byte(key + ".ignore_ids"))
			val = b.Get([]byte(id))
			if val == nil {
				if err = b.Put([]byte(id), []byte("1")); err != nil {
					return
				}
				b = tx.Bucket([]byte(key + ".ids"))
				if err = b.Put([]byte(id), command); err != nil {
					return
				}
			} else {
				switch string(val) {
				default:
					err = errors.New("invalid ignore")
				case "1", "2":
					continue // ignore
				}
			}
		case "fset":
			if line, key = token(line); key == "" {
				err = errors.New("FSET is missing key")
				return
			}
			if keyIgnoreM[key] {
				continue // ignore
			}
			if line, id = token(line); id == "" {
				err = errors.New("FSET is missing id")
				return
			}
			if line, field = token(line); field == "" {
				err = errors.New("FSET is missing field")
				return
			}
			if keyBucketM.Get(&treeKeyBoolT{key}) == nil {
				if _, err = tx.CreateBucket([]byte(key + ".ids")); err != nil {
					return
				}
				if _, err = tx.CreateBucket([]byte(key + ".ignore_ids")); err != nil {
					return
				}
				keyBucketM.ReplaceOrInsert(&treeKeyBoolT{key})
			}
			b = tx.Bucket([]byte(key + ".ignore_ids"))
			val = b.Get([]byte(id))
			if val == nil {
				b = tx.Bucket([]byte(key + ":" + id + ":0"))
				if b == nil {
					if b, err = tx.CreateBucket([]byte(key + ":" + id + ":0")); err != nil {
						return
					}
				}
				if b.Get([]byte(field)) == nil {
					if err = b.Put([]byte(field), command); err != nil {
						return
					}
				}
			} else {
				switch string(val) {
				default:
					err = errors.New("invalid ignore")
				case "1":
					b = tx.Bucket([]byte(key + ":" + id + ":1"))
					if b == nil {
						if b, err = tx.CreateBucket([]byte(key + ":" + id + ":1")); err != nil {
							return
						}
					}
					if b.Get([]byte(field)) == nil {
						if err = b.Put([]byte(field), command); err != nil {
							return
						}
					}
				case "2":
					continue // ignore
				}
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return
	}
	tx, err = db.Begin(false)
	if err != nil {
		return
	}
	keyBucketM.Ascend(func(item btree.Item) bool {
		key := item.(*treeKeyBoolT).key
		b := tx.Bucket([]byte(key + ".ids"))
		if b != nil {
			err = b.ForEach(func(id, command []byte) error {
				// parse the SET command
				_, fields, values, etype, eargs, err := c.parseSetArgs(string(command[4:]))
				if err != nil {
					return err
				}
				// store the fields in a map
				var fieldm = map[string]float64{}
				for i, field := range fields {
					fieldm[field] = values[i]
				}
				// append old FSET values. these are FSETs that existed prior to the last SET.
				f1 := tx.Bucket([]byte(key + ":" + string(id) + ":1"))
				if f1 != nil {
					err = f1.ForEach(func(field, command []byte) error {
						d, err := c.parseFSetArgs(string(command[5:]))
						if err != nil {
							return err
						}
						if _, ok := fieldm[d.field]; !ok {
							fieldm[d.field] = d.value
						}
						return nil
					})
					if err != nil {
						return err
					}
				}
				// append new FSET values. these are FSETs that were added after the last SET.
				f0 := tx.Bucket([]byte(key + ":" + string(id) + ":0"))
				if f0 != nil {
					f0.ForEach(func(field, command []byte) error {
						d, err := c.parseFSetArgs(string(command[5:]))
						if err != nil {
							return err
						}
						fieldm[d.field] = d.value
						return nil
					})
				}
				// rebuild the SET command
				ncommand := "set " + key + " " + string(id)
				for field, value := range fieldm {
					if value != 0 {
						ncommand += " field " + field + " " + strconv.FormatFloat(value, 'f', -1, 64)
					}
				}
				ncommand += " " + strings.ToUpper(etype) + " " + eargs
				_, err = writeCommand(nf, []byte(ncommand))
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return false
			}
		}
		return true
	})
}
