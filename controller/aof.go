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

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/client"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
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

type errAOFHook struct {
	err error
}

func (err errAOFHook) Error() string {
	return fmt.Sprintf("hook: %v", err.err)
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
	rd := resp.NewReader(c.f)
	for {
		v, _, n, err := rd.ReadMultiBulk()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		values := v.Array()
		if len(values) == 0 {
			return errors.New("multibulk missing command component")
		}
		msg := &server.Message{
			Command: strings.ToLower(values[0].String()),
			Values:  values,
		}
		if _, _, err := c.command(msg, nil); err != nil {
			if commandErrIsFatal(err) {
				return err
			}
		}
		c.aofsz += n
		count++
	}
}

func commandErrIsFatal(err error) bool {
	// FSET (and other writable commands) may return errors that we need
	// to ignore during the loading process. These errors may occur (though unlikely)
	// due to the aof rewrite operation.
	switch err {
	case errKeyNotFound, errIDNotFound:
		return false
	}
	return true
}

func (c *Controller) writeAOF(value resp.Value, d *commandDetailsT) error {
	if d != nil {
		if !d.updated {
			return nil // just ignore writes if the command did not update
		}
		// process hooks
		if hm, ok := c.hookcols[d.key]; ok {
			for _, hook := range hm {
				if err := c.DoHook(hook, d); err != nil {
					if d.revert != nil {
						d.revert()
					}
					return errAOFHook{err}
				}
			}
		}
	}
	data, err := value.MarshalRESP()
	if err != nil {
		return err
	}
	n, err := c.f.Write(data)
	if err != nil {
		return err
	}
	c.aofsz += n

	// notify aof live connections that we have new data
	c.fcond.L.Lock()
	c.fcond.Broadcast()
	c.fcond.L.Unlock()

	if d != nil {
		// write to live connection streams
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
			command, _, _, err := client.ReadMessage(rd, nil)
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
			rd := resp.NewReader(f)
			for {
				v, _, err := rd.ReadValue()
				if err != io.EOF {
					if err != nil {
						return err
					}
					data, err := v.MarshalRESP()
					if err != nil {
						return err
					}
					if _, err := conn.Write(data); err != nil {
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
