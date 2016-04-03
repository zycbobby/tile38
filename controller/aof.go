package controller

import (
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
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

// AsyncHooks indicates that the hooks should happen in the background.
const AsyncHooks = true

type errAOFHook struct {
	err error
}

func (err errAOFHook) Error() string {
	return fmt.Sprintf("hook: %v", err.err)
}

func (c *Controller) loadAOF() error {
	fi, err := c.f.Stat()
	if err != nil {
		return err
	}
	start := time.Now()
	var count int
	defer func() {
		d := time.Now().Sub(start)
		ps := float64(count) / (float64(d) / float64(time.Second))
		suf := []string{"bytes/s", "KB/s", "MB/s", "GB/s", "TB/s"}
		bps := float64(fi.Size()) / (float64(d) / float64(time.Second))
		for i := 0; bps > 1024; i++ {
			if len(suf) == 1 {
				break
			}
			bps /= 1024
			suf = suf[1:]
		}
		byteSpeed := fmt.Sprintf("%.0f %s", bps, suf[0])
		log.Infof("AOF loaded %d commands: %.2fs, %.0f/s, %s",
			count, float64(d)/float64(time.Second), ps, byteSpeed)
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
		if c.config.FollowHost == "" {
			// process hooks, for leader only
			if err := c.processHooks(d); err != nil {
				return err
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

func (c *Controller) processHooks(d *commandDetailsT) error {
	if hm, ok := c.hookcols[d.key]; ok {
		for _, hook := range hm {
			if AsyncHooks {
				go hook.Do(d)
			} else {
				if err := hook.Do(d); err != nil {
					if d.revert != nil {
						d.revert()
					}
					return errAOFHook{err}
				}
			}
		}
	}
	return nil
}

type liveAOFSwitches struct {
	pos int64
}

func (s liveAOFSwitches) Error() string {
	return "going live"
}

func (c *Controller) cmdAOFMD5(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	var spos, ssize string
	if vs, spos, ok = tokenval(vs); !ok || spos == "" {
		return "", errInvalidNumberOfArguments
	}
	if vs, ssize, ok = tokenval(vs); !ok || ssize == "" {
		return "", errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
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
	switch msg.OutputType {
	case server.JSON:
		res = fmt.Sprintf(`{"ok":true,"md5":"%s","elapsed":"%s"}`, sum, time.Now().Sub(start))
	case server.RESP:
		data, err := resp.SimpleStringValue(sum).MarshalRESP()
		if err != nil {
			return "", err
		}
		res = string(data)
	}
	return res, nil
}

func (c *Controller) cmdAOF(msg *server.Message) (res string, err error) {
	vs := msg.Values[1:]
	var ok bool
	var spos string
	if vs, spos, ok = tokenval(vs); !ok || spos == "" {
		return "", errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return "", errInvalidNumberOfArguments
	}
	pos, err := strconv.ParseInt(spos, 10, 64)
	if err != nil || pos < 0 {
		return "", errInvalidArgument(spos)
	}
	f, err := os.Open(c.f.Name())
	if err != nil {
		return "", err
	}
	defer f.Close()
	n, err := f.Seek(0, 2)
	if err != nil {
		return "", err
	}
	if n < pos {
		return "", errors.New("pos is too big, must be less that the aof_size of leader")
	}
	var s liveAOFSwitches
	s.pos = pos
	return "", s
}

func (c *Controller) liveAOF(pos int64, conn net.Conn, rd *server.AnyReaderWriter, msg *server.Message) error {
	c.mu.Lock()
	c.aofconnM[conn] = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.aofconnM, conn)
		c.mu.Unlock()
		conn.Close()
	}()

	if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
		return err
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

			b := make([]byte, 4096)
			// The reader needs to be OK with the eof not
			for {
				n, err := f.Read(b)
				if err != io.EOF && n > 0 {
					if err != nil {
						return err
					}
					if _, err := conn.Write(b[:n]); err != nil {
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
