package controller

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tidwall/tile38/client"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
)

const backwardsBufferSize = 50000

// checksum performs a simple md5 checksum on the aof file
func (c *Controller) checksum(pos, size int64) (sum string, err error) {
	if pos+size > int64(c.aofsz) {
		return "", io.EOF
	}
	var f *os.File
	f, err = os.Open(c.f.Name())
	if err != nil {
		return
	}
	defer f.Close()
	data := make([]byte, size)
	err = func() error {
		if size == 0 {
			n, err := f.Seek(int64(c.aofsz), 0)
			if err != nil {
				return err
			}
			if pos >= n {
				return io.EOF
			}
			return nil
		}
		_, err = f.Seek(pos, 0)
		if err != nil {
			return err
		}
		_, err = io.ReadFull(f, data)
		if err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		return "", err
	}
	return fmt.Sprintf("%x", md5.Sum(data)), nil
}

func connAOFMD5(conn *client.Conn, pos, size int64) (sum string, err error) {
	type md5resT struct {
		OK  bool   `json:"ok"`
		MD5 string `json:"md5"`
		Err string `json:"err"`
	}
	msg, err := conn.Do(fmt.Sprintf("aofmd5 %d %d", pos, size))
	if err != nil {
		return "", err
	}
	var res md5resT
	if err := json.Unmarshal(msg, &res); err != nil {
		return "", err
	}
	if !res.OK || len(res.MD5) != 32 {
		if res.Err != "" {
			if res.Err == "EOF" {
				return "", io.EOF
			}
			return "", errors.New(res.Err)
		}
		return "", errors.New("checksum not ok")
	}
	sum = res.MD5
	return
}

func (c *Controller) matchChecksums(conn *client.Conn, pos, size int64) (match bool, err error) {
	sum, err := c.checksum(pos, size)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	csum, err := connAOFMD5(conn, pos, size)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	return csum == sum, nil
}

// followCheckSome is not a full checksum. It just "checks some" data.
// We will do some various checksums on the leader until we find the correct position to start at.
func (c *Controller) followCheckSome(addr string, followc uint64) (pos int64, err error) {
	if core.ShowDebugMessages {
		log.Debug("follow:", addr, ":check some")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.followc != followc {
		return 0, errNoLongerFollowing
	}
	if c.aofsz < checksumsz {
		return 0, nil
	}
	conn, err := client.Dial(addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	min := int64(0)
	max := int64(c.aofsz) - checksumsz
	limit := int64(c.aofsz)
	match, err := c.matchChecksums(conn, min, checksumsz)
	if err != nil {
		return 0, err
	}
	if match {
		min += checksumsz // bump up the min
		for {
			if max < min || max+checksumsz > limit {
				pos = min
				break
			} else {
				match, err = c.matchChecksums(conn, max, checksumsz)
				if err != nil {
					return 0, err
				}
				if match {
					min = max + checksumsz
				} else {
					limit = max
				}
				max = (limit-min)/2 - checksumsz/2 + min // multiply
			}
		}
	}
	fullpos := pos
	fname := c.f.Name()
	if pos == 0 {
		c.f.Close()
		c.f, err = os.Create(fname)
		if err != nil {
			log.Fatalf("could not recreate aof, possible data loss. %s", err.Error())
			return 0, err
		}
		return 0, nil
	}

	// we want to truncate at a command location
	// search for nearest command
	f, err := os.Open(c.f.Name())
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err := f.Seek(pos, 0); err != nil {
		return 0, err
	}
	// need to read backwards looking for null byte
	const bufsz = backwardsBufferSize
	buf := make([]byte, bufsz)
outer:
	for {
		if pos < int64(len(buf)) {
			pos = 0
			break
		}
		if _, err := f.Seek(pos-bufsz, 0); err != nil {
			return 0, err
		}
		if _, err := io.ReadFull(f, buf); err != nil {
			return 0, err
		}
		for i := len(buf) - 1; i >= 0; i-- {
			if buf[i] == 0 {
				tpos := pos - bufsz + int64(i) - 4
				if tpos < 0 {
					pos = 0
					break outer // at beginning of file
				}
				if _, err := f.Seek(tpos, 0); err != nil {
					return 0, err
				}
				szb := make([]byte, 4)
				if _, err := io.ReadFull(f, szb); err != nil {
					return 0, err
				}
				sz2 := int64(binary.LittleEndian.Uint32(szb))
				tpos = tpos - sz2 - 4
				if tpos < 0 {
					continue // keep scanning
				}
				if _, err := f.Seek(tpos, 0); err != nil {
					return 0, err
				}
				if _, err := io.ReadFull(f, szb); err != nil {
					return 0, err
				}
				sz1 := int64(binary.LittleEndian.Uint32(szb))
				if sz1 == sz2 {
					pos = pos - bufsz + int64(i) + 1
					break outer // we found our match
				}
			}
		}
		pos -= bufsz
	}
	if pos == fullpos {
		if core.ShowDebugMessages {
			log.Debug("follow: aof fully intact")
		}
		return pos, nil
	}
	log.Warnf("truncating aof to %d", pos)
	// any errror below are fatal.
	f.Close()
	c.f.Close()
	if err := os.Truncate(fname, pos); err != nil {
		log.Fatalf("could not truncate aof, possible data loss. %s", err.Error())
		return 0, err
	}
	c.f, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatalf("could not create aof, possible data loss. %s", err.Error())
		return 0, err
	}
	// reset the entire system.
	log.Infof("reloading aof commands")
	c.reset()
	if err := c.loadAOF(); err != nil {
		log.Fatalf("could not reload aof, possible data loss. %s", err.Error())
		return 0, err
	}
	if int64(c.aofsz) != pos {
		log.Fatalf("aof size mismatch during reload, possible data loss.")
		return 0, errors.New("?")
	}
	return pos, nil
}
