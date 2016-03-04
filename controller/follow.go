package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/tile38/client"
	"github.com/tidwall/tile38/log"
)

var errNoLongerFollowing = errors.New("no longer following")

const checksumsz = 512 * 1024

func (c *Controller) cmdFollow(line string) error {
	var host, sport string
	if line, host = token(line); host == "" {
		return errInvalidNumberOfArguments
	}
	if line, sport = token(line); sport == "" {
		return errInvalidNumberOfArguments
	}
	if line != "" {
		return errInvalidNumberOfArguments
	}
	host = strings.ToLower(host)
	sport = strings.ToLower(sport)
	var update bool
	pconfig := c.config
	if host == "no" && sport == "one" {
		update = c.config.FollowHost != "" || c.config.FollowPort != 0
		c.config.FollowHost = ""
		c.config.FollowPort = 0
	} else {
		n, err := strconv.ParseUint(sport, 10, 64)
		if err != nil {
			return errInvalidArgument(sport)
		}
		port := int(n)
		update = c.config.FollowHost != host || c.config.FollowPort != port
		if update {
			c.mu.Unlock()
			conn, err := client.DialTimeout(fmt.Sprintf("%s:%d", host, port), time.Second*2)
			if err != nil {
				c.mu.Lock()
				return fmt.Errorf("cannot follow: %v", err)
			}
			defer conn.Close()
			msg, err := conn.Stats()
			if err != nil {
				c.mu.Lock()
				return fmt.Errorf("cannot follow: %v", err)
			}
			if msg.Stats.ServerID == c.config.ServerID {
				c.mu.Lock()
				return fmt.Errorf("cannot follow self")
			}
			if msg.Stats.Following != "" {
				c.mu.Lock()
				return fmt.Errorf("cannot follow a follower")
			}
			c.mu.Lock()
		}
		c.config.FollowHost = host
		c.config.FollowPort = port
	}
	if err := c.writeConfig(); err != nil {
		c.config = pconfig // revert
		return err
	}
	if update {
		c.followc++
		if c.config.FollowHost != "" {
			log.Infof("following new host '%s' '%s'.", host, sport)
			go c.follow(c.config.FollowHost, c.config.FollowPort, c.followc)
		} else {
			log.Infof("following no one")
		}
	}
	return nil
}

func (c *Controller) followHandleCommand(line string, followc uint64, w io.Writer) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.followc != followc {
		return c.aofsz, errNoLongerFollowing
	}
	_, d, err := c.command(line, w)
	if err != nil {
		return c.aofsz, err
	}
	if err := c.writeAOF(line, &d); err != nil {
		return c.aofsz, err
	}
	return c.aofsz, nil
}

func (c *Controller) followStep(host string, port int, followc uint64) error {
	c.mu.Lock()
	if c.followc != followc {
		c.mu.Unlock()
		return errNoLongerFollowing
	}
	c.fcup = false
	c.mu.Unlock()
	addr := fmt.Sprintf("%s:%d", host, port)
	// check if we are following self
	conn, err := client.DialTimeout(addr, time.Second*2)
	if err != nil {
		return fmt.Errorf("cannot follow: %v", err)
	}
	defer conn.Close()
	stats, err := conn.Stats()
	if err != nil {
		return fmt.Errorf("cannot follow: %v", err)
	}
	if stats.Stats.ServerID == c.config.ServerID {
		return fmt.Errorf("cannot follow self")
	}
	if stats.Stats.Following != "" {
		return fmt.Errorf("cannot follow a follower")
	}
	// verify checksum
	pos, err := c.followCheckSome(addr, followc)
	if err != nil {
		return err
	}

	// make real connection
	conn, err = client.Dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg, err := conn.Do(fmt.Sprintf("aof %d", pos))
	if err != nil {
		return err
	}

	if string(msg) != client.LiveJSON {
		var m map[string]interface{}
		err := json.Unmarshal(msg, &m)
		if err != nil {
			return err
		}
		if errs, ok := m["err"].(string); ok && errs != "" {
			return errors.New(errs)
		}
		return errors.New("invalid response to aof live request")
	}
	if ShowDebugMessages {
		log.Debug("follow:", addr, ":read aof")
	}
	caughtUp := pos >= int64(stats.Stats.AOFSize)
	if caughtUp {
		c.mu.Lock()
		c.fcup = true
		c.mu.Unlock()
		log.Info("caught up")
	}
	nullw := ioutil.Discard
	rd := NewAOFReader(conn.Reader())
	for {
		buf, err := rd.ReadCommand()
		if err != nil {
			return err
		}
		aofsz, err := c.followHandleCommand(string(buf), followc, nullw)
		if err != nil {
			return err
		}
		if !caughtUp {
			if aofsz >= stats.Stats.AOFSize {
				caughtUp = true
				c.mu.Lock()
				c.fcup = true
				c.mu.Unlock()
				log.Info("caught up")
			}
		}

	}
}

func (c *Controller) follow(host string, port int, followc uint64) {
	for {
		err := c.followStep(host, port, followc)
		if err == errNoLongerFollowing {
			return
		}
		if err != nil && err != io.EOF {
			log.Debug("follow: " + err.Error())
		}
		time.Sleep(time.Second)
	}
}
