package controller

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
)

var validProperties = []string{"requirepass", "leaderauth", "protected-mode", "maxmemory"}

// Config is a tile38 config
type Config struct {
	FollowHost string `json:"follow_host,omitempty"`
	FollowPort int    `json:"follow_port,omitempty"`
	FollowID   string `json:"follow_id,omitempty"`
	FollowPos  int    `json:"follow_pos,omitempty"`
	ServerID   string `json:"server_id,omitempty"`
	ReadOnly   bool   `json:"read_only,omitempty"`

	// Properties
	RequirePassP   string `json:"requirepass,omitempty"`
	RequirePass    string `json:"-"`
	LeaderAuthP    string `json:"leaderauth,omitempty"`
	LeaderAuth     string `json:"-"`
	ProtectedModeP string `json:"protected-mode,omitempty"`
	ProtectedMode  string `json:"-"`
	MaxMemoryP     string `json:"maxmemory,omitempty"`
	MaxMemory      int    `json:"-"`
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
	// load properties
	if err := c.setConfigProperty("requirepass", c.config.RequirePassP, true); err != nil {
		return err
	}
	if err := c.setConfigProperty("leaderauth", c.config.LeaderAuthP, true); err != nil {
		return err
	}
	if err := c.setConfigProperty("protected-mode", c.config.ProtectedModeP, true); err != nil {
		return err
	}
	if err := c.setConfigProperty("maxmemory", c.config.MaxMemoryP, true); err != nil {
		return err
	}
	return nil
}

func parseMemSize(s string) (bytes int, ok bool) {
	if s == "" {
		return 0, true
	}
	s = strings.ToLower(s)
	var n uint64
	var sz int
	var err error
	if strings.HasSuffix(s, "gb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int(n * 1024 * 1024 * 1024)
	} else if strings.HasSuffix(s, "mb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int(n * 1024 * 1024)
	} else if strings.HasSuffix(s, "kb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int(n * 1024)
	} else {
		n, err = strconv.ParseUint(s, 10, 64)
		sz = int(n)
	}
	if err != nil {
		return 0, false
	}
	return sz, true
}

func formatMemSize(sz int) string {
	if sz <= 0 {
		return ""
	}
	if sz < 1024 {
		return strconv.FormatInt(int64(sz), 10)
	}
	sz /= 1024
	if sz < 1024 {
		return strconv.FormatInt(int64(sz), 10) + "kb"
	}
	sz /= 1024
	if sz < 1024 {
		return strconv.FormatInt(int64(sz), 10) + "mb"
	}
	sz /= 1024
	return strconv.FormatInt(int64(sz), 10) + "gb"
}

func (c *Controller) setConfigProperty(name, value string, fromLoad bool) error {
	var invalid bool
	switch name {
	default:
		return fmt.Errorf("Unsupported CONFIG parameter: %s", name)
	case "requirepass":
		c.config.RequirePass = value
	case "leaderauth":
		c.config.LeaderAuth = value
	case "maxmemory":
		sz, ok := parseMemSize(value)
		if !ok {
			return fmt.Errorf("Invalid argument '%s' for CONFIG SET '%s'", value, name)
		}
		c.config.MaxMemory = sz
	case "protected-mode":
		switch strings.ToLower(value) {
		case "":
			if fromLoad {
				c.config.ProtectedMode = "yes"
			} else {
				invalid = true
			}
		case "yes", "no":
			c.config.ProtectedMode = strings.ToLower(value)
		default:
			invalid = true
		}
	}
	if invalid {
		return fmt.Errorf("Invalid argument '%s' for CONFIG SET '%s'", value, name)
	}
	return nil
}

func (c *Controller) getConfigProperties(pattern string) map[string]interface{} {
	m := make(map[string]interface{})
	for _, name := range validProperties {
		matched, _ := glob.Match(pattern, name)
		if matched {
			m[name] = c.getConfigProperty(name)
		}
	}
	return m
}
func (c *Controller) getConfigProperty(name string) string {
	switch name {
	default:
		return ""
	case "requirepass":
		return c.config.RequirePass
	case "leaderauth":
		return c.config.LeaderAuth
	case "protected-mode":
		return c.config.ProtectedMode
	case "maxmemory":
		return formatMemSize(c.config.MaxMemory)
	}
}

func (c *Controller) initConfig() error {
	c.config = Config{ServerID: randomKey(16)}
	return c.writeConfig(true)
}

func (c *Controller) writeConfig(writeProperties bool) error {
	var err error
	bak := c.config
	defer func() {
		if err != nil {
			// revert changes
			c.config = bak
		}
	}()
	if writeProperties {
		// save properties
		c.config.RequirePassP = c.config.RequirePass
		c.config.LeaderAuthP = c.config.LeaderAuth
		c.config.ProtectedModeP = c.config.ProtectedMode
		c.config.MaxMemoryP = formatMemSize(c.config.MaxMemory)
	}
	var data []byte
	data, err = json.MarshalIndent(c.config, "", "\t")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(c.dir+"/config", data, 0600)
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) cmdConfigGet(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	var name string
	if vs, name, ok = tokenval(vs); !ok {
		return "", errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return "", errInvalidNumberOfArguments
	}
	m := c.getConfigProperties(name)
	switch msg.OutputType {
	case server.JSON:
		data, err := json.Marshal(m)
		if err != nil {
			return "", err
		}
		res = `{"ok":true,"properties":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}"
	case server.RESP:
		vals := respValuesSimpleMap(m)
		data, err := resp.ArrayValue(vals).MarshalRESP()
		if err != nil {
			return "", err
		}
		res = string(data)
	}
	return
}
func (c *Controller) cmdConfigSet(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	var name string
	if vs, name, ok = tokenval(vs); !ok {
		return "", errInvalidNumberOfArguments
	}
	var value string
	if vs, value, ok = tokenval(vs); !ok {
		if strings.ToLower(name) != "requirepass" {
			return "", errInvalidNumberOfArguments
		}
	}
	if len(vs) != 0 {
		return "", errInvalidNumberOfArguments
	}
	if err := c.setConfigProperty(name, value, false); err != nil {
		return "", err
	}
	return server.OKMessage(msg, start), nil
}
func (c *Controller) cmdConfigRewrite(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	if len(vs) != 0 {
		return "", errInvalidNumberOfArguments
	}
	if err := c.writeConfig(true); err != nil {
		return "", err
	}
	return server.OKMessage(msg, start), nil
}
