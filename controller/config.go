package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

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
	return nil
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

func (c *Controller) cmdConfig(line string) (string, error) {
	var start = time.Now()
	var cmd, name, value string
	if line, cmd = token(line); cmd == "" {
		return "", errInvalidNumberOfArguments
	}
	var buf bytes.Buffer
	buf.WriteString(`{"ok":true`)
	switch strings.ToLower(cmd) {
	default:
		return "", errInvalidArgument(cmd)
	case "get":
		if line, name = token(line); name == "" || line != "" {
			return "", errInvalidNumberOfArguments
		}
		value = c.getConfigProperty(name)
		buf.WriteString(`,"value":` + jsonString(value))
	case "set":
		if line, name = token(line); name == "" {
			return "", errInvalidNumberOfArguments
		}
		value = strings.TrimSpace(line)
		if err := c.setConfigProperty(name, value, false); err != nil {
			return "", err
		}
	case "rewrite":
		if err := c.writeConfig(true); err != nil {
			return "", err
		}
	}
	buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	return buf.String(), nil
}
