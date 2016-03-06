package controller

import (
	"strings"

	"github.com/tidwall/tile38/controller/log"
)

func (c *Controller) cmdReadOnly(line string) error {
	var arg string
	if line, arg = token(line); arg == "" {
		return errInvalidNumberOfArguments
	}
	if line != "" {
		return errInvalidNumberOfArguments
	}
	backup := c.config
	switch strings.ToLower(arg) {
	default:
		return errInvalidArgument(arg)
	case "yes":
		if c.config.ReadOnly {
			return nil
		}
		c.config.ReadOnly = true
		log.Info("read only")
	case "no":
		if !c.config.ReadOnly {
			return nil
		}
		c.config.ReadOnly = false
		log.Info("read write")
	}
	err := c.writeConfig()
	if err != nil {
		c.config = backup
		return err
	}
	return nil
}
