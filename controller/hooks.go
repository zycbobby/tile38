package controller

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/tile38/controller/log"
)

type EndpointProtocol string

const (
	HTTP   = EndpointProtocol("http")
	Disque = EndpointProtocol("disque")
)

type Endpoint struct {
	Protocol EndpointProtocol
	Original string
}
type Hook struct {
	Key        string
	Name       string
	Endpoint   Endpoint
	Command    string
	Fence      *liveFenceSwitches
	ScanWriter *scanWriter
}

func (c *Controller) DoHook(hook *Hook, details *commandDetailsT) error {
	msgs := c.FenceMatch(hook.ScanWriter, hook.Fence, details, false)
	for _, msg := range msgs {
		println(">>", string(msg))
	}
	return nil
}

type hooksByName []*Hook

func (a hooksByName) Len() int {
	return len(a)
}

func (a hooksByName) Less(i, j int) bool {
	return a[i].Name < a[j].Name
}

func (a hooksByName) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func parseEndpoint(s string) (Endpoint, error) {
	var endpoint Endpoint
	endpoint.Original = s
	switch {
	default:
		return endpoint, errors.New("unknown scheme")
	case strings.HasPrefix(s, "http:"):
		endpoint.Protocol = HTTP
	case strings.HasPrefix(s, "https:"):
		endpoint.Protocol = HTTP
	case strings.HasPrefix(s, "disque:"):
		endpoint.Protocol = Disque
	}
	s = s[strings.Index(s, ":")+1:]
	if !strings.HasPrefix(s, "//") {
		return endpoint, errors.New("missing the two slashes")
	}
	s = strings.Split(s[2:], "/")[0]
	if s == "" {
		return endpoint, errors.New("missing host")
	}
	return endpoint, nil
}

func (c *Controller) cmdAddHook(line string) (err error) {
	//start := time.Now()
	var name, value, cmd string
	if line, name = token(line); name == "" {
		return errInvalidNumberOfArguments
	}
	if line, value = token(line); value == "" {
		return errInvalidNumberOfArguments
	}
	endpoint, err := parseEndpoint(value)
	if err != nil {
		log.Errorf("addhook: %v", err)
		return errInvalidArgument(value)
	}
	command := line
	if line, cmd = token(line); cmd == "" {
		return errInvalidNumberOfArguments
	}
	cmdlc := strings.ToLower(cmd)
	var types []string
	switch cmdlc {
	default:
		return errInvalidArgument(cmd)
	case "nearby":
		types = nearbyTypes
	case "within", "intersects":
		types = withinOrIntersectsTypes
	}
	s, err := c.cmdSearchArgs(cmdlc, line, types)
	if err != nil {
		return err
	}
	if !s.fence {
		return errors.New("missing FENCE argument")
	}
	s.cmd = cmdlc
	hook := &Hook{
		Key:      s.key,
		Name:     name,
		Endpoint: endpoint,
		Fence:    &s,
		Command:  command,
	}
	var wr bytes.Buffer
	hook.ScanWriter, err = c.newScanWriter(&wr, s.key, s.output, s.precision, s.glob, s.limit, s.wheres, s.nofields)
	if err != nil {
		return err
	}

	// delete the previous hook
	if h, ok := c.hooks[name]; ok {
		if hm, ok := c.hookcols[h.Key]; ok {
			delete(hm, h.Name)
		}
		delete(c.hooks, h.Name)
	}
	c.hooks[name] = hook
	hm, ok := c.hookcols[hook.Key]
	if !ok {
		hm = make(map[string]*Hook)
		c.hookcols[hook.Key] = hm
	}
	hm[name] = hook
	return nil
}

func (c *Controller) cmdDelHook(line string) (err error) {
	var name string
	if line, name = token(line); name == "" {
		return errInvalidNumberOfArguments
	}
	if line != "" {
		return errInvalidNumberOfArguments
	}
	if h, ok := c.hooks[name]; ok {
		if hm, ok := c.hookcols[h.Key]; ok {
			delete(hm, h.Name)
		}
		delete(c.hooks, h.Name)
	}
	return
}

func (c *Controller) cmdHooks(line string, w io.Writer) (err error) {
	start := time.Now()

	var pattern string
	if line, pattern = token(line); pattern == "" {
		return errInvalidNumberOfArguments
	}
	if line != "" {
		return errInvalidNumberOfArguments
	}

	var hooks []*Hook
	for name, hook := range c.hooks {
		if ok, err := globMatch(pattern, name); err == nil && ok {
			hooks = append(hooks, hook)
		} else if err != nil {
			return errInvalidArgument(pattern)
		}
	}
	sort.Sort(hooksByName(hooks))

	buf := &bytes.Buffer{}
	io.WriteString(buf, `{"ok":true,"hooks":[`)
	for i, hook := range hooks {
		if i > 0 {
			io.WriteString(buf, `,`)
		}
		io.WriteString(buf, `"hook":{`)
		io.WriteString(buf, `"name":`+jsonString(hook.Name))
		io.WriteString(buf, `,"key":`+jsonString(hook.Key))
		io.WriteString(buf, `,"endpoint":`+jsonString(hook.Endpoint.Original))
		io.WriteString(buf, `,"command":`+jsonString(hook.Command))
		io.WriteString(buf, `}`)
	}
	io.WriteString(buf, `],"elapsed":"`+time.Now().Sub(start).String()+"\"}")

	w.Write(buf.Bytes())
	return
}
