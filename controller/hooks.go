package controller

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
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
	Disque   struct {
		Host      string
		Port      int
		QueueName string
		Options   struct {
			Replicate int
		}
	}
}

type Hook struct {
	Key        string
	Name       string
	Endpoints  []Endpoint
	Command    string
	Fence      *liveFenceSwitches
	ScanWriter *scanWriter
}

func (c *Controller) DoHook(hook *Hook, details *commandDetailsT) error {
	msgs := c.FenceMatch(hook.Name, hook.ScanWriter, hook.Fence, details, false)
	for _, msg := range msgs {
		for _, endpoint := range hook.Endpoints {
			switch endpoint.Protocol {
			case HTTP:
				if err := c.sendHTTPMessage(endpoint, msg); err != nil {
					return err
				}
				return nil //sent
			case Disque:
				if err := c.sendDisqueMessage(endpoint, msg); err != nil {
					return err
				}
				return nil // sent
			}
		}
	}
	return errors.New("not sent")
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
	sqp := strings.Split(s[2:], "?")
	sp := strings.Split(sqp[0], "/")
	s = sp[0]
	if s == "" {
		return endpoint, errors.New("missing host")
	}
	if endpoint.Protocol == Disque {

		dp := strings.Split(s, ":")
		switch len(dp) {
		default:
			return endpoint, errors.New("invalid disque url")
		case 1:
			endpoint.Disque.Host = dp[0]
			endpoint.Disque.Port = 7711
		case 2:
			endpoint.Disque.Host = dp[0]
			n, err := strconv.ParseUint(dp[1], 10, 16)
			if err != nil {
				return endpoint, errors.New("invalid disque url")
			}
			endpoint.Disque.Port = int(n)
		}
		if len(sp) > 1 {
			var err error
			endpoint.Disque.QueueName, err = url.QueryUnescape(sp[1])
			if err != nil {
				return endpoint, errors.New("invalid disque queue name")
			}
		}
		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid disque url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "replicate":
					n, err := strconv.ParseUint(val[0], 10, 8)
					if err != nil {
						return endpoint, errors.New("invalid disque replicate value")
					}
					endpoint.Disque.Options.Replicate = int(n)
				}
			}
		}
		if endpoint.Disque.QueueName == "" {
			return endpoint, errors.New("missing disque queue name")
		}

	}
	return endpoint, nil
}

func (c *Controller) cmdSetHook(line string) (err error) {
	//start := time.Now()
	var name, values, cmd string
	if line, name = token(line); name == "" {
		return errInvalidNumberOfArguments
	}
	if line, values = token(line); values == "" {
		return errInvalidNumberOfArguments
	}
	var endpoints []Endpoint
	for _, value := range strings.Split(values, ",") {
		endpoint, err := parseEndpoint(value)
		if err != nil {
			log.Errorf("sethook: %v", err)
			return errInvalidArgument(value)
		}
		endpoints = append(endpoints, endpoint)
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
		Key:       s.key,
		Name:      name,
		Endpoints: endpoints,
		Fence:     &s,
		Command:   command,
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
	buf.WriteString(`{"ok":true,"hooks":[`)
	for i, hook := range hooks {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(`{`)
		buf.WriteString(`"name":` + jsonString(hook.Name))
		buf.WriteString(`,"key":` + jsonString(hook.Key))
		buf.WriteString(`,"endpoints":[`)
		for i, endpoint := range hook.Endpoints {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(jsonString(endpoint.Original))
		}
		buf.WriteString(`],"command":` + jsonString(hook.Command))
		buf.WriteString(`}`)
	}
	buf.WriteString(`],"elapsed":"` + time.Now().Sub(start).String() + "\"}")

	w.Write(buf.Bytes())
	return
}

func (c *Controller) sendHTTPMessage(endpoint Endpoint, msg []byte) error {
	resp, err := http.Post(endpoint.Original, "application/json", bytes.NewBuffer(msg))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("enpoint returned status code %d", resp.StatusCode)
	}
	return nil
}

func (c *Controller) sendDisqueMessage(endpoint Endpoint, msg []byte) error {
	addr := fmt.Sprintf("%s:%d", endpoint.Disque.Host, endpoint.Disque.Port)
	conn, err := redis.DialTimeout("tcp", addr, time.Second/4, time.Second/4, time.Second/4)
	if err != nil {
		return err
	}
	defer conn.Close()
	options := []interface{}{endpoint.Disque.QueueName, msg, 0}
	replicate := endpoint.Disque.Options.Replicate
	if replicate > 0 {
		options = append(options, "REPLICATE")
		options = append(options, endpoint.Disque.Options.Replicate)
	}
	id, err := redis.String(conn.Do("ADDJOB", options...))
	if err != nil {
		return err
	}
	p := strings.Split(id, "-")
	if len(p) != 4 {
		return errors.New("invalid disque reply")
	}
	return nil
}
