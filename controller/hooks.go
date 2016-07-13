package controller

import (
	"bytes"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

// EndpointProtocol is the type of protocol that the endpoint represents.
type EndpointProtocol string

const (
	HTTP   = EndpointProtocol("http")   // HTTP
	Disque = EndpointProtocol("disque") // Disque
)

// Endpoint represents an endpoint.
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

// Hook represents a hook.
type Hook struct {
	Key        string
	Name       string
	Endpoints  []Endpoint
	Message    *server.Message
	Fence      *liveFenceSwitches
	ScanWriter *scanWriter
}

// Do performs a hook.
func (hook *Hook) Do(details *commandDetailsT) error {
	var lerrs []error
	msgs := FenceMatch(hook.Name, hook.ScanWriter, hook.Fence, details)
nextMessage:
	for _, msg := range msgs {
	nextEndpoint:
		for _, endpoint := range hook.Endpoints {
			switch endpoint.Protocol {
			case HTTP:
				if err := sendHTTPMessage(endpoint, []byte(msg)); err != nil {
					lerrs = append(lerrs, err)
					continue nextEndpoint
				}
				continue nextMessage // sent
			case Disque:
				if err := sendDisqueMessage(endpoint, []byte(msg)); err != nil {
					lerrs = append(lerrs, err)
					continue nextEndpoint
				}
				continue nextMessage // sent
			}
		}
	}
	if len(lerrs) == 0 {
		//	log.Notice("YAY")
		return nil
	}
	var errmsgs []string
	for _, err := range lerrs {
		errmsgs = append(errmsgs, err.Error())
	}
	err := errors.New("not sent: " + strings.Join(errmsgs, ","))
	log.Error(err)
	return err
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

func (c *Controller) cmdSetHook(msg *server.Message) (res string, d commandDetailsT, err error) {
	start := time.Now()

	vs := msg.Values[1:]
	var name, values, cmd string
	var ok bool
	if vs, name, ok = tokenval(vs); !ok || name == "" {
		return "", d, errInvalidNumberOfArguments
	}
	if vs, values, ok = tokenval(vs); !ok || values == "" {
		return "", d, errInvalidNumberOfArguments
	}
	var endpoints []Endpoint
	for _, value := range strings.Split(values, ",") {
		endpoint, err := parseEndpoint(value)
		if err != nil {
			log.Errorf("sethook: %v", err)
			return "", d, errInvalidArgument(value)
		}
		endpoints = append(endpoints, endpoint)
	}

	commandvs := vs
	if vs, cmd, ok = tokenval(vs); !ok || cmd == "" {
		return "", d, errInvalidNumberOfArguments
	}

	cmdlc := strings.ToLower(cmd)
	var types []string
	switch cmdlc {
	default:
		return "", d, errInvalidArgument(cmd)
	case "nearby":
		types = nearbyTypes
	case "within", "intersects":
		types = withinOrIntersectsTypes
	}
	s, err := c.cmdSearchArgs(cmdlc, vs, types)
	if err != nil {
		return "", d, err
	}
	if !s.fence {
		return "", d, errors.New("missing FENCE argument")
	}
	s.cmd = cmdlc

	cmsg := &server.Message{}
	*cmsg = *msg
	cmsg.Values = commandvs
	cmsg.Command = strings.ToLower(cmsg.Values[0].String())

	hook := &Hook{
		Key:       s.key,
		Name:      name,
		Endpoints: endpoints,
		Fence:     &s,
		Message:   cmsg,
	}
	var wr bytes.Buffer
	hook.ScanWriter, err = c.newScanWriter(&wr, cmsg, s.key, s.output, s.precision, s.glob, false, s.limit, s.wheres, s.nofields)
	if err != nil {
		return "", d, err
	}

	if h, ok := c.hooks[name]; ok {
		// lets see if the previous hook matches the new hook
		if h.Key == hook.Key && h.Name == hook.Name {
			if len(h.Endpoints) == len(hook.Endpoints) {
				match := true
				for i, endpoint := range h.Endpoints {
					if endpoint.Original != hook.Endpoints[i].Original {
						match = false
						break
					}
				}
				if match && resp.ArrayValue(h.Message.Values).Equals(resp.ArrayValue(hook.Message.Values)) {
					switch msg.OutputType {
					case server.JSON:
						return server.OKMessage(msg, start), d, nil
					case server.RESP:
						return ":0\r\n", d, nil
					}
				}
			}
		}

		// delete the previous hook
		if hm, ok := c.hookcols[h.Key]; ok {
			delete(hm, h.Name)
		}
		delete(c.hooks, h.Name)
	}
	d.updated = true
	d.timestamp = time.Now()
	c.hooks[name] = hook
	hm, ok := c.hookcols[hook.Key]
	if !ok {
		hm = make(map[string]*Hook)
		c.hookcols[hook.Key] = hm
	}
	hm[name] = hook
	switch msg.OutputType {
	case server.JSON:
		return server.OKMessage(msg, start), d, nil
	case server.RESP:
		return ":1\r\n", d, nil
	}
	return "", d, nil
}

func (c *Controller) cmdDelHook(msg *server.Message) (res string, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var name string
	var ok bool
	if vs, name, ok = tokenval(vs); !ok || name == "" {
		return "", d, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return "", d, errInvalidNumberOfArguments
	}
	if h, ok := c.hooks[name]; ok {
		if hm, ok := c.hookcols[h.Key]; ok {
			delete(hm, h.Name)
		}
		delete(c.hooks, h.Name)
		d.updated = true
	}
	d.timestamp = time.Now()

	switch msg.OutputType {
	case server.JSON:
		return server.OKMessage(msg, start), d, nil
	case server.RESP:
		if d.updated {
			return ":1\r\n", d, nil
		}
		return ":0\r\n", d, nil
	}
	return
}

func (c *Controller) cmdHooks(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var pattern string
	var ok bool
	if vs, pattern, ok = tokenval(vs); !ok || pattern == "" {
		return "", errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return "", errInvalidNumberOfArguments
	}

	var hooks []*Hook
	for name, hook := range c.hooks {
		match, _ := glob.Match(pattern, name)
		if match {
			hooks = append(hooks, hook)
		}
	}
	sort.Sort(hooksByName(hooks))

	switch msg.OutputType {
	case server.JSON:
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
			buf.WriteString(`],"command":[`)
			for i, v := range hook.Message.Values {
				if i > 0 {
					buf.WriteString(`,`)
				}
				buf.WriteString(jsonString(v.String()))
			}

			buf.WriteString(`]}`)
		}
		buf.WriteString(`],"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return buf.String(), nil
	case server.RESP:
		var vals []resp.Value
		for _, hook := range hooks {
			var hvals []resp.Value
			hvals = append(hvals, resp.StringValue(hook.Name))
			hvals = append(hvals, resp.StringValue(hook.Key))
			var evals []resp.Value
			for _, endpoint := range hook.Endpoints {
				evals = append(evals, resp.StringValue(endpoint.Original))
			}
			hvals = append(hvals, resp.ArrayValue(evals))
			hvals = append(hvals, resp.ArrayValue(hook.Message.Values))
			vals = append(vals, resp.ArrayValue(hvals))
		}
		data, err := resp.ArrayValue(vals).MarshalRESP()
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	return "", nil
}
