package controller

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// EndpointProtocol is the type of protocol that the endpoint represents.
type EndpointProtocol string

const (
	HTTP   = EndpointProtocol("http")   // HTTP
	Disque = EndpointProtocol("disque") // Disque
)

type EndpointManager struct {
	mu        sync.RWMutex // this is intentionally exposed
	Endpoints map[string]*Endpoint
}

func NewEndpointCollection() *EndpointManager {
	return &EndpointManager{
		Endpoints: make(map[string]*Endpoint),
	}
}

// Get finds an endpoint based on its url. If the enpoint does not
// exist a new only is created.
func (epc *EndpointManager) Validate(url string) error {
	_, err := parseEndpoint(url)
	return err
	/*
		pendpoint := epc.Endpoints[url]
		if pendpoint == nil {
			pendpoint = endpoint
			epc.Endpoints[url] = pendpoint
		}
		return pendpoint, nil
	*/
}

// We use a retain/relase on endoints.
// The calls are directed to the collection instead of
// endpoint itself to avoid having a circular reference to
// the collection.
func (epc *EndpointManager) Open(endpoint string) {
	epc.mu.Lock()
	defer epc.mu.Unlock()
	/*
		ep.referenceCount++
		if ep.referenceCount == 1 {
			ep.Open()
		}
	*/
}

func (epc *EndpointManager) Close(endpoint string) {
	epc.mu.Lock()
	defer epc.mu.Unlock()
	/*
		ep.referenceCount--
		if ep.referenceCount < 0 {
			panic("reference count below zero")
		}
		if ep.referenceCount == 0 {
			ep.Close()
			delete(epc.Endpoints, ep.Original)
		}
	*/
}
func (epc *EndpointManager) Send(endpoint, val string) error {
	epc.mu.Lock()
	defer epc.mu.Unlock()
	return errors.New("unavailable")
	return nil
}

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

/*
func (ep *Endpoint) Open() {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	println("open " + ep.Original)
	// Even though open is called we should wait until the a messages
	// is sent before establishing a network connection.
}

func (ep *Endpoint) Close() {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	println("close " + ep.Original)
	// Make sure to forece close the network connection here.
}

func (ep *Endpoint) Send() error {
	return nil
}
*/
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
