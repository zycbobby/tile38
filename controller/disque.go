package controller

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// TODO: add one connection pool per endpoint. Use Redigo.
// The current implementation is too slow.
var endpointDisqueMu sync.Mutex

type endpointDisqueConn struct {
	mu sync.Mutex
}

var endpointDisqueM = make(map[string]*endpointDisqueConn)

func sendDisqueMessage(endpoint Endpoint, msg []byte) error {
	endpointDisqueMu.Lock()
	conn, ok := endpointDisqueM[endpoint.Original]
	if !ok {
		conn = &endpointDisqueConn{
		//client: &http.Client{Transport: &http.Transport{}},
		}
		endpointDisqueM[endpoint.Original] = conn
	}
	endpointDisqueMu.Unlock()
	conn.mu.Lock()
	defer conn.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", endpoint.Disque.Host, endpoint.Disque.Port)
	dconn, err := DialTimeout(addr, time.Second/4)
	if err != nil {
		return err
	}
	defer dconn.Close()
	options := []interface{}{endpoint.Disque.QueueName, msg, 0}
	replicate := endpoint.Disque.Options.Replicate
	if replicate > 0 {
		options = append(options, "REPLICATE")
		options = append(options, endpoint.Disque.Options.Replicate)
	}
	v, err := dconn.Do("ADDJOB", options...)
	if err != nil {
		return err
	}
	if v.Error() != nil {
		return v.Error()
	}
	id := v.String()
	p := strings.Split(id, "-")
	if len(p) != 4 {
		return errors.New("invalid disque reply")
	}
	return nil
}
