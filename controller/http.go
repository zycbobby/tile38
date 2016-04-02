package controller

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
)

var endpointHTTPMu sync.Mutex

type endpointHTTPConn struct {
	mu     sync.Mutex
	client *http.Client
}

var endpointHTTPM = make(map[string]*endpointHTTPConn)

func sendHTTPMessage(endpoint Endpoint, msg []byte) error {
	endpointHTTPMu.Lock()
	conn, ok := endpointHTTPM[endpoint.Original]
	if !ok {
		conn = &endpointHTTPConn{
			client: &http.Client{Transport: &http.Transport{}},
		}
		endpointHTTPM[endpoint.Original] = conn
	}
	endpointHTTPMu.Unlock()
	conn.mu.Lock()
	defer conn.mu.Unlock()
	res, err := conn.client.Post(endpoint.Original, "application/json", bytes.NewBuffer(msg))
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("endpoint returned status code %d", res.StatusCode)
	}
	return nil
}
