package endpoint

import (
	"fmt"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

const (
	mqttExpiresAfter = time.Second * 30
)

type MQTTEndpointConn struct {
	mu   sync.Mutex
	ep   Endpoint
	conn paho.Client
	ex   bool
	t    time.Time
}

func (conn *MQTTEndpointConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Now().Sub(conn.t) > mqttExpiresAfter {
			conn.close()
			conn.ex = true
		}
	}
	return conn.ex
}

func (conn *MQTTEndpointConn) close() {
	if conn.conn != nil {
		if conn.conn.IsConnected() {
			conn.conn.Disconnect(250)
		}

		conn.conn = nil
	}
}

func (conn *MQTTEndpointConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.ex {
		return errExpired
	}
	conn.t = time.Now()

	if conn.conn == nil {
		uri := fmt.Sprintf("tcp://%s:%d", conn.ep.MQTT.Host, conn.ep.MQTT.Port)
		ops := paho.NewClientOptions().SetClientID("tile38").AddBroker(uri)
		c := paho.NewClient(ops)

		if token := c.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}

		conn.conn = c
	}

	t := conn.conn.Publish(conn.ep.MQTT.QueueName, 0, false, msg)
	t.Wait()

	if t.Error() != nil {
		conn.close()
		return t.Error()
	}

	return nil
}

func newMQTTEndpointConn(ep Endpoint) *MQTTEndpointConn {
	return &MQTTEndpointConn{
		ep: ep,
		t:  time.Now(),
	}
}
