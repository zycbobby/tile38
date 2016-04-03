package controller

import (
	"net"
	"time"

	"github.com/tidwall/resp"
)

// Conn represents a simple resp connection.
type Conn struct {
	conn net.Conn
	rd   *resp.Reader
	wr   *resp.Writer
}

// DialTimeout dials a resp server.
func DialTimeout(address string, timeout time.Duration) (*Conn, error) {
	tcpconn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	conn := &Conn{
		conn: tcpconn,
		rd:   resp.NewReader(tcpconn),
		wr:   resp.NewWriter(tcpconn),
	}
	return conn, nil
}

// Close closes the connection.
func (conn *Conn) Close() error {
	conn.wr.WriteMultiBulk("quit")
	return conn.conn.Close()
}

// Do performs a command and returns a resp value.
func (conn *Conn) Do(commandName string, args ...interface{}) (val resp.Value, err error) {
	if err := conn.wr.WriteMultiBulk(commandName, args...); err != nil {
		return val, err
	}
	val, _, err = conn.rd.ReadValue()
	return val, err
}
