package controller

import (
	"net"
	"time"

	"github.com/tidwall/resp"
)

type Conn struct {
	conn net.Conn
	rd   *resp.Reader
	wr   *resp.Writer
}

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

func (conn *Conn) Close() error {
	conn.wr.WriteMultiBulk("quit")
	return conn.conn.Close()
}

func (conn *Conn) Do(commandName string, args ...interface{}) (val resp.Value, err error) {
	if err := conn.wr.WriteMultiBulk(commandName, args...); err != nil {
		return val, err
	}
	val, _, err = conn.rd.ReadValue()
	return val, err
}
