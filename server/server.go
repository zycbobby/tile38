package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/tidwall/tile38/client"
	"github.com/tidwall/tile38/log"
)

var errCloseHTTP = errors.New("close http")

// ShowDebugMessages allows for log.Debug to print to console.
var ShowDebugMessages = false

// ListenAndServe starts a tile38 server at the specified address.
func ListenAndServe(
	port int,
	handler func(command []byte, conn net.Conn, rd *bufio.Reader, w io.Writer, websocket bool) error,
) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	log.Infof("The server is now ready to accept connections on port %d", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Error(err)
			continue
		}
		go handleConn(conn, handler)
	}
}

func handleConn(
	conn net.Conn,
	handler func(command []byte, conn net.Conn, rd *bufio.Reader, w io.Writer, websocket bool) error,
) {
	if ShowDebugMessages {
		addr := conn.RemoteAddr().String()
		log.Debugf("opened connection: %s", addr)
		defer func() {
			log.Debugf("closed connection: %s", addr)
		}()
	}
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for i := 0; ; i++ {
		err := func() error {
			command, proto, err := client.ReadMessage(rd, conn)
			if err != nil {
				return err
			}
			if len(command) > 0 && (command[0] == 'Q' || command[0] == 'q') && strings.ToLower(string(command)) == "quit" {
				return io.EOF
			}
			var b bytes.Buffer
			if err := handler(command, conn, rd, &b, proto == client.WebSocket); err != nil {
				if proto == client.HTTP {
					conn.Write([]byte(`HTTP/1.1 500 ` + err.Error() + "\r\nConnection: close\r\n\r\n"))
				}
				return err
			}
			switch proto {
			case client.Native:
				if err := client.WriteMessage(conn, b.Bytes()); err != nil {
					return err
				}
			case client.HTTP:
				if err := client.WriteHTTP(conn, b.Bytes()); err != nil {
					return err
				}
				return errCloseHTTP
			case client.WebSocket:
				if err := client.WriteWebSocket(conn, b.Bytes()); err != nil {
					return err
				}
				if _, err := conn.Write([]byte{137, 0}); err != nil {
					return err
				}
				return errCloseHTTP
			default:
				b.WriteString("\r\n")
				if _, err := conn.Write(b.Bytes()); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			if err == io.EOF {
				return
			}
			if err == errCloseHTTP ||
				strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			log.Error(err)
			return
		}
	}
}
