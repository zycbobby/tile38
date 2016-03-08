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
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
)

// This phrase is copied nearly verbatim from Redis.
// https://github.com/antirez/redis/blob/cf42c48adcea05c1bd4b939fcd36a01f23ec6303/src/networking.c
var deniedMessage = []byte(strings.TrimSpace(`
ACCESS DENIED
Tile38 is running in protected mode because protected mode is enabled, no host
address was specified, no authentication password is requested to clients. In
this mode connections are only accepted from the loopback interface. If you
want to connect from external computers to Tile38 you may adopt one of the
following solutions: 

1) Disable protected mode by sending the command 'CONFIG SET protected-mode no'
   from the loopback interface by connecting to Tile38 from the same host 
   the server is running, however MAKE SURE Tile38 is not publicly accessible
   from internet if you do so. Use CONFIG REWRITE to make this change
   permanent. 
2) Alternatively you can just disable the protected mode by editing the Tile38
   configuration file, and setting the 'protected-mode' option to 'no', and
   then restarting the server.
3) If you started the server manually just for testing, restart it with the
   '--protected-mode no' option. 
4) Use a host address or an authentication password. 

NOTE: You only need to do one of the above things in order for the server 
to start accepting connections from the outside.
`) + "\r\n")

type Conn struct {
	net.Conn
	Authenticated bool
}

var errCloseHTTP = errors.New("close http")

// ListenAndServe starts a tile38 server at the specified address.
func ListenAndServe(
	host string, port int,
	protected func() bool,
	handler func(conn *Conn, command []byte, rd *bufio.Reader, w io.Writer, websocket bool) error,
) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
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
		go handleConn(&Conn{Conn: conn}, protected, handler)
	}
}

func writeCommandErr(proto client.Proto, conn *Conn, err error) error {
	if proto == client.HTTP || proto == client.WebSocket {
		conn.Write([]byte(`HTTP/1.1 500 ` + err.Error() + "\r\nConnection: close\r\n\r\n"))
	}
	return err
}

func handleConn(
	conn *Conn,
	protected func() bool,
	handler func(conn *Conn, command []byte, rd *bufio.Reader, w io.Writer, websocket bool) error,
) {
	addr := conn.RemoteAddr().String()
	if core.ShowDebugMessages {
		log.Debugf("opened connection: %s", addr)
		defer func() {
			log.Debugf("closed connection: %s", addr)
		}()
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") && !strings.HasPrefix(addr, "[::1]:") {
		if protected() {
			// This is a protected server. Only loopback is allowed.
			conn.Write(deniedMessage)
			conn.Close()
			return
		}
	}
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for i := 0; ; i++ {
		err := func() error {
			command, proto, auth, err := client.ReadMessage(rd, conn)
			if err != nil {
				return err
			}
			if len(command) > 0 && (command[0] == 'Q' || command[0] == 'q') && strings.ToLower(string(command)) == "quit" {
				return io.EOF
			}
			var b bytes.Buffer
			var denied bool
			if (proto == client.HTTP || proto == client.WebSocket) && auth != "" {
				if err := handler(conn, []byte("AUTH "+auth), rd, &b, proto == client.WebSocket); err != nil {
					return writeCommandErr(proto, conn, err)
				}
				if strings.HasPrefix(b.String(), `{"ok":false`) {
					denied = true
				} else {
					b.Reset()
				}
			}
			if !denied {
				if err := handler(conn, command, rd, &b, proto == client.WebSocket); err != nil {
					return writeCommandErr(proto, conn, err)
				}
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
