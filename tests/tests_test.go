package tests

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/tidwall/log"
	"github.com/tidwall/tile38/controller"
)

const port = 21098

func init() {
	rand.Seed(time.Now().UnixNano())
}

func uid() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic("random error: " + err.Error())
	}
	return hex.EncodeToString(b)
}
func makeKey(size int) string {
	s := "key+" + uid()
	for len(s) < size {
		s += "+" + uid()
	}
	return s
}
func makeID(size int) string {
	s := "key+" + uid()
	for len(s) < size {
		s += "+" + uid()
	}
	return s
}
func makeJSON(size int) string {
	var buf bytes.Buffer
	buf.WriteString(`{"type":"MultiPoint","coordinates":[`)
	for i := 0; buf.Len() < size; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "[%f,%f]", rand.Float64()*360-180, rand.Float64()*180-90)
	}

	buf.WriteString(`]}`)
	return buf.String()
}

func TestServer(t *testing.T) {
	dir, err := ioutil.TempDir("", "tile38")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	var ln net.Listener
	var done = make(chan bool, 2)
	var ignoreErrs bool
	go func() {
		log.Default = log.New(ioutil.Discard, nil)
		err := controller.ListenAndServeEx("localhost", port, dir, &ln)
		if err != nil {
			if !ignoreErrs {
				t.Fatal(err)
			}
		}
		done <- true
	}()
	defer func() {
		ignoreErrs = true
		ln.Close()
		<-done
	}()
	time.Sleep(time.Millisecond * 100)
	t.Run("PingPong", SubTestPingPong)
	t.Run("SetPoint", SubTestSetPoint)
	t.Run("Set100KB", SubTestSet100KB)
	t.Run("Set1MB", SubTestSet1MB)
	t.Run("Set10MB", SubTestSet10MB)
}

func SubTestPingPong(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rd := bufio.NewReader(conn)
	if _, err := conn.Write(buildCommand("PING")); err != nil {
		t.Fatal(err)
	}
	resp, err := readResponse(rd)
	if err != nil {
		t.Fatal(err)
	}
	if resp != "+PONG\r\n" {
		t.Fatal("expected pong")
	}
}

func SubTestSetPoint(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rd := bufio.NewReader(conn)
	cmd := buildCommand("SET", makeKey(100), makeID(100), "POINT", "33.5", "-115.5")
	if _, err := conn.Write(cmd); err != nil {
		t.Fatal(err)
	}
	resp, err := readResponse(rd)
	if err != nil {
		t.Fatal(err)
	}
	if resp != "+OK\r\n" {
		t.Fatal("expected pong")
	}
}
func testSet(t *testing.T, jsonSize, keyIDSize, frag int) {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rd := bufio.NewReader(conn)
	key := makeKey(keyIDSize)
	id := makeID(keyIDSize)
	json := makeJSON(jsonSize)
	cmd := buildCommand("SET", key, id, "OBJECT", json)
	if frag == 0 {
		if _, err := conn.Write(cmd); err != nil {
			t.Fatal(err)
		}
	} else {
		var nn int
		olen := len(cmd)
		for len(cmd) >= frag {
			if n, err := conn.Write(cmd[:frag]); err != nil {
				t.Fatal(err)
			} else {
				nn += int(n)
			}
			cmd = cmd[frag:]
		}
		if len(cmd) > 0 {
			if n, err := conn.Write(cmd); err != nil {
				t.Fatal(err)
			} else {
				nn += int(n)
			}
		}
		if nn != olen {
			t.Fatal("invalid sent amount")
		}
	}
	resp, err := readResponse(rd)
	if err != nil {
		println(len(resp))
		t.Fatal(err)
	}
	if resp != "+OK\r\n" {
		t.Fatal("expected pong")
	}
	cmd = buildCommand("GET", key, id)
	if _, err := conn.Write(cmd); err != nil {
		t.Fatal(err)
	}
	resp, err = readResponse(rd)
	if err != nil {
		t.Fatal(err)
	}
	diff := float64(len(json))/float64(len(resp)) - 1.0
	if diff > 0.1 {
		t.Fatal("too big of a difference")
	}
}
func SubTestSet100KB(t *testing.T) {
	testSet(t, 100*1024, 5000, 1024)
}
func SubTestSet1MB(t *testing.T) {
	testSet(t, 1024*1024, 5000, 1024)
}
func SubTestSet10MB(t *testing.T) {
	testSet(t, 10*1024*1024, 5000, 1024)
}
func buildCommand(arg ...string) []byte {
	var b []byte
	b = append(b, '*')
	b = append(b, []byte(strconv.FormatInt(int64(len(arg)), 10))...)
	b = append(b, '\r', '\n')
	for _, arg := range arg {
		b = append(b, '$')
		b = append(b, []byte(strconv.FormatInt(int64(len(arg)), 10))...)
		b = append(b, '\r', '\n')
		b = append(b, []byte(arg)...)
		b = append(b, '\r', '\n')
	}
	return b
}

func readResponse(rd *bufio.Reader) (string, error) {
	c, err := rd.ReadByte()
	if err != nil {
		return "", err
	}
	var resp []byte
	switch c {
	default:
		return string(resp), errors.New("invalid response")
	case '+':
		resp, err = readString(rd, []byte{'+'})
	case '$':
		resp, err = readBulk(rd, []byte{'$'})
	}
	if err != nil {
		return string(resp), err
	}
	return string(resp), nil
}

func readString(rd *bufio.Reader, b []byte) ([]byte, error) {
	line, err := rd.ReadBytes('\n')
	if err != nil {
		return b, err
	}
	if len(line) == 1 || line[len(line)-2] != '\r' {
		return b, errors.New("invalid response")
	}
	b = append(b, line...)
	return b, nil
}

func readBulk(rd *bufio.Reader, b []byte) ([]byte, error) {
	line, err := rd.ReadBytes('\n')
	if err != nil {
		return b, err
	}
	if len(line) == 1 || line[len(line)-2] != '\r' {
		return b, errors.New("invalid response")
	}
	b = append(b, line...)
	sz, err := strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
	if err != nil {
		return b, err
	}
	data := make([]byte, int(sz))
	if _, err := io.ReadFull(rd, data); err != nil {
		return b, err
	}
	if len(data) < 2 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return b, errors.New("invalid response")
	}
	b = append(b, data...)
	return b, nil
}
