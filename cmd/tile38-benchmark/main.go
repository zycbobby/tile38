package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tidwall/tile38/core"
)

var (
	hostname = "127.0.0.1"
	port     = 9851
	clients  = 50
	requests = 100000
	quiet    = false
	pipeline = 1
	csv      = false
	json     = false
	tests    = "PING,SET,GET,SEARCH"
	redis    = false
)

var addr string

func showHelp() bool {
	gitsha := ""
	if core.GitSHA == "" || core.GitSHA == "0000000" {
		gitsha = ""
	} else {
		gitsha = " (git:" + core.GitSHA + ")"
	}
	fmt.Fprintf(os.Stdout, "tile38-benchmark %s%s\n\n", core.Version, gitsha)
	fmt.Fprintf(os.Stdout, "Usage: tile38-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests>]\n")

	fmt.Fprintf(os.Stdout, " -h <hostname>      Server hostname (default: %s)\n", hostname)
	fmt.Fprintf(os.Stdout, " -p <port>          Server port (default: %d)\n", port)
	fmt.Fprintf(os.Stdout, " -c <clients>       Number of parallel connections (default %d)\n", clients)
	fmt.Fprintf(os.Stdout, " -n <requests>      Total number or requests (default %d)\n", requests)
	fmt.Fprintf(os.Stdout, " -q                 Quiet. Just show query/sec values\n")
	fmt.Fprintf(os.Stdout, " -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).\n")
	fmt.Fprintf(os.Stdout, " -t <tests>         Only run the comma separated list of tests. The test\n")
	fmt.Fprintf(os.Stdout, "                    names are the same as the ones produced as output.\n")
	fmt.Fprintf(os.Stdout, " --csv              Output in CSV format.\n")
	fmt.Fprintf(os.Stdout, " --json             Request JSON responses (default is RESP output)\n")
	fmt.Fprintf(os.Stdout, " --redis            Runs against a Redis server\n")
	fmt.Fprintf(os.Stdout, "\n")
	return false
}

func parseArgs() bool {
	defer func() {
		if v := recover(); v != nil {
			if v, ok := v.(string); ok && v == "bad arg" {
				showHelp()
			}
		}
	}()

	args := os.Args[1:]
	readArg := func(arg string) string {
		if len(args) == 0 {
			panic("bad arg")
		}
		var narg = args[0]
		args = args[1:]
		return narg
	}
	readIntArg := func(arg string) int {
		n, err := strconv.ParseUint(readArg(arg), 10, 64)
		if err != nil {
			panic("bad arg")
		}
		return int(n)
	}
	badArg := func(arg string) bool {
		fmt.Fprintf(os.Stderr, "Unrecognized option or bad number of args for: '%s'\n", arg)
		return false
	}

	for len(args) > 0 {
		arg := readArg("")
		if arg == "--help" || arg == "-?" {
			return showHelp()
		}
		if !strings.HasPrefix(arg, "-") {
			args = append([]string{arg}, args...)
			break
		}
		switch arg {
		default:
			return badArg(arg)
		case "-h":
			hostname = readArg(arg)
		case "-p":
			port = readIntArg(arg)
		case "-c":
			clients = readIntArg(arg)
			if clients <= 0 {
				clients = 1
			}
		case "-n":
			requests = readIntArg(arg)
			if requests <= 0 {
				requests = 0
			}
		case "-q":
			quiet = true
		case "-P":
			pipeline = readIntArg(arg)
			if pipeline <= 0 {
				pipeline = 1
			}
		case "-t":
			tests = readArg(arg)
		case "--csv":
			csv = true
		case "--json":
			json = true
		case "--redis":
			redis = true
		}
	}
	return true
}

func fillOpts() *Options {
	return &Options{
		JSON:     json,
		CSV:      csv,
		Clients:  clients,
		Pipeline: pipeline,
		Quiet:    quiet,
		Requests: requests,
	}
}
func randPoint() (lat, lon float64) {
	return rand.Float64()*180 - 90, rand.Float64()*360 - 180
}
func randRect() (minlat, minlon, maxlat, maxlon float64) {
	for {
		minlat, minlon = randPoint()
		maxlat, maxlon = minlat+1, minlon+1
		if maxlat <= 180 && maxlon <= 180 {
			return
		}
	}
}
func main() {
	rand.Seed(time.Now().UnixNano())
	if !parseArgs() {
		return
	}
	addr = fmt.Sprintf("%s:%d", hostname, port)
	for _, test := range strings.Split(tests, ",") {
		switch strings.ToUpper(strings.TrimSpace(test)) {
		case "PING":
			RedisBench("PING", addr, fillOpts(),
				func(buf []byte, _ ServerType) []byte {
					return AppendCommand(buf, "PING")
				},
			)
		case "SET":
			if redis {
				RedisBench("SET", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						return AppendCommand(buf, "SET", "key:__rand_int__", "xxx")
					},
				)
			} else {
				var i int64
				RedisBench("SET (point)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						i := atomic.AddInt64(&i, 1)
						lat, lon := randPoint()
						return AppendCommand(buf, "SET", "key:bench", "id:"+strconv.FormatInt(i, 10), "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
						)
					},
				)
				RedisBench("SET (rect)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						i := atomic.AddInt64(&i, 1)
						minlat, minlon, maxlat, maxlon := randRect()
						return AppendCommand(buf, "SET", "key:bench", "id:"+strconv.FormatInt(i, 10), "BOUNDS",
							strconv.FormatFloat(minlat, 'f', 5, 64),
							strconv.FormatFloat(minlon, 'f', 5, 64),
							strconv.FormatFloat(maxlat, 'f', 5, 64),
							strconv.FormatFloat(maxlon, 'f', 5, 64),
						)
					},
				)
				RedisBench("SET (string)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						i := atomic.AddInt64(&i, 1)
						return AppendCommand(buf, "SET", "key:bench", "id:"+strconv.FormatInt(i, 10), "STRING", "xxx")
					},
				)
			}
		case "GET":
			if redis {
				RedisBench("GET", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						return AppendCommand(buf, "GET", "key:__rand_int__")
					},
				)
			} else {
				var i int64
				RedisBench("GET (point)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						i := atomic.AddInt64(&i, 1)
						return AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "POINT")
					},
				)
				RedisBench("GET (rect)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						i := atomic.AddInt64(&i, 1)
						return AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "BOUNDS")
					},
				)
				RedisBench("GET (string)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						i := atomic.AddInt64(&i, 1)
						return AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "OBJECT")
					},
				)
			}
		case "SEARCH":
			if !redis {
				RedisBench("SEARCH (nearby 1km)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						lat, lon := randPoint()
						return AppendCommand(buf, "NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"1000")
					},
				)
				RedisBench("SEARCH (nearby 10km)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						lat, lon := randPoint()
						return AppendCommand(buf, "NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"10000")
					},
				)
				RedisBench("SEARCH (nearby 100km)", addr, fillOpts(),
					func(buf []byte, _ ServerType) []byte {
						lat, lon := randPoint()
						return AppendCommand(buf, "NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"100000")
					},
				)
			}
		}
	}
}
func readResp(rd *bufio.Reader, n int) error {
	for i := 0; i < n; i++ {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return err
		}
		switch line[0] {
		default:
			return errors.New("invalid server response")
		case '+', ':':
		case '-':
			//panic(string(line))
		case '$':
			n, err := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
			if err != nil {
				return err
			}
			if _, err = io.CopyN(ioutil.Discard, rd, n+2); err != nil {
				return err
			}
		case '*':
			n, err := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
			if err != nil {
				return err
			}
			readResp(rd, int(n))
		}
	}
	return nil
}

type ServerType int

const (
	Redis  = 1
	Tile38 = 2
)

type Options struct {
	Requests int
	Clients  int
	Pipeline int
	Quiet    bool
	CSV      bool
	JSON     bool
}

func RedisBench(
	name string,
	addr string,
	opts *Options,
	fill func(buf []byte, server ServerType) []byte,
) {
	var server ServerType
	var totalPayload uint64
	var count uint64
	var duration int64
	rpc := opts.Requests / opts.Clients
	rpcex := opts.Requests % opts.Clients
	var tstop int64
	remaining := int64(opts.Clients)
	errs := make([]error, opts.Clients)
	durs := make([][]time.Duration, opts.Clients)
	conns := make([]net.Conn, opts.Clients)

	// create all clients
	for i := 0; i < opts.Clients; i++ {
		crequests := rpc
		if i == opts.Clients-1 {
			crequests += rpcex
		}
		durs[i] = make([]time.Duration, crequests)
		for j := 0; j < len(durs[i]); j++ {
			durs[i][j] = -1
		}
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			if i == 0 {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				return
			}
			errs[i] = err
		}
		conns[i] = conn
		if conn != nil {
			if i == 0 {
				conn.Write([]byte("info server\r\n"))
				resp := make([]byte, 500)
				conn.Read(resp)
				if strings.Contains(string(resp), "redis_version") {
					if strings.Contains(string(resp), "tile38_version") {
						server = Tile38
					} else {
						server = Redis
					}
				}
			}
			if opts.JSON {
				conn.Write([]byte("output json\r\n"))
				resp := make([]byte, 100)
				conn.Read(resp)
			}
		}
	}

	tstart := time.Now()
	for i := 0; i < opts.Clients; i++ {
		crequests := rpc
		if i == opts.Clients-1 {
			crequests += rpcex
		}

		go func(conn net.Conn, client, crequests int) {
			defer func() {
				atomic.AddInt64(&remaining, -1)
			}()
			if conn == nil {
				return
			}
			err := func() error {
				var buf []byte
				rd := bufio.NewReader(conn)
				for i := 0; i < crequests; i += opts.Pipeline {
					n := opts.Pipeline
					if i+n > crequests {
						n = crequests - i
					}
					buf = buf[:0]
					for i := 0; i < n; i++ {
						buf = fill(buf, server)
					}
					atomic.AddUint64(&totalPayload, uint64(len(buf)))
					start := time.Now()
					_, err := conn.Write(buf)
					if err != nil {
						return err
					}
					if err := readResp(rd, n); err != nil {
						return err
					}
					stop := time.Since(start)
					for j := 0; j < n; j++ {
						durs[client][i+j] = stop / time.Duration(n)
					}
					atomic.AddInt64(&duration, int64(stop))
					atomic.AddUint64(&count, uint64(n))
					atomic.StoreInt64(&tstop, int64(time.Since(tstart)))
				}
				return nil
			}()
			if err != nil {
				errs[client] = err
			}
		}(conns[i], i, crequests)
	}
	var die bool
	for {
		remaining := int(atomic.LoadInt64(&remaining))        // active clients
		count := int(atomic.LoadUint64(&count))               // completed requests
		real := time.Duration(atomic.LoadInt64(&tstop))       // real duration
		totalPayload := int(atomic.LoadUint64(&totalPayload)) // size of all bytes sent
		more := remaining > 0
		var realrps float64
		if real > 0 {
			realrps = float64(count) / (float64(real) / float64(time.Second))
		}
		if !opts.CSV {
			fmt.Printf("\r%s: %.2f", name, realrps)
			if more {
				fmt.Printf("\r")
			} else if opts.Quiet {
				fmt.Printf(" requests per second\n")
			} else {
				fmt.Printf("\r====== %s ======\n", name)
				fmt.Printf("  %d requests completed in %.2f seconds\n", opts.Requests, float64(real)/float64(time.Second))
				fmt.Printf("  %d parallel clients\n", opts.Clients)
				fmt.Printf("  %d bytes payload\n", totalPayload/opts.Requests)
				fmt.Printf("  keep alive: 1\n")
				fmt.Printf("\n")
				var limit time.Duration
				var lastper float64
				for {
					limit += time.Millisecond
					var hits, count int
					for i := 0; i < len(durs); i++ {
						for j := 0; j < len(durs[i]); j++ {
							dur := durs[i][j]
							if dur == -1 {
								continue
							}
							if dur < limit {
								hits++
							}
							count++
						}
					}
					per := float64(hits) / float64(count)
					if math.Floor(per*10000) == math.Floor(lastper*10000) {
						continue
					}
					lastper = per
					fmt.Printf("%.2f%% <= %d milliseconds\n", per*100, (limit-time.Millisecond)/time.Millisecond)
					if per == 1.0 {
						break
					}
				}
				fmt.Printf("%.2f requests per second\n\n", realrps)
			}
		}
		if !more {
			if opts.CSV {
				fmt.Printf("\"%s\",\"%.2f\"\n", name, realrps)
			}
			for _, err := range errs {
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s\n", err)
					die = true
					if count == 0 {
						break
					}
				}
			}
			break
		}
		time.Sleep(time.Second / 5)
	}

	// close clients
	for i := 0; i < len(conns); i++ {
		if conns[i] != nil {
			conns[i].Close()
		}
	}
	if die {
		os.Exit(1)
	}
}
func AppendCommand(buf []byte, args ...string) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}
