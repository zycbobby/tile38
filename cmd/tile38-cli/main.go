package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/peterh/liner"
	"github.com/tidwall/tile38/client"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/kml"
)

func userHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

var (
	historyFile = filepath.Join(userHomeDir(), ".liner_example_history")
)

type connError struct {
	OK  bool   `json:"ok"`
	Err string `json:"err"`
}

var (
	hostname   = "127.0.0.1"
	port       = 9851
	oneCommand string
	tokml      bool
)

func showHelp() bool {
	fmt.Fprintf(os.Stdout, "tile38-cli %s (git:%s)\n\n", core.Version, core.GitSHA)
	fmt.Fprintf(os.Stdout, "Usage: tile38-cli [OPTIONS] [cmd [arg [arg ...]]]\n")
	fmt.Fprintf(os.Stdout, " -h <hostname>      Server hostname (default: %s).\n", hostname)
	fmt.Fprintf(os.Stdout, " -p <port>          Server port (default: %d).\n", port)
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
	badArg := func(arg string) bool {
		fmt.Fprintf(os.Stderr, "Unrecognized option or bad number of args for: '%s'\n", arg)
		return false
	}
	for len(args) > 0 {
		arg := readArg("")
		if arg == "--help" {
			return showHelp()
		}
		if !strings.HasPrefix(arg, "-") {
			args = append([]string{arg}, args...)
			break
		}
		switch arg {
		default:
			return badArg(arg)
		case "-kml":
			tokml = true
		case "-h":
			hostname = readArg(arg)
		case "-p":
			n, err := strconv.ParseUint(readArg(arg), 10, 16)
			if err != nil {
				return badArg(arg)
			}
			port = int(n)
		}
	}
	oneCommand = strings.Join(args, " ")
	return true
}

func refusedErrorString(addr string) string {
	return fmt.Sprintf("Could not connect to Tile38 at %s: Connection refused", addr)
}

var groupsM = make(map[string][]string)

func main() {
	if !parseArgs() {
		return
	}

	addr := fmt.Sprintf("%s:%d", hostname, port)
	conn, err := client.Dial(addr)
	if err != nil {
		if _, ok := err.(net.Error); ok {
			fmt.Fprintln(os.Stderr, refusedErrorString(addr))
		} else {
			fmt.Fprintln(os.Stderr, err.Error())
		}
		return
	}
	defer conn.Close()
	livemode := false
	aof := false
	defer func() {
		if livemode {
			var err error
			if aof {
				_, err = io.Copy(os.Stdout, conn.Reader())
				fmt.Fprintln(os.Stderr, "")
			} else {
				var msg []byte
				for {
					msg, err = conn.ReadMessage()
					if err != nil {
						break
					}
					fmt.Fprintln(os.Stderr, string(msg))
				}
			}
			if err != nil && err != io.EOF {
				fmt.Fprintln(os.Stderr, err.Error())
			}
		}
	}()

	line := liner.NewLiner()
	defer line.Close()

	var commands []string
	for name, command := range core.Commands {
		commands = append(commands, name)
		groupsM[command.Group] = append(groupsM[command.Group], name)
	}
	sort.Strings(commands)
	var groups []string
	for group, arr := range groupsM {
		groups = append(groups, "@"+group)
		sort.Strings(arr)
		groupsM[group] = arr
	}
	sort.Strings(groups)

	line.SetMultiLineMode(false)
	line.SetCtrlCAborts(true)
	line.SetCompleter(func(line string) (c []string) {
		if strings.HasPrefix(strings.ToLower(line), "help ") {
			var nitems []string
			nline := strings.TrimSpace(line[5:])
			if nline == "" || nline[0] == '@' {
				for _, n := range groups {
					if strings.HasPrefix(strings.ToLower(n), strings.ToLower(nline)) {
						nitems = append(nitems, line[:len(line)-len(nline)]+strings.ToLower(n))
					}
				}
			} else {
				for _, n := range commands {
					if strings.HasPrefix(strings.ToLower(n), strings.ToLower(nline)) {
						nitems = append(nitems, line[:len(line)-len(nline)]+strings.ToUpper(n))
					}
				}
			}
			for _, n := range nitems {
				if strings.HasPrefix(strings.ToLower(n), strings.ToLower(line)) {
					c = append(c, n)
				}
			}
		} else {
			for _, n := range commands {
				if strings.HasPrefix(strings.ToLower(n), strings.ToLower(line)) {
					c = append(c, n)
				}
			}
		}
		return
	})
	if f, err := os.Open(historyFile); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
	defer func() {
		if f, err := os.Create(historyFile); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}()
	var raw bool
	for {
		var command string
		var err error
		if oneCommand == "" {
			command, err = line.Prompt(addr + "> ")
		} else {
			command = oneCommand
		}
		if err == nil {
			nohist := strings.HasPrefix(command, " ")
			command = strings.TrimSpace(command)
			if command == "" {
				_, err := conn.Do("pInG")
				if err != nil {
					if err != io.EOF {
						fmt.Fprintln(os.Stderr, err.Error())
					} else {
						fmt.Fprintln(os.Stderr, refusedErrorString(addr))
					}
					return
				}
			} else {
				if !nohist {
					line.AppendHistory(command)
				}
				if (command[0] == 'e' || command[0] == 'E') && strings.ToLower(command) == "exit" {
					return
				}
				if (command[0] == 'q' || command[0] == 'Q') && strings.ToLower(command) == "quit" {
					return
				}
				if (command[0] == 'r' || command[0] == 'R') && strings.ToLower(command) == "raw" {
					raw = true
					fmt.Fprintln(os.Stderr, "raw mode is ON")
					continue
				}
				if (command[0] == 'h' || command[0] == 'H') && (strings.ToLower(command) == "help" || strings.HasPrefix(strings.ToLower(command), "help")) {
					err = help(strings.TrimSpace(command[4:]))
					if err != nil {
						return
					}
					continue
				}
				if (command[0] == 'p' || command[0] == 'P') && strings.ToLower(command) == "pretty" {
					raw = false
					fmt.Fprintln(os.Stderr, "raw mode is OFF")
					continue
				}
				aof = (command[0] == 'a' || command[0] == 'A') && strings.HasPrefix(strings.ToLower(command), "aof ")
				msg, err := conn.Do(command)
				if err != nil {
					if err != io.EOF {
						fmt.Fprintln(os.Stderr, err.Error())
					} else {
						fmt.Fprintln(os.Stderr, refusedErrorString(addr))
					}
					return
				}
				mustOutput := true
				if oneCommand == "" && !strings.HasPrefix(string(msg), `{"ok":true`) {
					var cerr connError
					if err := json.Unmarshal(msg, &cerr); err == nil {
						fmt.Fprintln(os.Stderr, "(error) "+cerr.Err)
						mustOutput = false
					}
				} else if string(msg) == client.LiveJSON {
					fmt.Fprintln(os.Stderr, string(msg))
					livemode = true
					break // break out of prompt and just feed data to screen
				}
				if mustOutput {
					if tokml {
						msg = convert2kml(msg)
					}
					if raw {
						fmt.Fprintln(os.Stdout, string(msg))
					} else {
						fmt.Fprintln(os.Stdout, string(msg))
					}
				}
			}
		} else if err == liner.ErrPromptAborted {
			return
		} else {
			fmt.Fprintf(os.Stderr, "Error reading line: %s", err.Error())
		}
		if oneCommand != "" {
			return
		}
	}
}

func convert2kml(msg []byte) []byte {
	k := kml.New()
	var m map[string]interface{}
	if err := json.Unmarshal(msg, &m); err == nil {
		if v, ok := m["points"].([]interface{}); ok {
			for _, v := range v {
				if v, ok := v.(map[string]interface{}); ok {
					if v, ok := v["point"].(map[string]interface{}); ok {
						var name string
						var lat, lon float64
						if v, ok := v["id"].(string); ok {
							name = v
						}
						if v, ok := v["lat"].(float64); ok {
							lat = v
						}
						if v, ok := v["lon"].(float64); ok {
							lon = v
						}
						k.AddPoint(name, lat, lon)
					}
				}
			}
		}
		return k.Bytes()
	}
	return []byte(`{"ok":false,"err":"results must contain points"}`)
}

func help(arg string) error {
	if arg == "" {
		fmt.Fprintf(os.Stderr, "tile38-cli %s (git:%s)\n", core.Version, core.GitSHA)
		fmt.Fprintf(os.Stderr, `Type: "help @<group>" to get a list of commands in <group>`+"\n")
		fmt.Fprintf(os.Stderr, `      "help <command>" for help on <command>`+"\n")
		fmt.Fprintf(os.Stderr, `      "help <tab>" to get a list of possible help topics`+"\n")
		fmt.Fprintf(os.Stderr, `      "quit" to exit`+"\n")
		return nil
	}
	if strings.HasPrefix(arg, "@") {
		for _, command := range groupsM[arg[1:]] {
			fmt.Fprintf(os.Stderr, "%s\n", core.Commands[command].TermOutput("  "))
		}
	} else {
		if command, ok := core.Commands[strings.ToUpper(arg)]; ok {
			fmt.Fprintf(os.Stderr, "%s\n", command.TermOutput("  "))
		}
	}
	return nil
}
