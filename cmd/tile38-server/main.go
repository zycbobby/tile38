package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/tidwall/tile38/controller"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
)

var (
	dir         string
	port        int
	host        string
	verbose     bool
	veryVerbose bool
	devMode     bool
	quiet       bool
)

// Fire up a webhook test server by using the --webhook-consumer-port
// for example
//   $ ./tile38-server --webhook-consumer-port 9999
//
// The create hooks like such...
//   SETHOOK myhook http://localhost:9999/myhook NEARBY mykey FENCE POINT 33.5 -115.5 1000

func main() {
	if len(os.Args) == 3 && os.Args[1] == "--webhook-consumer-port" {
		log.Default = log.New(os.Stderr, &log.Config{})
		port, err := strconv.ParseUint(os.Args[2], 10, 16)
		if err != nil {
			log.Fatal(err)
		}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Error(err)
				return
			}
			log.HTTPf("http: %s : %s", r.URL.Path, string(data))
		})
		log.Infof("webhook server http://localhost:%d/", port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Fatal(err)
		}
		return
	}

	// parse non standard args.
	nargs := []string{os.Args[0]}
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--protected-mode", "-protected-mode":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "no":
					core.ProtectedMode = "no"
				case "yes":
					core.ProtectedMode = "yes"
				}
				continue
			}
			fmt.Fprintf(os.Stderr, "protected-mode must be 'yes' or 'no'\n")
			os.Exit(1)
		case "--dev", "-dev":
			devMode = true
			continue
		}
		nargs = append(nargs, os.Args[i])
	}
	os.Args = nargs

	flag.IntVar(&port, "p", 9851, "The listening port.")
	flag.StringVar(&host, "h", "", "The listening host.")
	flag.StringVar(&dir, "d", "data", "The data directory.")
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging.")
	flag.BoolVar(&quiet, "q", false, "Quiet logging. Totally silent.")
	flag.BoolVar(&veryVerbose, "vv", false, "Enable very verbose logging.")
	flag.Parse()
	var logw io.Writer = os.Stderr
	if quiet {
		logw = ioutil.Discard
	}
	log.Default = log.New(logw, &log.Config{
		HideDebug: !veryVerbose,
		HideWarn:  !(veryVerbose || verbose),
	})
	core.DevMode = devMode
	core.ShowDebugMessages = veryVerbose

	hostd := ""
	if host != "" {
		hostd = "Addr: " + host + ", "
	}
	gitsha := " (" + core.GitSHA + ")"
	if gitsha == " (0000000)" {
		gitsha = ""
	}

	//  _____ _ _     ___ ___
	// |_   _|_| |___|_  | . |
	//   | | | | | -_|_  | . |
	//   |_| |_|_|___|___|___|

	fmt.Fprintf(logw, `
   _______ _______
  |       |       |
  |____   |   _   |   Tile38 %s%s %d bit (%s/%s)
  |       |       |   %sPort: %d, PID: %d
  |____   |   _   |
  |       |       |   tile38.com
  |_______|_______|
`+"\n", core.Version, gitsha, strconv.IntSize, runtime.GOARCH, runtime.GOOS, hostd, port, os.Getpid())

	if err := controller.ListenAndServe(host, port, dir); err != nil {
		log.Fatal(err)
	}
}
