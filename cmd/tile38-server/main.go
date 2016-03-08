package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/tidwall/tile38/controller"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
)

var (
	dir           string
	port          int
	host          string
	verbose       bool
	veryVerbose   bool
	devMode       bool
	quiet         bool
	protectedMode bool = true
)

func main() {
	// parse non standard args.
	nargs := []string{os.Args[0]}
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--protected-mode", "-protected-mode":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "no":
					protectedMode = false
				case "yes":
					protectedMode = true
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
	core.ProtectedMode = protectedMode

	hostd := ""
	if host != "" {
		hostd = "Addr: " + host + ", "
	}

	//  _____ _ _     ___ ___
	// |_   _|_| |___|_  | . |
	//   | | | | | -_|_  | . |
	//   |_| |_|_|___|___|___|

	fmt.Fprintf(logw, `
   _______ _______
  |       |       |
  |____   |   _   |   Tile38 %s (%s) %d bit (%s/%s)
  |       |       |   %sPort: %d, PID: %d
  |____   |   _   |
  |       |       |   tile38.com
  |_______|_______|
`+"\n", core.Version, core.GitSHA, strconv.IntSize, runtime.GOARCH, runtime.GOOS, hostd, port, os.Getpid())

	if err := controller.ListenAndServe(host, port, dir); err != nil {
		log.Fatal(err)
	}
}
