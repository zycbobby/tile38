package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"

	"github.com/tidwall/tile38/controller"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/log"
	"github.com/tidwall/tile38/server"
)

var (
	dir         string
	port        int
	verbose     bool
	veryVerbose bool
	devMode     bool
	quiet       bool
)

func main() {
	flag.IntVar(&port, "p", 9851, "The listening port for communication.")
	flag.StringVar(&dir, "d", "data", "The data directory.")
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging.")
	flag.BoolVar(&quiet, "q", false, "Quiet logging. Totally silent.")
	flag.BoolVar(&veryVerbose, "vv", false, "Enable very verbose logging.")
	flag.BoolVar(&devMode, "dev", false, "Activates dev mode. DEV ONLY.")
	flag.Parse()
	var logw io.Writer = os.Stderr
	if quiet {
		logw = ioutil.Discard
	}
	log.Default = log.New(logw, &log.Config{
		HideDebug: !veryVerbose,
		HideWarn:  !(veryVerbose || verbose),
	})
	controller.DevMode = devMode
	controller.ShowDebugMessages = veryVerbose
	server.ShowDebugMessages = veryVerbose

	//  _____ _ _     ___ ___
	// |_   _|_| |___|_  | . |
	//   | | | | | -_|_  | . |
	//   |_| |_|_|___|___|___|

	fmt.Fprintf(logw, `
   _______ _______
  |       |       |
  |____   |   _   |   Tile38 %s (%s) %d bit (%s/%s)
  |       |       |   Port: %d, PID: %d
  |____   |   _   |
  |       |       |   tile38.com
  |_______|_______|
`+"\n", core.Version, core.GitSHA, strconv.IntSize, runtime.GOARCH, runtime.GOOS, port, os.Getpid())

	if err := controller.ListenAndServe(port, dir); err != nil {
		log.Fatal(err)
	}
}
