// mdzctl is a small command line tool for controlling meduza
package main

import (
	"fmt"
	"os"

	"github.com/codegangsta/cli"
	"github.com/dvirsky/go-pylog/logging"
)

func main() {

	logging.SetLevel(0)
	app := cli.NewApp()
	app.Name = "mdzctl"
	app.Usage = "Meduza cli helper"
	app.Version = "0.1"
	app.Commands = []cli.Command{
		genCommand,
		deployCommand,
		confDumpCommand,
		statsCommand,
		dumpCommand,
		loadCommand,
	}
	app.RunAndExitOnError()
}

func perror(msg string, args ...interface{}) {
	fmt.Fprintln(os.Stderr, "ERROR:", fmt.Sprintf(msg, args...))
}
