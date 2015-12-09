package main

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/codegangsta/cli"
)

var confDumpCommand = cli.Command{
	Name:  "confdump",
	Usage: "Dump the current config of a server",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "server, s",
			Value: "http://localhost:9966",
			Usage: "The meduza control server to deploy to",
		},
	},
	Action: config_dump,
}

func config_dump(c *cli.Context) {

	server := c.String("server")

	u := fmt.Sprintf("%s/confdump", server)

	res, err := http.Get(u)

	if err != nil {
		perror("Could not post dumpconf request to server: %s", err)
		return
	}
	defer res.Body.Close()

	if _, err = io.Copy(os.Stdout, res.Body); err != nil {
		perror("Could not get body: %s", err)
	}

}
