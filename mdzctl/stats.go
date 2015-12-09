package main

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/codegangsta/cli"
)

var statsCommand = cli.Command{
	Name:  "stats",
	Usage: "Dump driver and model stats",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "server, s",
			Value: "http://localhost:9966",
			Usage: "The meduza control server to deploy to",
		},
	},
	Action: stats,
}

func stats(c *cli.Context) {

	server := c.String("server")

	u := fmt.Sprintf("%s/stats", server)

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
