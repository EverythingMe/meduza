package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/codegangsta/cli"
)

var dumpCommand = cli.Command{
	Name:  "dump",
	Usage: "Dump data for a table, for backup",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "server, s",
			Value: "http://localhost:9966",
			Usage: "The meduza control server to deploy to",
		},

		cli.StringFlag{
			Name:  "schema, S",
			Usage: "The schema we'll be dumping",
		},

		cli.StringFlag{
			Name:  "table, t",
			Usage: "The table we'll be dumping",
		},
	},

	Action: dump,
}

func dump(c *cli.Context) {

	server := c.String("server")
	table := c.String("table")
	schm := c.String("schema")

	if table == "" || schm == "" {
		perror("No schema or table given")
		return
	}

	u := fmt.Sprintf("%s/dump?schema=%s&table=%s", server, url.QueryEscape(schm), url.QueryEscape(table))

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
