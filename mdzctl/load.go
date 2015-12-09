package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/codegangsta/cli"
)

var loadCommand = cli.Command{
	Name:  "load",
	Usage: "Load dump data from a backup file",
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

		cli.StringFlag{
			Name:  "file, f",
			Usage: "The file to load, set as - for stdin",
		},
	},

	Action: load,
}

func load(c *cli.Context) {

	server := c.String("server")
	table := c.String("table")
	schm := c.String("schema")

	file := c.String("file")

	if table == "" || schm == "" {
		perror("No schema or table given")
		return
	}
	var body io.Reader

	if file == "-" {
		body = os.Stdin

	} else {

		fp, err := os.Open(file)
		if err != nil {
			perror("Could not open schema file: %s", err)
			return
		}
		defer fp.Close()

		body = fp
	}

	u := fmt.Sprintf("%s/load?schema=%s&table=%s", server, url.QueryEscape(schm), url.QueryEscape(table))

	if file == "-" {
		body = os.Stdin

	} else {

		fp, err := os.Open(file)
		if err != nil {
			perror("Could not open schema file: %s", err)
			return
		}
		defer fp.Close()

		body = fp
	}

	res, err := http.Post(u, "application/octet-stream", body)

	if err != nil {
		perror("Could not post request to server: %s", err)
		return
	}
	defer res.Body.Close()

	if _, err = io.Copy(os.Stdout, res.Body); err != nil {
		perror("Could not get body: %s", err)
	}

}
