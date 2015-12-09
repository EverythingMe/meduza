package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/codegangsta/cli"
)

var deployCommand = cli.Command{
	Name:  "deploy",
	Usage: "Deploy a schema file to meduza",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "file, f",
			Usage: "schema file to upload. pass - to read from stdin",
		},
		cli.StringFlag{
			Name:  "name, n",
			Usage: "Name of the schema to deploy. Mandatory if reading from stdin, otherwise defaults to the name of the file",
		},
		cli.StringFlag{
			Name:  "server, s",
			Value: "http://localhost:9966",
			Usage: "The meduza control server to deploy to",
		},
	},
	Action: deploy,
}

func deploy(c *cli.Context) {

	file := c.String("file")
	server := c.String("server")

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

	u := fmt.Sprintf("%s/deploy", server)

	res, err := http.Post(u, "text/yaml", body)

	if err != nil {
		perror("Could not post deploy request to server: %s", err)
		return
	}
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		perror("Could not read response body: %s", err)
		return
	}

	s := string(b)
	if s == "OK" {
		fmt.Println("Schema deployed successfully")
	} else {
		perror("Error deploying schema. Server error: %s", s)
	}

}
