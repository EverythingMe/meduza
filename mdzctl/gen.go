package main

import (
	"fmt"
	"os"

	"github.com/codegangsta/cli"
	"github.com/EverythingMe/meduza/mdzctl/codegen"
)

var langs = []string{"py", "go"}

var genCommand = cli.Command{
	Name:  "gen",
	Usage: "Generate models from a schema file",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "lang, l",
			Usage: "language to generate either py or go",
		},
		cli.StringFlag{
			Name:  "file, f",
			Usage: "schema file to read. pass - to read from stdin",
		},

		cli.StringFlag{
			Name:  "output, o",
			Usage: "(optional) write output to this file. otherwise to stdout",
		},
	},
	Action: gen,
}

func gen(c *cli.Context) {

	lang := c.String("lang")
	file := c.String("file")
	output := c.String("output")

	if file == "" {
		fmt.Fprintln(os.Stderr, "No schema file given")
		return
	}

	var b []byte
	var err error
	if file == "-" {

		b, err = codegen.Generate(lang, os.Stdin)
	} else {
		b, err = codegen.GenerateFile(lang, file)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "Error generating:", err)
		return
	}

	if output != "" {
		fp, err := os.Create(output)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error creating out file:", err)
			return
		}
		defer fp.Close()
		if _, err = fp.Write(b); err != nil {
			fmt.Fprintln(os.Stderr, "Error writing out file:", err)
			return
		}

		fmt.Println("Generated code written to ", output)
	} else {
		fmt.Println(string(b))
	}

}
