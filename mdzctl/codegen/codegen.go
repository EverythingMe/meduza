package codegen

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/template"

	"go/format"
	"go/parser"
	"go/token"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/schema"
)

func formatGo(b []byte, err error) ([]byte, error) {
	if err != nil || b == nil {
		return b, err
	}

	fset := token.NewFileSet() // positions are relative to fset
	f, err := parser.ParseFile(fset, "src.go", b, parser.ParseComments)
	if err != nil {
		return nil, errors.NewError("Error parsing generated file: %s", err)
	}

	out := bytes.NewBuffer(nil)
	if err = format.Node(out, fset, f); err != nil {
		return nil, errors.NewError("Error generating file: %s", err)
	}
	return out.Bytes(), nil
}

func generate(templateString string, sc *schema.Schema) ([]byte, error) {
	tpl, err := template.New("schema").Funcs(template.FuncMap{
		"getDefault": preprocessDefault,
	}).Parse(templateString)

	if err != nil {
		return nil, errors.NewError("Could not parse template: %s", err)
	}

	buf := bytes.NewBuffer(nil)

	if err = tpl.Execute(buf, sc); err != nil {
		return nil, errors.NewError("Could not execute template: %s", err)
	}

	return buf.Bytes(), nil

}

func GenerateSchema(lang string, sc *schema.Schema) ([]byte, error) {

	tpl := ""
	switch lang {
	case "py":
		tpl = pythonTemplate
	case "go":
		tpl = goTemplate
	default:
		return nil, errors.NewError("Invalid language: %s. Supported: 'py', 'go'", lang)
	}

	tpl = strings.Replace(
		strings.Replace(
			strings.Replace(tpl, "\\\n", "", -1),
			"\\n", "\n", -1),
		"~", "`", -1)

	logging.Debug("Generating client for %s", lang)
	if lang == "py" {
		return generate(tpl, sc)
	} else {
		return formatGo(generate(tpl, sc))
	}

}

func GenerateFile(lang string, schemaFile string) ([]byte, error) {

	sc, err := schema.LoadFile(schemaFile)
	if err != nil {
		return nil, err
	}

	return GenerateSchema(lang, sc)

}

func Generate(lang string, r io.Reader) ([]byte, error) {

	sc, err := schema.Load(r)
	if err != nil {
		return nil, err
	}

	return GenerateSchema(lang, sc)

}

func preprocessDefault(value interface{}, lang string) string {

	if lang == "py" {
		if value == "$now" {
			return "Timestamp.now"
		}

	}

	if lang == "py" {
		return fmt.Sprintf("%#v", value)
	} else {
		return strings.Trim(fmt.Sprintf("%#v", value), "\"")
	}

}
