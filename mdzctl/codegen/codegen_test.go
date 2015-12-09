package codegen

import (
	"bytes"
	"fmt"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/EverythingMe/meduza/schema"
)

var Title = strings.Title

var scm = `
schema: testung
tables:
    MarketInfo:
        comment:
            Represents localized info from the market for native apps	
        class:
            MarketInfoClass
        engines: 
            - redis
            
        primary:
            type: random
            columns: [packageId, locale]            
       
        columns:    
            packageId:
                type: Text
                options: 
                     required: true
                     max_len: 100

            locale:
                type: Text
                default: en-US

            name:
                type: Text

            score:
                type: Float
                default: 0

            rank:
                type: Float
                default: 0

            installs:
                clientName: installseLowerBounds
                type: Int

            description:
                type: Text
                options:
                    max_len: 10000
            price:
                type: Float
                
            currency:
                type: Text
                options:
                    choices: ['usd', 'nis']
                    
            screens:
                comment: "Ids of the screenshot urls copied to s3"
                clientName: screenshots
                type: List
                options:
                    subtype: Text
            
            lmtime:
                clientName: lastModification
                type: Timestamp
                default: $now
                
            properties:
                type: Map
                options:
                    subtype: Text
                
`

func testGen(t *testing.T, lang string) []byte {

	gen, err := GenerateFile(lang, "../../schema/example.schema.yaml")

	if err != nil {
		t.Fatal(err)
	}

	if len(gen) == 0 || gen == nil {
		t.Fatal("No generated text")
	}

	sc, err := schema.Load(strings.NewReader(scm))
	if err != nil {
		t.Fatal(err)
	}

	gen, err = GenerateSchema(lang, sc)
	if err != nil {
		t.Fatal(err)
	}

	if len(gen) < 200 {
		t.Fatal("gen too short")
	}

	return gen

}

func TestGenPython(t *testing.T) {
	gen := testGen(t, "py")

	fmt.Println(string(gen))
	tmpnam := os.TempDir() + "/gentest.py"
	fp, err := os.Create(tmpnam)
	if err != nil {
		t.Fatal("Could not create temp python file", err)
	}
	if _, err = fp.Write(gen); err != nil {
		t.Fatal(err)
	}
	fp.Close()
	defer os.Remove(tmpnam)

	cmd := exec.Command("python", "-m", "py_compile", tmpnam)

	out, err := cmd.CombinedOutput()

	if !cmd.ProcessState.Success() || string(out) != "" {
		t.Errorf("Error parsing python file: %s", string(out))
	}

}

func TestGenGo(t *testing.T) {
	gen := testGen(t, "go")

	fset := token.NewFileSet() // positions are relative to fset
	f, err := parser.ParseFile(fset, "src.go", gen, 0)
	if err != nil {
		t.Fatalf("Error parsing go: %s", err)
	}

	out := bytes.NewBuffer(nil)
	format.Node(out, fset, f)
	if out.Len() < 500 {
		t.Fatal("Output too small")
	}
}
