package schema

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/dvirsky/go-pylog/logging"
)

const tmpSchema = `
schema: tmp
tables:
    Users:
        engines: 
            - redis
        primary:
            type: random
        columns:
            name: 
                type: Text
            email:
                type: Text
            lastVisit:
                type: Timestamp
            score:
                type: Int
        indexes:
            -   type: simple
                columns: [name]
            -   type: compound
                columns: [name,email]
            -   type: compound
                columns: [name,score]
`

func TestFilesProvider(t *testing.T) {

	sp := NewFilesProvider("./")

	err := sp.Init()
	if err != nil {
		t.Errorf("Failed initializing schema provider: %s", err)
	}

	schemas := sp.Schemas()
	if schemas == nil || len(schemas) == 0 {
		t.Errorf("No schemas loaded for files provider")
	}

	for _, sc := range schemas {
		if sc == nil || len(sc.Tables) == 0 {
			t.Errorf("Invalid schema: %s", *sc)
		}
	}

	numUpdates := 0
	ch, err := sp.Updates()
	if err != nil {
		t.Fatal("Failed getting updates:", err)
	}

	fileName := "tmp.schema.yaml"
	expectedName := "tmp"

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {

		for sc := range ch {
			fmt.Println("Got update for shcema %s", sc)
			if sc.Name != expectedName {
				t.Fatal("Wrong schema name: %s", sc.Name)
			}
			numUpdates++
			wg.Done()
			return
		}

	}()

	fp, err := os.Create(fileName)
	if err != nil {
		t.Errorf("Failed creating temp schema file: %s", err)
	}
	defer os.Remove(fileName)

	fp.WriteString(tmpSchema)
	fp.Close()
	logging.Debug("Update num: ", numUpdates)
	wg.Wait()
	sp.Stop()
	fmt.Println("Update num: ", numUpdates)
	if numUpdates != 1 {
		t.Errorf("Expected 1 async update, got %d", numUpdates)
	}

}
