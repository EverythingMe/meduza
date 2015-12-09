package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

func TestPrimary(t *testing.T) {
	//t.SkipNow()
	p := compoundPrimaryIndex{
		basePrimaryIndex: basePrimaryIndex{},
		properties:       propertyList{"name", "surname"},
		hashed:           false,
	}

	filters := query.NewFilters(query.Within("name", "john", "jane"), query.Within("surname", "doe", "blow"))
	st := time.Now()
	ids, err := p.filtersToIds(filters)
	fmt.Println(time.Since(st))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 {
		t.Errorf("Wrong number of ids generated: %d", len(ids))
	}

	expected := []string{"john|doe|", "john|blow|", "jane|doe|", "jane|blow|"}
	for i, id := range ids {
		_id := string(id.(schema.Key))
		fmt.Println(_id)
		if _id != expected[i] {
			t.Errorf("Expected id %s doesn't match %s", expected[i], _id)
		}
	}

	ents := []schema.Entity{
		*schema.NewEntity("", schema.NewText("name", "john"), schema.NewText("surname", "doe")),
		*schema.NewEntity("", schema.NewText("name", "jack"), schema.NewText("surname", "black")),
	}

	expected = []string{"john|doe|", "jack|black|"}
	for i, ent := range ents {
		id, err := p.GenerateId(ent)
		if err != nil {
			t.Fatal(err)
		}
		if string(id) != expected[i] {
			t.Errorf("Expected id %s doesn't match %s", expected[i], id)
		}
	}

	// Test hashing
	p.hashed = true

	expected = []string{"8686e4d649bdf8b8", "3e0e8cd37d0e5813"}
	for i, ent := range ents {
		id, err := p.GenerateId(ent)
		if err != nil {
			t.Fatal(err)
		}
		if string(id) != expected[i] {
			t.Errorf("Expected id %s doesn't match %s", expected[i], id)
		}
	}

}
