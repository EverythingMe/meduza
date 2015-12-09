package schema

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EverythingMe/bson/bson"
	"golang.org/x/text/language"
)

var mockSchema = `
# Mock Schema
schema: mock
tables:
    users:
        engines: 
            - redis
        columns:
            name: 
                comment: "The name of this user"
                type: Text
                options: 
                    not_null: true
            num:
                type: Int
            bum:
                type: Int				
        indexes:
            - type: simple
              columns: [bum]								
    losers:
        engines: 
            - redis
        columns:
            name: 
                comment: "The name of this user"
                type: Text				
`
var mockSchema2 = `
schema: mock
tables:
    users:
        engines: 
            - redis
        columns:
            name: 
                comment: "The name of this user"
                type: Text
                options: 
                    not_null: true
            num:
                type: Text
            sum:
                type: Text
        indexes:
            - type: simple
              columns: [name]				
    bars:
        engines: 
            - redis
        columns:
            name: 
                comment: "The name of this user"
                type: Text
                options: 
                    not_null: true
            num:
                type: Int	
`

func TestLoad(t *testing.T) {
	//t.SkipNow()
	r := strings.NewReader(mockSchema)

	sc, e := Load(r)
	if e != nil {
		t.Fatal(e)
	}

	if sc.Name != "mock" {
		t.Error("Wrong name: expected mock, got %s", sc.Name)
	}

	//b, _ := json.MarshalIndent(sc, "-", "  ")

	if tbl, found := sc.Tables["users"]; !found {
		t.Fatal("Table users not found")
	} else {

		if len(tbl.Columns) != 3 {
			t.Fatal("Expected 2 columns for users")
		}

		if c, found := tbl.Columns["name"]; !found {
			t.Fatal("no column name")
		} else {
			if c.Type != TextType {
				t.Fatal("name is not text but", c.Type)
			}
		}
		if c, found := tbl.Columns["num"]; !found {
			t.Fatal("no column num")
		} else {
			if c.Type != IntType {
				t.Fatal("num is not int but", c.Type)
			}
		}
	}
}

func TestDiff(t *testing.T) {
	//t.SkipNow()
	r := strings.NewReader(mockSchema)

	sc, e := Load(r)
	if e != nil {
		t.Fatal(e)
	}

	r = strings.NewReader(mockSchema2)

	sc2, e := Load(r)
	if e != nil {
		t.Fatal(e)
	}

	diff, err := sc.Diff(sc2)
	if err != nil {
		t.Fatal(err)
	}

	if len(diff) == 0 {
		t.Fatal("No diff detected")
	}

	for _, change := range diff {
		//fmt.Println(reflect.TypeOf(change))
		//b, _ := json.MarshalIndent(change, "-", "  ")
		//fmt.Println(string(b))

		switch ch := change.(type) {
		case TableAddedChange:
			if ch.Table.Name != "mock.bars" {
				t.Fatal("Wrong table added")
			}
		case TableDeletedChange:
			if ch.Table.Name != "mock.losers" {
				t.Fatal("Wrong table deleted")
			}

		case ColumnAlterChange:
			if ch.Column.Name != "num" || ch.Column.Type != TextType {
				t.Fatal("Wrong column change", ch.Column)
			}
		case ColumnAddedChange:
			if ch.Column.Name != "sum" {
				t.Fatal("Wrong column added:", ch.Column.Name)
			}

		case ColumnDeletedChange:
			if ch.Column.Name != "bum" {
				t.Fatal("Wrong column deleted: ", ch.Column.Name)
			}

		case IndexAddedChange:
			if ch.Index.Name != "mock.users__name_simple" {
				t.Fatal("Wrong index added: %s", ch.Index.Name)
			}
		case IndexRemovedChange:
			if ch.Index.Name != "mock.users__bum_simple" {
				t.Fatal("Wrong index deleted: %s", ch.Index.Name)
			}
		default:
			t.Error("Undetected change: ", reflect.TypeOf(ch))
		}
	}

}

func TestNormalization(t *testing.T) {
	//t.SkipNow()
	normalizer := NewNormalizer(language.Und, true, true)

	input := "Hello, Café - WORLD!... אבוללה"
	out, err := normalizer.Normalize([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hello cafe world אבוללה" {
		t.Fatal("Wrong normalization: ", string(out))
	}

}

func TestSet(t *testing.T) {
	s := NewSet("foo", "bar", "baz")

	ent := NewEntity("foo").Set("bar", s)

	b, err := bson.Marshal(ent)
	if err != nil {
		t.Fatal(err)
	}
	e2 := Entity{}

	err = bson.Unmarshal(b, &e2)
	if err != nil {
		t.Fatal(err)
	}

	v, found := e2.Get("bar")

	if !found {
		t.Error("encoded set not found in decoded entity")
	}
	s2 := v.(Set)

	if len(s) != len(s2) {
		t.Errorf("Incompatible list sizes: %d/%d", len(s2), len(s))
	}

	for k := range s2 {
		if _, found := s[k]; !found {
			t.Error(k, "not in ", s)
		}
	}
}

func TestTTL(t *testing.T) {

	ttl := 150 * time.Millisecond
	ent := NewEntity("foo").Set("bar", "baz").Expire(ttl)
	if ent.TTL != ttl {
		t.Fatal("TTL not set correctly, got %v", ent.TTL)
	}
	b, err := bson.Marshal(ent)
	if err != nil {
		t.Fatal(err)
	}

	e2 := Entity{}

	if err = bson.Unmarshal(b, &e2); err != nil {
		t.Fatal(err)
	}

	if e2.TTL != ent.TTL {
		t.Errorf("Unmatching ttls. Want %v, got %v", ent.TTL, e2.TTL)
	}
}
func TestList(t *testing.T) {
	s := NewList("foo", "bar", "baz")

	ent := NewEntity("foo").Set("bar", s)

	b, err := bson.Marshal(ent)
	if err != nil {
		t.Fatal(err)
	}

	e2 := Entity{}

	err = bson.Unmarshal(b, &e2)
	if err != nil {
		t.Fatal(err)
	}

	s2, found := e2.Get("bar")
	if !found {
		t.Error("encoded set not found in decoded entity")
	}
	l2 := s2.(List)
	if len(l2) != len(s) {
		t.Errorf("Incompatible list sizes: %d/%d", len(l2), len(s))
	}
	for i := range l2 {
		if s[i] != l2[i] {
			t.Errorf("Incompatible list elements %v /%v", s[i], l2[i])
		}

	}
}

func TestMap(t *testing.T) {

	m := NewMap().
		Set("foo", "Bar").
		Set("bar", 123)

	ent := NewEntity("foo").Set("map", m)

	b, err := bson.Marshal(ent)
	if err != nil {
		t.Fatal(err)
	}

	e2 := Entity{}

	err = bson.Unmarshal(b, &e2)
	if err != nil {
		t.Fatal(err)
	}

	p2, found := e2.Get("map")
	fmt.Printf("%#v\n", p2)
	if !found {
		t.Error("encoded set not found in decoded entity")
	}
	m2 := p2.(Map)

	if len(m2) != len(m) {
		t.Errorf("Incompatible list sizes: %d/%d", len(m2), len(m))
	}

	for k, v := range m2 {
		if m[k] != v {
			t.Errorf("Incompatible map elements %v(%s)/%v(%s)", m[k], reflect.TypeOf(m[k]), v, reflect.TypeOf(v))
		}
	}

}
