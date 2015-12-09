package schema

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/dvirsky/go-pylog/logging"
)

type model struct {
	Id Key `db:",primary"`

	Txt    Text   `db:"text"`
	Int    Int    `db:"int"`
	Float  Float  `db:"float"`
	Bool   Bool   `db:"bool"`
	Binary Binary `db:"binary"`
	Time   Timestamp
	//TODO: Test map/set/list

}

func BenchmarkDecoding(b *testing.B) {

	logging.SetLevel(logging.NOTHING)
	runtime.GOMAXPROCS(4)
	ent := NewEntity("foo")
	ent.Set("text", Text("bar"))
	ent.Set("int", Int(345))
	ent.Set("float", Float(1.5))
	ent.Set("bool", Bool(true))
	ent.Set("binary", Binary([]byte{1, 2, 3, 5}))
	ent.Set("Time", Timestamp(time.Now()))

	m := &model{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			DecodeEntity(*ent, m)
		}
	})
}

func testDecoding(t *testing.T, ent Entity) {

	m := &model{}
	err := DecodeEntity(ent, m)
	if err != nil {
		t.Error("Failed mapping entity to object: ", err)
	}

	if ent.Id != m.Id {
		t.Error("Unmatching ids")
	}

	if m.Int != ent.Properties["int"] {
		t.Error("Unmatching int mapping")
	}
	if m.Float != ent.Properties["float"] {
		t.Error("Unmatching float mapping")
	}
	if m.Bool != ent.Properties["bool"] {
		t.Error("Unmatching bool mapping")
	}
	if m.Txt != ent.Properties["text"] {
		t.Errorf("Unmatching text mapping: '%s'/'%s'", m.Txt, ent.Properties["text"])
	}

	if !reflect.DeepEqual(m.Binary, ent.Properties["binary"]) {
		t.Error("Unmatching binary mapping")
	}

	if !reflect.DeepEqual(m.Time, ent.Properties["Time"]) {
		t.Error("Unmatching time mapping", m.Time, ent.Properties["Time"])
	}
}

func TestEncoding(t *testing.T) {

	runtime.GOMAXPROCS(4)
	ent := NewEntity("foo")
	ent.Set("text", Text("bar"))
	ent.Set("int", Int(345))
	ent.Set("float", 1.5)
	ent.Set("bool", Bool(true))
	ent.Set("binary", Binary([]byte{1, 2, 3, 5}))
	ent.Set("Time", Timestamp(time.Now()))

	m := &model{}
	err := DecodeEntity(*ent, m)
	if err != nil {
		t.Error("Could not decode entity: ", err)
	}
	fmt.Println(m)

	encoded, err := EncodeStruct(m)
	if err != nil || encoded == nil {
		t.Error("Could not encode to entity: ", err)
	}
	if ent.Id != m.Id {
		t.Error("Invalid id decoded")
	}

	if _, found := ent.Get("Id"); found {
		t.Error("Id should not be mapped as a property")
	}

	if len(ent.Properties) != len(encoded.Properties) {
		t.Error("Unmatching number of properties %d/%d", len(ent.Properties), len(encoded.Properties))
	}

	if _, err = EncodeStruct(nil); err == nil {
		t.Error("Encoding nil should have failed")
	}

	if _, err = EncodeStruct("foo"); err == nil {
		t.Error("Encoding non-struct should have failed")
	}

}

func TestDecoding(t *testing.T) {

	// 1. Test with normal types
	ent := NewEntity("foo")
	ent.Set("text", Text("bar"))
	ent.Set("int", Int(345))
	ent.Set("float", Float(1.5))
	ent.Set("bool", Bool(true))
	ent.Set("binary", Binary([]byte{1, 2, 3, 5}))
	ent.Set("Time", Timestamp(time.Now()))
	testDecoding(t, *ent)

	// 2. Test with equivalent assignable types
	ent.Set("text", "bar")
	ent.Set("int", 345)
	ent.Set("float", 1.5)
	ent.Set("bool", true)
	ent.Set("binary", []byte{1, 2, 3, 5})
	ent.Set("Time", time.Now())
	m := &model{}
	err := DecodeEntity(*ent, m)
	if err != nil {
		t.Error("Error converting compatible types: ", err)
	}

	// 3. Test with invalid types
	ent.Set("float", "asdfasdf")
	if nil == DecodeEntity(*ent, m) {
		t.Error("Invalid mapping success")
	}

}
