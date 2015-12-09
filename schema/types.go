package schema

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/EverythingMe/bson/bson"
	"github.com/EverythingMe/bson/bytes2"
	"github.com/dvirsky/go-pylog/logging"
	"github.com/EverythingMe/meduza/errors"
)

type Value interface {
}

// data types
const (
	IntType       ColumnType = "Int"
	UintType      ColumnType = "Uint"
	FloatType     ColumnType = "Float"
	TextType      ColumnType = "Text"
	BoolType      ColumnType = "Bool"
	TimestampType ColumnType = "Timestamp"
	BinaryType    ColumnType = "Binary"
	SetType       ColumnType = "Set"
	ListType      ColumnType = "List"
	MapType       ColumnType = "Map"
	UnknownType   ColumnType = ""
)

const NilValue = "{NIL}"

type (
	Int       int64 //
	Uint      uint64
	Float     float64 //
	Text      string  //
	Bool      bool    //
	Timestamp time.Time
	Binary    []byte
	Set       map[interface{}]struct{}
	Map       map[string]interface{} //
	List      []interface{}          //

)

// TypeOf returns the ColumnType representation of an internal data type variable.
// If the variable is not of any internal type, we return UnknownType
func TypeOf(v interface{}) ColumnType {

	switch v.(type) {
	case Int:
		return IntType
	case Uint:
		return UintType
	case Float:
		return FloatType
	case Text:
		return TextType
	case Binary:
		return BinaryType
	case Bool:
		return BoolType
	case Timestamp:
		return TimestampType
	case Set:
		return SetType
	case Map:
		return MapType
	case List:
		return ListType

	}

	return UnknownType
}

// InternalType takes an arbitrary object and maps it to the internal db type matching it
func InternalType(v interface{}) (interface{}, error) {

	if v == nil {
		return nil, nil
	}
	//fmt.Println(reflect.TypeOf(v), v)
	switch val := v.(type) {
	case Int, Float, Uint, Bool, Text, Binary, Timestamp, Map, List, Set, Key:
		return v, nil
	case string:
		return Text(val), nil
	case int:
		return Int(val), nil
	case time.Time:
		return Timestamp(val), nil

	case int32:
		return Int(val), nil
	case int64:
		return Int(val), nil
	case uint:
		return Uint(val), nil
	case uint32:
		return Uint(val), nil
	case uint64:
		return Uint(val), nil
	case float32:
		return Float(val), nil
	case float64:
		return Float(val), nil
	case bool:
		return Bool(val), nil
	case []byte:
		return Binary(val), nil
	case []interface{}:
		return NewList(val...), nil
	case map[interface{}]struct{}:
		return NewSetFromMap(val), nil
	case map[string]interface{}:
		return Map(val), nil
		//TODO: handle slice/map/set
	}

	return nil, errors.NewError("Unconvertible type %s", reflect.TypeOf(v))

}

// MarshalBson encodes a timestamp property as a Bson time
func (t Timestamp) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeTime(buf, key, time.Time(t))
}

// MarshalBson encodes a timestamp property as a Bson time
func (t Timestamp) MarshalJSON() ([]byte, error) {

	if time.Time(t).IsZero() {
		return []byte{'"', '"'}, nil
	}
	return []byte(fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02T15:04:05.00"))), nil

}

// GobEncode implements the gob.GobEncoder interface.
func (t Timestamp) GobEncode() ([]byte, error) {

	return time.Time(t).GobEncode()
}

// GobDecode implements the gob.GobDecoder interface.
func (t *Timestamp) GobDecode(data []byte) error {

	tt := &time.Time{}
	if err := tt.GobDecode(data); err != nil {
		return err
	}

	*t = Timestamp(*tt)
	return nil
}

// MarshalBson overrides the vitess library's marshaling of strings that
// encodes strings as binary
func (t Text) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodePrefix(buf, bson.String, key)
	binary.LittleEndian.PutUint32(buf.Reserve(4), uint32(len(t)+1))
	buf.WriteString(string(t))
	buf.WriteByte(0)
}

const SetBson = "__MDZS__"
const ListBson = "__MDZL__"

// MarshalBson overrides the vitess library's marshaling for set objects
func (s Set) MarshalBson(buf *bytes2.ChunkedWriter, key string) {

	lst := make([]interface{}, len(s)+1)
	lst[0] = SetBson
	i := 1
	for k := range s {
		lst[i] = k
		i++
	}
	encodeSlice(buf, key, lst)
}

// MarshalBson encodes a timestamp property as a Bson time
func (s Set) MarshalJSON() ([]byte, error) {

	lst := make([]interface{}, 0, len(s))
	for k := range s {
		lst = append(lst, k)
	}

	return json.Marshal(lst)

}

func encodeSlice(buf *bytes2.ChunkedWriter, key string, lst []interface{}) {
	bson.EncodePrefix(buf, bson.Array, key)
	encodeSliceContent(buf, lst)
}

func encodeSliceContent(buf *bytes2.ChunkedWriter, lst []interface{}) {
	lenWriter := bson.NewLenWriter(buf)
	for i, elem := range lst {
		bson.EncodeField(buf, bson.Itoa(i), elem)
	}
	lenWriter.Close()
}

// NewSet creates a new set from a list of elements
func NewSet(elements ...interface{}) Set {
	s := make(Set)
	for _, e := range elements {
		v, err := InternalType(e)
		if err != nil {
			logging.Error("Error initializing set: %s", err)
			continue
		}

		s[v] = struct{}{}
	}

	//logging.Debug("Created new set: %s", s)
	return s
}

func (s Set) Add(e interface{}) {
	s[e] = struct{}{}
}

func NewSetFromMap(m map[interface{}]struct{}) Set {
	elements := make([]interface{}, 0, len(m))
	for k := range m {
		elements = append(elements, k)
	}
	return NewSet(elements...)
}

func (l List) MarshalBson(buf *bytes2.ChunkedWriter, key string) {

	encodeSlice(buf, key, append([]interface{}{ListBson}, l...))

}

func NewList(elements ...interface{}) List {

	var err error
	for i := range elements {
		elements[i], err = InternalType(elements[i])
		if err != nil {
			logging.Error("Error converting %v: %s", elements[i], err)
		}
	}

	return List(elements)
}

func (m Map) Set(k string, val interface{}) Map {
	v, e := InternalType(val)
	if e != nil {
		logging.Error("Error converting %v: %s", v, e)
	} else {
		m[k] = v
	}

	return m
}

func NewMap() Map {
	return make(Map)
}

// UnmarshalBson overrieds the default unmarshaling of a map, to allow us to extract our internal types
func (m Map) UnmarshalBson(buf *bytes.Buffer, kind byte) {

	//result := make(map[string]interface{})
	bson.Next(buf, 4)
	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {

		key := bson.ReadCString(buf)
		fmt.Println(kind, key)
		m[key] = unmarshalBsonInternal(buf, kind)

	}

}
