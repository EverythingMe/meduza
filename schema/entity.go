package schema

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/EverythingMe/bson/bson"
	"github.com/dvirsky/go-pylog/logging"
	"github.com/EverythingMe/meduza/errors"
)

type Property struct {
	Name  string      `bson:"name"`
	Value interface{} `bson:"value"`
}

func (p *Property) UnmarshalBson(buf *bytes.Buffer, kind byte) {

	bson.Next(buf, 4)
	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		key := bson.ReadCString(buf)

		if key == "name" {
			p.Name = string(bson.DecodeString(buf, kind))
		} else if key == "value" {
			p.Value = unmarshalBsonInternal(buf, kind)
		}

	}

}

func (p Property) String() string {
	return fmt.Sprintf("Property(%s: %v (%v))", p.Name, p.Value, reflect.TypeOf(p.Value))
}

type PropertyMap map[string]interface{}

type Entity struct {
	Id         Key           `bson:"id"`
	Properties PropertyMap   `bson:"properties"`
	TTL        time.Duration `bson:"ttl"`
}

func (e Entity) Validate() error {

	for k, v := range e.Properties {
		if k == "" {
			return errors.NewError("Empty properties not allowed in entities")
		}

		if v == nil {
			continue
		}
		switch v.(type) {
		case Int, Uint, Float, Bool, Text, Binary, Timestamp, Set, List, Map:
			//		case Map:
			//			return errors.NewError("Type %d not yet implemented :(", reflect.TypeOf(v))
		default:
			return errors.NewError("Type %d not allowed for entities", reflect.TypeOf(v))
		}
	}

	return nil

}

//func (e Entity) String() string {
//	return fmt.Sprintf("Entity<%s>: %s\n", e.Id, e.Properties)
//}

func NewEntity(id Key, properties ...Property) *Entity {

	props := make(PropertyMap)
	for _, prop := range properties {

		val, err := InternalType(prop.Value)
		if err != nil {
			logging.Error("Could not convert %s to internal type", reflect.TypeOf(prop.Value))
			val = prop.Value
		}
		props[prop.Name] = val
	}

	return &Entity{
		Id:         id,
		Properties: props,
	}

}

func unmarshalBsonInternal(buf *bytes.Buffer, kind byte) interface{} {

	if kind == bson.Null {
		return nil
	}

	switch kind {
	case bson.Null:
		return nil
	case bson.Boolean:
		return Bool(bson.DecodeBool(buf, kind))
	case bson.Int:
		return Int(bson.DecodeInt32(buf, kind))
	case bson.Long:
		return Int(bson.DecodeInt64(buf, kind))
	case bson.Ulong:
		return Uint(bson.DecodeUint64(buf, kind))
	case bson.Number:
		return Float(bson.DecodeFloat64(buf, kind))
	case bson.String:
		return Text(bson.DecodeString(buf, kind))
	case bson.Binary:
		return Binary(bson.DecodeBinary(buf, kind))
	case bson.Datetime:
		return Timestamp(bson.DecodeTime(buf, kind))
	case bson.Object:
		ret := NewMap()
		for k, v := range bson.DecodeMap(buf, kind) {
			ret.Set(k, v)
		}
		return ret
	case bson.Array:
		return decodeBsonArray(buf, kind)

	}
	logging.Error("Unknown BSON type: %x", kind)
	return nil
}

func decodeBsonArray(buf *bytes.Buffer, kind byte) interface{} {

	l := bson.DecodeArray(buf, kind)

	if len(l) == 0 {
		return nil
	}
	logging.Debug("Decoding bson array, first element %v", l[0])
	var ident string = ""

	switch x := l[0].(type) {
	case string:
		ident = x
	case []byte:
		ident = string(x)
	}

	switch ident {
	case SetBson:
		return NewSet(l[1:]...)
	case ListBson:
		return NewList(l[1:]...)
	default:
		return NewList(l...)
	}
}

// UnmarshalBson overrieds the default unmarshaling of a map, to allow us to extract our internal types
func (p PropertyMap) UnmarshalBson(buf *bytes.Buffer, kind byte) {

	//result := make(map[string]interface{})
	bson.Next(buf, 4)
	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		key := bson.ReadCString(buf)

		p[key] = unmarshalBsonInternal(buf, kind)

	}

}

func NewText(k, v string) Property {
	return Property{
		Name:  k,
		Value: Text(v),
	}
}

func NewInt(k string, v int64) Property {
	return Property{
		Name:  k,
		Value: Int(v),
	}
}

func (e *Entity) SetId(id Key) {
	e.Id = id
}

func (e *Entity) Expire(ttl time.Duration) *Entity {
	e.TTL = ttl
	return e
}

func (e *Entity) Set(name string, value interface{}) *Entity {
	val, err := InternalType(value)
	if err != nil {
		logging.Error("Could not convert %s to internal type", reflect.TypeOf(val))
		val = value
	}
	e.Properties[name] = val
	return e
}

func (e *Entity) Get(property string) (interface{}, bool) {
	v, f := e.Properties[property]
	return v, f
}
