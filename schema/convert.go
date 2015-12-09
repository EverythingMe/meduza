package schema

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/EverythingMe/meduza/errors"
)

type Decoder interface {
	Decode(value []byte, tp ColumnType) (interface{}, error)
}

type Encoder interface {
	Encode(value interface{}) ([]byte, error)
}

type Transcoder interface {
	Transcode([]byte, ColumnType) ([]byte, error)
}

// RawDecoder taekes a raw byte array and type information and decodes them
type RawDecoder struct {
}

// ConvertInt makes sure the input is indeed an integer and just returns it as is
func (RawDecoder) decodeInt(v []byte) (Int, error) {

	if i, e := strconv.ParseInt(string(v), 10, 64); e != nil {
		return 0, e
	} else {
		return Int(i), nil
	}

}

// ConvertInt makes sure the input is indeed an integer and just returns it as is
func (RawDecoder) decodeUint(v []byte) (Uint, error) {

	if i, e := strconv.ParseUint(string(v), 10, 64); e != nil {
		return 0, e
	} else {
		return Uint(i), nil
	}

}

// ConvertInt makes sure the input is indeed an integer and just returns it as is
func (RawDecoder) decodeFloat(v []byte) (Float, error) {

	if f, e := strconv.ParseFloat(string(v), 64); e != nil {
		return 0, e
	} else {
		return Float(f), nil
	}
}

// ConvertBool checks that a value is indeed a boolean and returns either "0" or "1"
func (RawDecoder) decodeBool(v []byte) (Bool, error) {
	s := strings.ToLower(string(v))

	switch s {
	case "1", "true":
		return Bool(true), nil
	case "0", "false":
		return Bool(false), nil
	default:
		return Bool(false), fmt.Errorf("Invalid boolean argument: %s", s)
	}
}

func (RawDecoder) decodeText(v []byte) (Text, error) {
	return Text(v), nil
}

func (RawDecoder) decodeBinary(v []byte) (Binary, error) {
	return Binary(v), nil
}

// ConvertTimestamp takesa unix timestamp and returns it as is. If the value is @now it returns the current unix timestamp
// Override this for specific databases
func (d RawDecoder) decodeTimestamp(v []byte) (Timestamp, error) {

	i, e := d.decodeInt(v)
	return Timestamp(time.Unix(int64(i), 0)), e

}

func (d RawDecoder) Decode(value []byte, tp ColumnType) (interface{}, error) {

	switch tp {
	case IntType:
		return d.decodeInt(value)
	case UintType:
		return d.decodeUint(value)
	case FloatType:
		return d.decodeFloat(value)
	case BoolType:
		return d.decodeBool(value)
	case TextType:
		return d.decodeText(value)
	case BinaryType:
		return d.decodeBinary(value)
	case TimestampType:
		return d.decodeTimestamp(value)

	}

	return nil, errors.NewError("Unknown type: %s", tp)

}

type RawEncoder struct{}

func (c RawEncoder) encodeInt(i Int) []byte {
	return []byte(strconv.FormatInt(int64(i), 10))
}
func (c RawEncoder) encodeUint(i Uint) []byte {
	return []byte(strconv.FormatUint(uint64(i), 10))
}

func (c RawEncoder) encodeBool(b Bool) []byte {

	if b {
		return []byte("1")
	} else {
		return []byte("0")
	}
}

func (c RawEncoder) encodeFloat(f Float) []byte {
	return []byte(strconv.FormatFloat(float64(f), 'f', -1, 64))
}

func (c RawEncoder) encodeText(v Text) []byte {
	return []byte(v)
}

func (c RawEncoder) encodeBinary(v Binary) []byte {
	return v
}

func (c RawEncoder) encodeTimestamp(t Timestamp) []byte {
	return []byte(strconv.FormatInt(time.Time(t).Unix(), 10))
}

func (c RawEncoder) Encode(v interface{}) ([]byte, error) {

	switch val := v.(type) {
	case Int:
		return c.encodeInt(val), nil
	case Uint:
		return c.encodeUint(val), nil
	case Float:
		return c.encodeFloat(val), nil
	case Bool:
		return c.encodeBool(val), nil
	case Text:
		return c.encodeText(val), nil
	case Binary:
		return c.encodeBinary(val), nil
	case Timestamp:
		return c.encodeTimestamp(val), nil
	case nil:
		return nil, nil
	}

	return nil, errors.NewError("Unsupported type: %s", reflect.TypeOf(v))

}
