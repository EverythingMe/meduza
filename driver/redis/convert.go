package redis

import (
	"bytes"
	"compress/lzw"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/EverythingMe/bson/bson"
	"github.com/dvirsky/go-pylog/logging"
	"github.com/golang/snappy"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/schema"
)

func prepend(prefix []byte, b []byte) []byte {
	return append(prefix, b...)
}

const (
	IntPrefix                  = 'i'
	UintPrefix                 = 'u'
	FloatPrefix                = 'f'
	TextPrefix                 = 'x'
	CompressedTextPrefix       = 'z'
	CompressedTextPrefixSnappy = 'Z'
	BoolPrefix                 = 'b'
	TimestampPrefix            = 't'
	BinaryPrefix               = 'r'
	SetPrefix                  = 's'
	ListPrefix                 = 'l'
	NilPrefix                  = 'N'
	MapPrefix                  = 'm'
)

type Encoder struct {
	TextCompressThreshold int
}

func NewEncoder(compressThreshold int) Encoder {
	return Encoder{
		TextCompressThreshold: compressThreshold,
	}
}

func (c Encoder) encodeInt(i interface{}) []byte {
	return []byte(fmt.Sprintf("%d", i))
}
func (c Encoder) encodeUint(i interface{}) []byte {
	return []byte(fmt.Sprintf("%d", i))
}

func (c Encoder) encodeBool(b schema.Bool) []byte {

	if b {
		return []byte{BoolPrefix, '1'}
	} else {
		return []byte{BoolPrefix, '0'}
	}
}

func (c Encoder) encodeFloat(f interface{}) []byte {
	return []byte(fmt.Sprintf("%f", f))
}

func (c Encoder) encodeText(v schema.Text) []byte {

	if c.TextCompressThreshold > 0 && len(v) >= c.TextCompressThreshold {
		logging.Debug("Encoding text as compressed, len %d", len(v))
		return c.encodeCompressedText(v)
	}

	return append([]byte{TextPrefix}, []byte(v)...)
}

const lzwLitWidth = 8

func (c Encoder) encodeCompressedText(v schema.Text) []byte {

	buf := bufferPool.Get().([]byte)
	b := snappy.Encode(bufferPool.Get().([]byte), []byte(v))
	b = append([]byte{CompressedTextPrefixSnappy}, b...)
	bufferPool.Put(buf)
	return b

}

func (c Encoder) encodeCompressedTextLZW(v schema.Text) []byte {

	//	b, _ := lz4.Encode(bufferPool.Get().([]byte), []byte(v))
	//	return append([]byte{CompressedTextPrefix}, b...)

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(CompressedTextPrefix)

	w := lzw.NewWriter(buf, lzw.LSB, lzwLitWidth)

	_, err := w.Write([]byte(v))
	w.Close()

	if err == nil {
		return buf.Bytes()
	}
	return nil

}

func (c Encoder) encodeBinary(v schema.Binary) []byte {
	return append([]byte{BinaryPrefix}, []byte(v)...)
}

func (c Encoder) encodeTimestamp(t schema.Timestamp) []byte {
	logging.Info("%#v", time.Time(t), time.Time(t).IsZero())
	if time.Time(t).IsZero() {
		return []byte{}
	}
	return append([]byte{TimestampPrefix}, []byte(strconv.FormatInt(time.Time(t).Unix(), 10))...)
}

func (c Encoder) encodeSet(s schema.Set) ([]byte, error) {

	lst := make([]interface{}, len(s))
	i := 0
	for k := range s {
		lst[i] = k
		i++
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(SetPrefix)
	if err := bson.MarshalToStream(buf, lst); err != nil {
		return nil, errors.NewError("Could not encode set: %s", err)
	}

	return buf.Bytes(), nil

}

func (c Encoder) encodeMap(s schema.Map) ([]byte, error) {

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(MapPrefix)
	if err := bson.MarshalToStream(buf, s); err != nil {
		return nil, errors.NewError("Could not encode set: %s", err)
	}

	return buf.Bytes(), nil

}

func (c Encoder) encodeList(l schema.List) ([]byte, error) {

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(ListPrefix)
	if err := bson.MarshalToStream(buf, []interface{}(l)); err != nil {
		return nil, errors.NewError("Could not encode set: %s", err)
	}

	return buf.Bytes(), nil

}

func (c Encoder) Encode(v interface{}) ([]byte, error) {
	if v == nil {
		return []byte{NilPrefix}, nil
	}

	switch val := v.(type) {
	case schema.Int, int, int32, int64:
		return c.encodeInt(val), nil
	case schema.Uint, uint, uint32, uint64:
		return c.encodeUint(val), nil
	case schema.Float, float64, float32:
		return c.encodeFloat(val), nil
	case schema.Bool:
		return c.encodeBool(val), nil
	case bool:
		return c.encodeBool(schema.Bool(val)), nil
	case schema.Text:
		return c.encodeText(val), nil
	case string:
		return c.encodeText(schema.Text(val)), nil
	case schema.Binary:
		return c.encodeBinary(val), nil
	case schema.Timestamp:
		return c.encodeTimestamp(val), nil
	case schema.Set:
		return c.encodeSet(val)
	case schema.List:
		return c.encodeList(val)
	case schema.Map:
		return c.encodeMap(val)
	}

	return nil, errors.NewError("Unsupported type: %s", reflect.TypeOf(v))

}

type Decoder struct {
}

// ConvertInt makes sure the input is indeed an integer and just returns it as is
func (d Decoder) decodeInt(v []byte) (schema.Int, error) {

	if i, e := strconv.ParseInt(string(v), 10, 64); e != nil {
		return 0, e
	} else {
		return schema.Int(i), nil
	}

}

// ConvertInt makes sure the input is indeed an integer and just returns it as is
func (d Decoder) decodeUint(v []byte) (schema.Uint, error) {

	if i, e := strconv.ParseUint(string(v), 10, 64); e != nil {
		return 0, e
	} else {
		return schema.Uint(i), nil
	}

}

// ConvertInt makes sure the input is indeed an integer and just returns it as is
func (d Decoder) decodeFloat(v []byte) (schema.Float, error) {

	if f, e := strconv.ParseFloat(string(v), 64); e != nil {
		return 0, e
	} else {
		return schema.Float(f), nil
	}
}

// ConvertBool checks that a value is indeed a boolean and returns either "0" or "1"
func (Decoder) decodeBool(v []byte) (schema.Bool, error) {
	s := strings.ToLower(string(v))

	switch s {
	case "1", "true":
		return schema.Bool(true), nil
	case "0", "false":
		return schema.Bool(false), nil
	default:
		return schema.Bool(false), fmt.Errorf("Invalid boolean argument: %s", s)
	}
}

func (Decoder) decodeText(v []byte) (schema.Text, error) {
	return schema.Text(v), nil
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096)
	},
}

func (Decoder) decodeCompressedText(v []byte) (schema.Text, error) {

	buf := bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)
	decomp, err := snappy.Decode(buf, v)
	if err != nil {
		return "", err
	}

	return schema.Text(decomp), nil
}

func (Decoder) decodeCompressedTextLZW(v []byte) (schema.Text, error) {

	buf := bytes.NewReader(v)
	r := lzw.NewReader(buf, lzw.LSB, lzwLitWidth)

	w := bytes.NewBuffer(nil)
	tmp := bufferPool.Get().([]byte)
	defer bufferPool.Put(tmp)
	for {
		n, err := r.Read(tmp)
		if n == 0 || err == io.EOF {
			break
		}

		if err != nil && err != io.EOF {
			return "", err
		}

		w.Write(tmp[:n])

	}

	r.Close()

	return schema.Text(w.Bytes()), nil

}

func (Decoder) decodeBinary(v []byte) (schema.Binary, error) {
	return schema.Binary(v), nil
}

// ConvertTimestamp takesa unix timestamp and returns it as is. If the value is @now it returns the current unix timestamp
// Override this for specific databases
func (m Decoder) decodeTimestamp(v []byte) (schema.Timestamp, error) {

	i, e := m.decodeInt(v)
	return schema.Timestamp(time.Unix(int64(i), 0)), e

}

func (m Decoder) decodeSet(v []byte) (s schema.Set, err error) {
	return schema.NewSet(bson.DecodeArray(bytes.NewBuffer(v), bson.Array)...), nil
}

func (m Decoder) decodeMap(v []byte) (sm schema.Map, err error) {

	sm = schema.NewMap()
	mp := bson.DecodeMap(bytes.NewBuffer(v), bson.Object)
	for k, v := range mp {
		sm.Set(k, v)
	}
	return sm, nil
}

func (m Decoder) decodeList(v []byte) (s schema.List, err error) {

	ret := schema.NewList(bson.DecodeArray(bytes.NewBuffer(v), bson.Array)...)

	return ret, nil

}

var intRegex = regexp.MustCompile("^-?[0-9]+$")
var floatRegex = regexp.MustCompile("^[-]?[0-9]+\\.[0-9]+$")

// decodeNumbers decodes a number without knowing its exact type, by trying int, uint, float in order.
// Since we always write floats with a decimal point, and Go refuses to parse them as ints, this is safe.
// Uints are deocded only
func (d Decoder) decodeNumber(num string) (ret interface{}, err error) {

	if ret, err = strconv.ParseInt(num, 10, 64); err == nil {
		return schema.InternalType(ret)
	}

	if ret, err = strconv.ParseUint(num, 10, 64); err == nil {
		return schema.InternalType(ret)
	}

	if ret, err = strconv.ParseFloat(num, 64); err == nil {
		return schema.InternalType(ret)

	}

	err = errors.NewError("Invalid number format: %s", num)
	return

}

func (d Decoder) Decode(data []byte, t schema.ColumnType) (interface{}, error) {

	if data == nil || data[0] == NilPrefix {
		return nil, nil
	}

	prefix := data[0]
	value := data[1:]

	switch prefix {
	case IntPrefix:
		return d.decodeInt(value)
	case UintPrefix:
		return d.decodeUint(value)
	case FloatPrefix:
		return d.decodeFloat(value)
	case BoolPrefix:
		return d.decodeBool(value)
	case TextPrefix:
		return d.decodeText(value)
	case CompressedTextPrefix:
		return d.decodeCompressedTextLZW(value)
	case CompressedTextPrefixSnappy:
		return d.decodeCompressedText(value)

	case BinaryPrefix:
		return d.decodeBinary(value)
	case TimestampPrefix:
		return d.decodeTimestamp(value)
	case SetPrefix:
		return d.decodeSet(value)
	case ListPrefix:
		return d.decodeList(value)
	case MapPrefix:
		return d.decodeMap(value)
	default:

		// numbers do not have a prefix so they can be incremented automatically
		if (prefix >= '0' && prefix <= '9') || prefix == '-' {
			return d.decodeNumber(string(data))
		}

		return nil, errors.NewError("Invalid type prefix: %s", prefix)
	}

}
