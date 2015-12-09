package bson

import (
	"reflect"

	"github.com/EverythingMe/bson/bson"
	"github.com/dvirsky/go-pylog/logging"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/transport"
)

type BsonProtocol struct {
}

func read(msg transport.Message, v interface{}) error {
	if err := bson.Unmarshal(msg.Body, v); err != nil {
		return logging.Errorf("Could not unmarshal %s: %s", reflect.TypeOf(v), err)
	}
	return nil
}

func newMessage(v interface{}, t transport.MessageType) (transport.Message, error) {

	b, err := bson.Marshal(v)

	if err != nil {
		return transport.Message{}, logging.Errorf("Could not marshal %s to message: %s", reflect.TypeOf(v), err)
	}

	return transport.Message{
		Type: t,
		Body: b,
	}, nil

}
func (BsonProtocol) readGetQuery(msg transport.Message) (ret query.GetQuery, err error) {
	err = read(msg, &ret)
	return
}

func (BsonProtocol) readUpdateQuery(msg transport.Message) (ret query.UpdateQuery, err error) {
	err = read(msg, &ret)
	return
}

func (BsonProtocol) readPutQuery(msg transport.Message) (ret query.PutQuery, err error) {
	err = read(msg, &ret)
	return
}

func (BsonProtocol) readDelQuery(msg transport.Message) (ret query.DelQuery, err error) {
	err = read(msg, &ret)
	return
}
func (BsonProtocol) readGetResponse(msg transport.Message) (ret query.GetResponse, err error) {
	err = read(msg, &ret)
	return
}

func (BsonProtocol) readUpdateResponse(msg transport.Message) (ret query.UpdateResponse, err error) {
	err = read(msg, &ret)
	return
}

func (BsonProtocol) readAddResponse(msg transport.Message) (ret query.PutResponse, err error) {
	err = read(msg, &ret)
	return
}
func (BsonProtocol) readDelResponse(msg transport.Message) (ret query.DelResponse, err error) {
	err = read(msg, &ret)
	return
}
func (BsonProtocol) readPingResponse(msg transport.Message) (ret query.PingResponse, err error) {
	err = read(msg, &ret)
	return
}

// ReadMessage accepts a transport message, and according to its type, tries to deserialize it into
// a request or response object
func (p BsonProtocol) ReadMessage(msg transport.Message) (ret interface{}, err error) {

	switch msg.Type {
	case transport.GetMessage:
		ret, err = p.readGetQuery(msg)
	case transport.UpdateMessage:
		ret, err = p.readUpdateQuery(msg)
	case transport.PutMessage:
		ret, err = p.readPutQuery(msg)
	case transport.DelMessage:
		ret, err = p.readDelQuery(msg)
	case transport.PingMessage:
		ret, err = query.PingQuery{}, nil

	case transport.GetResponseMessage:
		ret, err = p.readGetResponse(msg)
	case transport.UpdateResponseMessage:
		ret, err = p.readUpdateResponse(msg)
	case transport.PutResponseMessage:
		ret, err = p.readAddResponse(msg)
	case transport.DelResponseMessage:
		ret, err = p.readDelResponse(msg)
	case transport.PingResponseMessage:
		ret, err = p.readPingResponse(msg)
	default:
		ret, err = nil, logging.Errorf("Could not read message: message type '%s' invalid", msg.Type)
	}

	logging.Debug("Read message: %s", ret)
	return

}

// WriteMessage takes a request or response object and serializes it into a transport message to be sent to a transport
func (BsonProtocol) WriteMessage(v interface{}) (msg transport.Message, err error) {

	// make sure that if we're talking about a pointer, we cast to its value
	// before we select on a type
	val := v
	if reflect.TypeOf(v).Kind() == reflect.Ptr {
		val = reflect.ValueOf(v).Elem().Interface()
	}

	switch val.(type) {
	case query.PutQuery:
		return newMessage(v, transport.PutMessage)
	case query.GetQuery:
		return newMessage(v, transport.GetMessage)
	case query.UpdateQuery:
		return newMessage(v, transport.UpdateMessage)
	case query.DelQuery:
		return newMessage(v, transport.DelMessage)
	case query.PingQuery:
		return newMessage(v, transport.PingMessage)
	case query.PutResponse:
		return newMessage(v, transport.PutResponseMessage)
	case query.GetResponse:
		return newMessage(v, transport.GetResponseMessage)
	case query.UpdateResponse:
		return newMessage(v, transport.UpdateResponseMessage)
	case query.DelResponse:
		return newMessage(v, transport.DelResponseMessage)
	case query.PingResponse:
		return newMessage(v, transport.PingResponseMessage)
	}

	return transport.Message{}, logging.Errorf("Invalid type for protocol serialization: %s", reflect.TypeOf(v))
}
