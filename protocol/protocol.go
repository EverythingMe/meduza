package protocol

import (
	"github.com/EverythingMe/meduza/transport"
)

// Protocol is in charge of serialization/deserialization of messages
type Protocol interface {
	ReadMessage(transport.Message) (interface{}, error)
	WriteMessage(interface{}) (transport.Message, error)
}
