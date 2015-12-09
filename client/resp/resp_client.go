package resp

import (
	"github.com/EverythingMe/meduza/client"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/protocol"
	"github.com/EverythingMe/meduza/transport"
	"github.com/dvirsky/go-pylog/logging"
	redigo "github.com/garyburd/redigo/redis"
)

// Client wraps a connection to the server and the protocol used
type Client struct {
	conn  redigo.Conn
	proto protocol.Protocol
}

// NewClientConn creates a client from an existing redis connection
func NewClient(proto protocol.Protocol, conn redigo.Conn) *Client {

	return &Client{
		conn:  conn,
		proto: proto,
	}

}

// Dialer creates client objects from a
type Dialer struct {
	pool  *redigo.Pool
	Proto protocol.Protocol
}

func (d Dialer) Dial() (client.Client, error) {

	c := NewClient(d.Proto, d.pool.Get())
	return c, nil
}

func NewDialer(proto protocol.Protocol, addr string) Dialer {
	return Dialer{
		pool: redigo.NewPool(func() (redigo.Conn, error) {
			return redigo.Dial("tcp", addr)
		}, 4),
		Proto: proto,
	}
}

// Do sends a query to the server and receives its response
// Returns an error if we could not send the message
func (c *Client) Do(query interface{}) (interface{}, error) {

	// TODO: make sure it's a real query message - the proto will allow responses as well, and we don't want that

	msg, err := c.proto.WriteMessage(query)
	if err != nil {
		return nil, errors.NewError("Could not send query: %s", err)
	}

	if msg, err = c.roundtrip(msg); err != nil {
		logging.Error("Could not roundtrip: %s", errors.Sprint(err))
		return nil, err
	}

	return c.proto.ReadMessage(msg)

}

func (c *Client) roundtrip(msg transport.Message) (transport.Message, error) {

	var ret transport.Message
	vals, err := redigo.Values(c.conn.Do(string(msg.Type), msg.Body))
	if err != nil {
		return ret, errors.NewError("Error receiving message: %s", err)
	}

	if len(vals) != 2 {
		return ret, errors.NewError("Invalid response read, expected 2 elements, got %d", len(vals))
	}

	msgType, ok := vals[0].([]byte)
	if !ok {
		return ret, errors.NewError("Invalid response: %v", vals[0])
	}

	data, ok := vals[1].([]byte)
	if !ok {
		return ret, errors.NewError("Invalid response data: %v", vals[1])
	}

	ret = transport.Message{
		Type: transport.MessageType(msgType),
		Body: data,
	}

	return ret, nil
}
