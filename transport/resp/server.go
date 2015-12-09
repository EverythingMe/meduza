package resp

import (
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime/debug"

	"github.com/dvirsky/go-pylog/logging"
	"gitlab.doit9.com/backend/instrument"
	"github.com/EverythingMe/meduza/driver"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/protocol"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/transport"
)

type Server struct {
	listener   net.Listener
	numClients uint
	isRunning  bool
	driver     driver.Driver
	proto      protocol.Protocol
}

func NewServer(d driver.Driver, p protocol.Protocol) *Server {
	return &Server{
		driver: d,
		proto:  p,
	}
}

func (r *Server) Listen(addr string) error {

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	logging.Info("Redis adapter listening on %s", addr)
	r.listener = listener
	r.isRunning = true

	for {
		conn, err := r.listener.Accept()
		if err != nil {
			logging.Error("Error accepting: %s", err)
			return err
		}
		logging.Info("Handling connection from", conn.RemoteAddr())
		go r.handleConnection(conn)
	}

	return nil
}
func (r *Server) handleConnection(c net.Conn) {

	defer func() {
		err := recover()
		if err != nil {

			logging.Error("PANIC hanlding request: %s. Stack: %s", err, string(debug.Stack()))

			instrument.Increment("handler_panic", 1)
			func() {
				defer recover()
				c.Close()
			}()
		}

	}()

	instrument.Hit("connection_rcv")

	trans := NewTransport(c)

	var err error = nil
	var msg transport.Message
	for err == nil {
		if msg, err = trans.ReadMessage(); err == nil {

			// query handling logic
			var q interface{}
			var res query.QueryResult
			if q, err = r.proto.ReadMessage(msg); err == nil {

				// answering ping/pong messages is out of band and does not get transfered to the drivers
				if msg.Type == transport.PingMessage {
					logging.Debug("Got ping message, writing PONG")
					res, _ := r.proto.WriteMessage(query.NewPingResponse())
					trans.WriteMessage(res)
					continue
				}

				instrument.Profile(fmt.Sprintf("query.%s", msg.Type), func() error {
					res = r.handleQuery(q)

					return res.Err()
				})

				logging.Info("Query result: %s", res)

				if msg, err = r.proto.WriteMessage(res); err == nil {
					err = trans.WriteMessage(msg)
				} else {
					instrument.Increment("send_error", 1)
					logging.Error("Error serializing response: %s", err)
				}

			} else {
				instrument.Increment("deserialize_error", 1)
				logging.Error("Error deserializing query: %s", err)
			}
		} else {

			if err == io.EOF || err == io.ErrClosedPipe {
				break
			}
			instrument.Increment("receive_error", 1)
			logging.Error("Error reading from redis transport: %s", err)
		}

	}

	logging.Debug("Exiting handler loop")
	c.Close()

}

func (r *Server) handleQuery(qu interface{}) query.QueryResult {

	switch q := qu.(type) {
	case query.PutQuery:
		if err := q.Validate(); err != nil {
			logging.Error("Error validating put query: %s", err)
			return query.NewPutResponse(err)
		}
		return r.driver.Put(q)
	case query.GetQuery:

		if err := q.Validate(); err != nil {
			logging.Error("Error validating GET query: %s", err)
			return query.NewGetResponse(err)
		}
		return r.driver.Get(q)
	case query.UpdateQuery:
		if err := q.Validate(); err != nil {
			logging.Error("Error validating UPDATE query: %s", err)
			return query.NewUpdateResponse(err, 0)
		}
		return r.driver.Update(q)
	case query.DelQuery:
		if err := q.Validate(); err != nil {
			logging.Error("Error validating DEL query: %s", err)
			return query.NewDelResponse(err, 0)
		}
		return r.driver.Delete(q)
	default:
		return query.NewResponse(errors.NewError("Invalid query type object %s", reflect.TypeOf(q)))
	}
}
