package resp

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/EverythingMe/meduza/driver/mock"
	"github.com/EverythingMe/meduza/protocol/bson"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/transport"
)

func TestTransport(t *testing.T) {

	buf := bytes.NewBuffer(nil)

	trans := NewTransport(buf)

	msg := transport.Message{
		Type: transport.GetMessage,
		Body: []byte("foo"),
	}

	err := trans.WriteMessage(msg)
	if err != nil {
		t.Error("Failed writing to transport:", err)
	}

	if buf.Bytes() == nil || len(buf.Bytes()) < 10 {
		t.Error("Invalid buf bytes: ", buf.Bytes())
	}

	msg2, err := trans.ReadMessage()
	if err != nil {
		t.Error("Error reading from transport: ", err)
	}
	fmt.Println(msg2, msg)

	if !reflect.DeepEqual(msg, msg2) {
		t.Error("Incompatible response/request messages:", msg, msg2)
	}
}

func TestClientServer(t *testing.T) {
	proto := bson.BsonProtocol{}
	trans := Transport{}
	drv := mock.MockDriver{}
	_ = proto
	_ = trans
	_ = drv
	srv := NewServer(drv, proto)
	go func() {
		e := srv.Listen(":9977")
		if e != nil {
			t.Fatal("Could not start server")
		}
	}()

	maxRetries := 10
	var conn net.Conn
	var err error
	for i := 0; i < maxRetries; i++ {

		conn, err = net.Dial("tcp", "localhost:9977")
		if err != nil {
			if i == maxRetries-1 {
				t.Fatal("Could not connect to server: %s", err)
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
	}
	client := NewTransport(conn)

	q := query.NewGetQuery("Users").Filter("name", query.In, "User 0", "User 1").Page(0, 10)
	msg, err := proto.WriteMessage(q)
	if err != nil {
		t.Error("Could not serialize message:", err)
	}

	if err := client.WriteMessage(msg); err != nil {
		t.Error("Could not write message:", err)
	}

	if msg, err = client.ReadMessage(); err != nil {
		t.Error("Could not read message:", err)
	}

	res, err := proto.ReadMessage(msg)
	if err != nil {
		t.Error("Could not deserialize message:", err)
	}

	fmt.Println(res)
}
