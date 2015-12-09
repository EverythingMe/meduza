package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

//var srv *disposable_redis.Server

func TestBatch(t *testing.T) {
	//t.SkipNow()
	conn, e := redis.Dial("tcp", srv.Addr())
	if e != nil {
		t.Fatal("Could not connect to server:", e)
	}

	var b *Batch = NewBatch(conn)

	val := fmt.Sprintf("Whatever %v", time.Now().UnixNano())

	p1, e := b.Send("SET", "foo", val)
	if e != nil {
		t.Fatal("Could not send batched command: ", e)
	}
	if p1 == nil {
		t.Fatal("Got a nil promise")
	}

	p2, e := b.Send("GET", "foo")
	if e != nil {
		t.Fatal("Could not send batched command: ", e)
	}
	if p2 == nil {
		t.Fatal("Got a nil promise")
	}

	results, e := b.Execute()
	if e != nil {
		t.Fatal("Error executing batch: ", e)
	}

	if len(results) != 2 {
		t.Fatal("Expected 2 results, got ", len(results))
	}

	if len(b.promises) != 0 {
		t.Fatal("Did not reset batch correctly")
	}

	if s, _ := redis.String(p1.Reply()); s != "OK" {
		t.Fatal("Invalid reply: ", s)
	}

	if s, _ := redis.String(p2.Reply()); s != val {
		t.Fatal("Invalid reply: ", s)
	}
}

func TestTransaction(t *testing.T) {

	//t.SkipNow()

	conn, e := redis.Dial("tcp", srv.Addr())
	if e != nil {
		t.Fatal("Could not connect to server:", e)
	}

	var b *Transaction
	b = NewTransaction(conn)

	val := fmt.Sprintf("Whatever %v", time.Now().UnixNano())

	p1, e := b.Send("SET", "foo", val)
	if e != nil {
		t.Fatal("Could not send batched command: ", e)
	}
	if p1 == nil {
		t.Fatal("Got a nil promise")
	}

	p2, e := b.Send("GET", "foo")
	if e != nil {
		t.Fatal("Could not send batched command: ", e)
	}
	if p2 == nil {
		t.Fatal("Got a nil promise")
	}

	results, e := b.Execute()
	if e != nil {
		t.Fatal("Error executing batch: ", e)
	}

	if len(results) != 2 {
		t.Fatal("Expected 2 results, got ", len(results))
	}

	if len(b.promises) != 0 {
		t.Fatal("Did not reset batch correctly")
	}

	if s, _ := redis.String(p1.Reply()); s != "OK" {
		t.Fatal("Invalid reply: ", s)
	}

	if s, _ := redis.String(p2.Reply()); s != val {
		t.Fatal("Invalid reply: ", s)
	}

	// now we check that aborting really does nothing
	p3, e := b.Send("SET", "foo", "not val")
	if e != nil {
		t.Fatal("Could not send batched command: ", e)
	}

	if e = b.Abort(); e != nil {
		t.Fatal("Could not abort transaction: ", e)
	}

	if p3.Value != nil {
		t.Fatal("Promise of aborted transaction has value")
	}

	conn, _ = redis.Dial("tcp", srv.Addr())
	b = NewTransaction(conn)

	p3, e = b.Send("GET", "foo")
	if e != nil {
		t.Fatal("Could not get value:", e)
	}
	if _, e = b.Execute(); e != nil {
		t.Error("Could not execute transaction", e)
	}

	if s, _ := redis.String(p3.Reply()); s != val {
		t.Fatal("Aborting transaction changed value to", s)
	}

}

func ExampleBatch() {

	return

	conn, e := redis.Dial("tcp", srv.Addr())
	if e != nil {
		panic(e)
	}

	// create a new batch from the connection
	b := NewBatch(conn)

	// send a SET command
	if _, e := b.Send("SET", "foo", "BAR"); e != nil {
		panic(e)
	}

	// Send a GET command and keep a promise that will contain its value after execution
	promise, e := b.Send("GET", "foo")
	if e != nil {
		panic(e)
	}

	// execute also returns the promises, but we're not interested in this right now
	if _, e := b.Execute(); e != nil {
		panic(e)
	}

	s, _ := redis.String(promise.Reply())

	fmt.Println(s)
	// Outpux: BAR

}

//func TestMain(m *testing.M) {
//	srv, _ = disposable_redis.NewServerRandomPort()

//	rc := m.Run()

//	srv.Stop()
//	os.Exit(rc)
//}
