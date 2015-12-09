package resp

import (
	"fmt"
	"testing"
	"time"

	"github.com/EverythingMe/meduza/driver/mock"
	"github.com/EverythingMe/meduza/protocol/bson"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
	"github.com/EverythingMe/meduza/transport/resp"
	"github.com/garyburd/redigo/redis"
)

func TestClient(t *testing.T) {

	addr := "localhost:9965"
	srv := resp.NewServer(mock.MockDriver{}, bson.BsonProtocol{})

	go func() {
		err := srv.Listen(addr)
		if err != nil {
			panic(err)
		}

	}()

	time.Sleep(250 * time.Millisecond)
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	cl := NewClient(bson.BsonProtocol{}, conn)

	resp, err := cl.Do(query.PingQuery{})
	if err != nil {
		t.Errorf("Error pinging: %s", err)
	}
	if r, ok := resp.(query.PingResponse); !ok {
		t.Errorf("Wrong response :%s", r)
	}

	resp, err = cl.Do(query.NewGetQuery("Users").FilterEq("id", "foo"))
	if err != nil {
		t.Errorf("Error running GET: %s", err)
	}
	if r, ok := resp.(query.GetResponse); !ok {
		t.Errorf("Wrong response :%s", r)
	} else {
		if r.Error != nil {
			t.Errorf("Error in GET: %s", r.Error)
		}
	}

}

func TestBenchmark(t *testing.T) {
	t.SkipNow()

	conn, err := redis.Dial("tcp", "localhost:9977")
	if err != nil {
		t.Fatal(err)
	}

	client := NewClient(bson.BsonProtocol{}, conn)
	N := 10000

	n, e := client.Do(query.NewDelQuery("Users").Where("id", query.All))
	fmt.Println(n, e)

	q := query.NewPutQuery("Users")
	for i := 0; i < N; i++ {

		q.AddEntity(*schema.NewEntity("").Set("name", schema.Text(fmt.Sprintf("User: %d", i))).
			Set("email", schema.Text("user@domain.com")).
			Set("time", schema.Timestamp(time.Now())))

	}
	st := time.Now()

	_, err = client.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(st)

	fmt.Printf("Creating %d users took %s. %.02frows/sec\n", N, elapsed, float64(N)/(float64(elapsed)/float64(time.Second)))
}
