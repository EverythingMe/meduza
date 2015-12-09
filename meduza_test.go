package meduza

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/EverythingMe/disposable-redis"
	"github.com/EverythingMe/meduza/client/resp"
	"github.com/EverythingMe/meduza/driver/redis"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/protocol/bson"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
	redis_transport "github.com/EverythingMe/meduza/transport/resp"
	redigo "github.com/garyburd/redigo/redis"
)

var server *redis_transport.Server

var redisServer *disposable_redis.Server

var conn redigo.Conn

const Schema = "evme"
const Table = "Users"

func initServer() (err error) {

	redisServer, err = disposable_redis.NewServerRandomPort()
	if err != nil {
		return errors.NewError("Error starting redis server: %s", err)
	}

	config := redis.Config{
		Network: "tcp",
		Addr:    redisServer.Addr(),
		Timeout: 0,
	}

	if err = redisServer.WaitReady(200 * time.Millisecond); err != nil {
		return
	}

	conn, err = redigo.Dial(config.Network, config.Addr)
	if err != nil {
		return
	}

	sp := schema.NewFilesProvider("./schema")
	if err := sp.Init(); err != nil {
		return err
	}

	drv := redis.NewDriver()
	drv.Init(sp, config)

	proto := bson.BsonProtocol{}
	server = redis_transport.NewServer(drv, proto)
	go func() {
		if err := server.Listen(":9977"); err != nil {
			panic(err)
		}
	}()

	return nil
}

func tearDown() {

	redisServer.Stop()
	conn.Close()

}

func setUp() {

	err := initServer()

	if err != nil {
		tearDown()
		panic("Exiting on error: " + err.Error())
	}

	Setup(Schema, resp.NewDialer(bson.BsonProtocol{}, "localhost:9977"))

}

func TestMain(m *testing.M) {

	setUp()
	defer tearDown()
	rc := m.Run()

	os.Exit(rc)

}

type User struct {
	Id    string    `db:",primary"`
	Name  string    `db:"name"`
	Email string    `db:"email"`
	Time  time.Time `db:"time"`
	Count int64     `db:"count"`
}

func TestPut(t *testing.T) {
	//t.SkipNow()
	defer conn.Do("FLUSHDB")

	u := []interface{}{
		&User{
			Name:  "user 1",
			Email: "user1@domain.com",
			Time:  time.Now(),
			Count: 1,
		},
		&User{
			Name:  "user 2",
			Email: "user2@domain.com",
			Time:  time.Now(),
			Count: 2,
		},
		&User{
			Id:    "user3",
			Name:  "user 3",
			Email: "user3@domain.com",
			Time:  time.Now(),
			Count: 3,
		},
	}

	ids, err := Put(Table, u...)

	if err != nil {
		t.Fatal("Could not save user: ", err)
	}

	if len(ids) != len(u) {
		t.Fatal("No ids returned")
	}

	for _, id := range ids {
		if id == "" {
			t.Error("Invalid id returned")
		}
	}

	if string(ids[2]) != u[2].(*User).Id {
		t.Error("User 3's id was pre-set, should have returned the same")
	}

	for _, obj := range u {
		fmt.Println(obj.(*User).Id)
	}
	fmt.Println("Returned ids:", ids)
}

func TestGet(t *testing.T) {
	//t.Skip()
	defer conn.Do("FLUSHDB")
	u := User{
		Name:  "user 1",
		Email: "user1@domain.com",
		Time:  time.Now().In(time.UTC).Round(time.Millisecond),
	}

	ids, err := Put(Table, &u)
	if err != nil {
		t.Errorf("Error putting object: %s", err)
	}

	if len(ids) != 1 {
		t.Fatalf("No ids returned from insert")
	}

	u2 := User{}
	// Test loading non pointer - should fail
	err = Get(Table, u2, ids[0])
	if err == nil {
		t.Error("Loading into non pointer should fail but it didn't")
	}

	err = Get(Table, &u2, ids[0])
	if err != nil {
		t.Errorf("Could not load user:%s", err)
	}

	if u.Id != u2.Id || u.Name != u2.Name || u.Email != u2.Email {
		t.Errorf("non identical objects: %s / %s", u, u2)
	}

}

func TestSelect(t *testing.T) {
	//t.Skip()
	defer conn.Do("FLUSHDB")

	u := []interface{}{
		&User{
			Name:  "Johnnie",
			Email: "user1@domain.com",
			Time:  time.Now(),
		},
		&User{
			Name:  "Johnnie",
			Email: "user2@domain.com",
			Time:  time.Now(),
		},
		&User{
			Id:    "user3",
			Name:  "user 3",
			Email: "user3@domain.com",
			Time:  time.Now(),
		},
	}

	ids, err := Put(Table, u...)
	_ = ids
	if err != nil {
		t.Fatal("Could not save user: ", err)
	}

	users := []User{}

	n, err := Select(Table, &users, 0, 10, query.Equals("name", schema.Text("Johnnie")))
	if err != nil {
		t.Fatal("Error loading users: ", err)
	}
	if n != 2 {
		t.Errorf("Wrong number of users loaded. expected 2, got %d", n)
	}
}

func TestUpdate(t *testing.T) {

	defer conn.Do("FLUSHDB")

	u := []interface{}{
		&User{
			Name:  "Johnnie",
			Email: "user1@domain.com",
			Time:  time.Now(),
		},
		&User{
			Name:  "Johnnie",
			Email: "user2@domain.com",
			Time:  time.Now(),
		},
	}

	ids, err := Put(Table, u...)
	_ = ids
	if err != nil {
		t.Fatal("Could not save user: ", err)
	}

	n, err := Update(Table, query.NewFilters(query.Equals("name", "Johnnie")), query.Set("name", "Ronnie"),
		query.Increment("count", 100))
	if err != nil {
		t.Fatal("Error loading users: ", err)
	}
	if n != 2 {
		t.Errorf("Wrong number of rows updated. expected 2, got %d", n)
	}

	users := []User{}

	n, err = Select(Table, &users, 0, 10, query.Equals("name", schema.Text("Johnnie")))
	if err != errors.EmptyResult {
		t.Fatal("Expected empty result")
	}

	n, err = Select(Table, &users, 0, 10, query.Equals("name", schema.Text("Ronnie")))
	if err != nil {
		t.Fatal("Expected empty result")
	}

	for _, u := range users {
		if u.Count < 100 {
			t.Errorf("Count not incremented, got %d", u.Count)
		}
	}

}

func TestDelete(t *testing.T) {
	//	t.SkipNow()
	defer conn.Do("FLUSHDB")

	u := []interface{}{
		&User{
			Name:  "Johnnie",
			Email: "user1@domain.com",
			Time:  time.Now(),
		},
		&User{
			Name:  "Johnnie",
			Email: "user2@domain.com",
			Time:  time.Now(),
		},
		&User{
			Name:  "Ronnie",
			Email: "user3@domain.com",
			Time:  time.Now(),
		},
	}

	ids, err := Put(Table, u...)
	_ = ids
	if err != nil {
		t.Fatal("Could not save users: ", err)
	}

	n, err := Delete(Table, query.Equals("name", schema.Text("Johnnie")))
	if n != 2 {
		t.Error("Expected 2 uers to be deleted. got %d", n)
	}

	users := []User{}
	n, err = Select(Table, &users, 0, 10, query.Equals("name", schema.Text("Johnnie")))
	if err != errors.EmptyResult {
		t.Fatal("Expected empty result")
	}

	n, err = Select(Table, &users, 0, 10, query.Equals("name", schema.Text("Ronnie")))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Error("Expected only one user to be loaded, got %d", n)
	}

}
