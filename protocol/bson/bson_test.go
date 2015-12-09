package bson

import (
	"reflect"
	"testing"
	"time"

	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
	"github.com/EverythingMe/meduza/transport"
)

func testObject(t *testing.T, q interface{}, tp transport.MessageType) {
	p := BsonProtocol{}

	msg, err := p.WriteMessage(q)
	if err != nil {
		t.Error(err)
	}
	if msg.Type != tp {
		t.Error("Incompatible message type. expected GetMessage and got", msg.Type)
	}
	//	fmt.Println(string(msg.Body))

	v, err := p.ReadMessage(msg)
	if err != nil {
		t.Error(err)
	}

	if v == nil {
		t.Error("Got no deserialized object from message")
	}

	if reflect.TypeOf(v) != reflect.TypeOf(q) {
		t.Error(reflect.TypeOf(v), "not matching", reflect.TypeOf(q))
	}

	//	if !reflect.DeepEqual(q, v) {
	//		t.Errorf("%s:\n\n%s\ndoes not equal\n%s\n", tp, q, v)
	//	}
	//	fmt.Println(q, reflect.TypeOf(q))
	//	fmt.Println(v, reflect.TypeOf(v))
}

func makeGetResponse() *query.GetResponse {
	r := query.NewGetResponse(nil)
	defer r.Done()
	ent := schema.NewEntity("foofoo")
	ent.Set("foo", schema.Text("bar"))
	ent.Set("dt", schema.Timestamp(time.Now().Round(time.Millisecond).In(time.UTC)))
	r.AddEntity(*ent)

	return r
}
func TestBson(t *testing.T) {

	testables := []struct {
		v interface{}
		t transport.MessageType
	}{
		{*query.NewGetQuery("Users").Filter("name", query.In, "User 0", "User 1").Page(0, 10), transport.GetMessage},
		{*query.NewUpdateQuery("Users").Set("foo", schema.Timestamp(time.Now().Round(time.Millisecond).In(time.UTC))).Where("id", "=", "bar"), transport.UpdateMessage},
		{query.PutQuery{Table: "foo", Entities: []schema.Entity{*schema.NewEntity("", schema.NewText("foo", "bar")).
			Set("s", schema.NewSet(schema.TextType, "foo", "bar", "baz"))}}, transport.PutMessage},
		{
			query.DelQuery{
				Table:   "foo",
				Filters: query.NewFilters(query.Within("name", "User 0", "User 1")),
			},
			transport.DelMessage},
		{*makeGetResponse(), transport.GetResponseMessage},
		{*query.NewUpdateResponse(nil, 0), transport.UpdateResponseMessage},
		{*query.NewPutResponse(nil, "foo", "bar"), transport.PutResponseMessage},
		{*query.NewDelResponse(nil, 10), transport.DelResponseMessage},
	}

	for _, x := range testables {
		testObject(t, x.v, x.t)
	}

}
