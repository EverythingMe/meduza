package query

import (
	"fmt"
	"testing"

	"github.com/EverythingMe/meduza/schema"
)

type User struct {
	Id   schema.Key  `db:",primary"`
	Name schema.Text `db:"name"`
}

func TestResponseMappingSlice(t *testing.T) {
	r := NewGetResponse(nil)

	r.AddEntity(*schema.NewEntity("foofoo").Set("name", schema.Text("alice")))
	r.AddEntity(*schema.NewEntity("booboo").Set("name", schema.Text("bob")))
	r.Done()
	fmt.Println(r.Entities)

	users := make([]User, 0, 10)

	err := r.MapEntities(&users)
	if err != nil {
		t.Error(err)
	}

	if len(users) != len(r.Entities) {
		t.Errorf("Loaded %d entities when we should have %d", len(users), len(r.Entities))
	}

	for i, user := range users {
		if user.Id != r.Entities[i].Id {
			t.Error("Mismatching ids: %s/%s", user.Id, r.Entities[i].Id)
		}

		if user.Name != r.Entities[i].Properties["name"] {
			t.Error("Mismatching names: %s/%s", user.Name, r.Entities[i].Properties["name"])
		}
	}
}

func TestResponseMappingSingle(t *testing.T) {

	// Test single entity
	r := NewGetResponse(nil)
	r.AddEntity(*schema.NewEntity("foofoo").Set("name", schema.Text("booboo")))

	user := User{}

	if err := r.MapEntities(&user); err != nil {
		t.Error(err)
	}

	if user.Id != r.Entities[0].Id {
		t.Error("Id not mapped")
	}
	if user.Name != r.Entities[0].Properties["name"] {
		t.Error("Name not mapped")
	}

	//check that non pointers and nils cannot be mapped
	if err := r.MapEntities(user); err == nil {
		t.Error("Mapping non pointer should have failed")
	}
	if err := r.MapEntities(nil); err == nil {
		t.Error("Mapping nil pointer should have failed")
	}

	// check that mapping a multi line result to a single entity should fail
	r.AddEntity(*schema.NewEntity("googoo").Set("name", schema.Text("doodoo")))
	if err := r.MapEntities(&user); err == nil {
		t.Error("Mapping many entities to a single object should have failed")
	}
}
