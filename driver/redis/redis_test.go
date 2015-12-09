package redis

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/stretchr/testify/assert"

	"github.com/EverythingMe/disposable-redis"
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/meduza/driver"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/query"
	"github.com/EverythingMe/meduza/schema"
)

var srv *disposable_redis.Server
var drv driver.Driver
var conn redis.Conn

const scm = `
schema: testung
tables:
    Users:
        engines: 
            - redis
        primary:
            type: random
        columns:
            name: 
                type: Text
            email:
                type: Text
            lastVisit:
                type: Timestamp
            score:
                type: Int
            mip:
                type: Map
        indexes:
            -   type: simple
                columns: [name]
            -   type: compound
                columns: [name,email]
            -   type: compound
                columns: [name,score]
                
    Apps:
        engines: 
            - redis	
        primary:
            type: compound
            columns: [packageId,locale]
            options:
                hashed: false
        columns:
            packageId:
                type: Text
            locale:
                type: Text
            name: 
                type: Text
        indexes:
            -   type: simple
                columns: [name]                             
`

func setUp() (err error) {

	logging.SetLevel(logging.ERROR)
	srv, err = disposable_redis.NewServerRandomPort()
	if err != nil {
		return err
	}
	addr := "localhost:6379"
	_ = addr
	conf := Config{
		Network:         "tcp",
		Addr:            srv.Addr(),
		DeleteChunkSize: 50,
	}

	if err = srv.WaitReady(200 * time.Millisecond); err != nil {
		return err
	}

	conn, err = redis.Dial(conf.Network, conf.Addr)
	if err != nil {
		return err
	}

	sp := schema.NewStringProvider(scm)
	if err := sp.Init(); err != nil {
		return err
	}

	drv = NewDriver()
	if err := drv.Init(sp, conf); err != nil {
		return err
	}
	conn.Do("FLUSHDB")
	return nil
}

func TestSorting(t *testing.T) {
	//t.SkipNow()
	pq := query.NewPutQuery(usersTable)

	defer conn.Do("FLSUSHDB")

	for _, i := range []float64{-1000, -300, -0.3, -0.0005, 0.00005, 100, 300.5, 50.34534534} {
		pq.AddEntity(*schema.NewEntity("").Set("score", i).Set("name", "sortable"))
	}

	pr := drv.Put(*pq)
	if pr.Error != nil {
		t.Fatal("Failed putting objects: %s", pr.Error)
	}

	gr := drv.Get(*query.NewGetQuery(usersTable).
		FilterEq("name", "sortable").
		FilterBetween("score", 0.0, 1000.0).
		OrderBy("score", query.ASC).
		Limit(10))
	if gr.Error != nil {
		t.Error("Error running get query: %s", gr.Error)
	}

	if len(gr.Entities) == 0 {
		t.Error("No entities returned")
	}
	lastScore := schema.Float(-100000.0)
	for _, e := range gr.Entities {
		if sc, ok := e.Properties["score"].(schema.Float); !ok {
			t.Errorf("Score not returned as float: %s", reflect.TypeOf(sc))
		} else {
			logging.Debug("Score for %s: %f", e.Id, sc)
			if sc < 0 {
				t.Error("Scores not in range: %f", sc)
			}
			if sc < lastScore {
				t.Error("Out of order. last %f, current %f", lastScore, sc)
			}
			lastScore = sc
		}
	}

	//try descending
	gr = drv.Get(*query.NewGetQuery(usersTable).
		FilterEq("name", "sortable").
		OrderBy("score", query.DESC).
		Limit(3))
	if gr.Error != nil {
		t.Error("Error running get query: %s", gr.Error)
	}

	if len(gr.Entities) == 0 {
		t.Error("No entities returned")
	}

	lastScore = schema.Float(1000000)
	for _, e := range gr.Entities {
		if sc, ok := e.Properties["score"].(schema.Float); !ok {
			t.Error("Score not returned as float: %s", reflect.TypeOf(sc))
		} else {
			logging.Debug("Score for %s: %f", e.Id, sc)

			if sc > lastScore {
				t.Error("Out of order. last %f, current %f", lastScore, sc)
			}
			lastScore = sc
		}
	}

	conn.Do("FLUSHDB")

	for x, i := range []float64{-1000, -300, -0.3, -0.0005, 0.00005, 100, 300.5, 50.34534534} {
		pq := query.NewPutQuery(usersTable)
		pq.AddEntity(*schema.NewEntity("").Set("score", i).Set("name", fmt.Sprintf("sortable%d", x)))
		pr = drv.Put(*pq)
		if pr.Error != nil {
			t.Fatal("Failed putting objects: %s", errors.Sprint(pr.Error))

		}
	}

	// Sort desc by single property index
	gr = drv.Get(*query.NewGetQuery(usersTable).
		FilterBetween("name", "sortable", "sortible").
		OrderBy("name", query.DESC).
		Limit(3))
	if gr.Error != nil {
		t.Errorf("Error running get query: %s", gr.Error)
	}

	if len(gr.Entities) != 3 {
		t.Error("No entities returned")
	}

	var lastName schema.Text
	for _, ent := range gr.Entities {
		fmt.Println(ent.Id, ent.Properties["name"])
		if lastName != "" && ent.Properties["name"].(schema.Text) > lastName {
			t.Errorf("Names out of order: %s => %s", lastName, ent.Properties["name"])
		}
		lastName = ent.Properties["name"].(schema.Text)
	}

}

func tearDown() {
	if srv != nil {
		srv.Stop()
	}
}

func TestMain(m *testing.M) {
	logging.SetLevel(logging.ALL)
	if err := setUp(); err != nil {
		panic(err)
	}
	rc := m.Run()
	tearDown()
	os.Exit(rc)
}

const usersTable = "testung.Users"
const appsTable = "testung.Apps"

var mipmap = schema.NewMap().Set("foo", "bar")
var ents = []schema.Entity{

	*schema.NewEntity("").Set("name", "user1").Set("email", "user1@domain.com").Set("mip", mipmap).Set("score", 1),
	*schema.NewEntity("id2").Set("name", schema.Text("user2")).Set("email", schema.Text("user2@domain.com")).Set("mip", mipmap).Set("score", 2),
}

func BenchmarkPut(b *testing.B) {
	b.SkipNow()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pq := query.NewPutQuery(usersTable).AddEntity(*schema.NewEntity("").Set("name", schema.Text("user1")).Set("email", schema.Text("user1@domain.com")))
			drv.Put(*pq)
		}
	})

}
func TestPut(t *testing.T) {
	//t.SkipNow()
	defer conn.Do("FLUSHDB")
	pq := query.NewPutQuery(usersTable).AddEntity(ents[0]).AddEntity(ents[1])

	res := drv.Put(*pq)
	if res.Error != nil {
		t.Error("Error running put query: %s", res.Error)
		t.FailNow()

	}
	if len(res.Ids) != len(ents) {
		t.Error("Invalid ids returned")
	}

	if res.Time == 0 {
		t.Error("No execution time set for response")
	}

	if res.Ids[1] != ents[1].Id {
		t.Error("Expected driver to respect the entity id %s, got %s", ents[1].Id, res.Ids[1])
	}

	for _, id := range res.Ids {
		res, err := redis.Strings(conn.Do("HGETALL", fmt.Sprintf("%s:%s", usersTable, id)))
		if err != nil {
			t.Error(err)
		}

		if len(res) == 0 {
			t.Error("Entity not written to redis", id)
		}

		if len(res) != 8 {
			t.Error("Invalid num of elements in set expected %d, got %d", 8, len(res))
		}
	}

	// delete data before other tests
	conn.Do("FLUSHDB")

}

func TestGet(t *testing.T) {

	//t.SkipNow()
	defer conn.Do("FLUSHDB")

	pq := query.NewPutQuery(usersTable).AddEntity(ents[0]).AddEntity(ents[1])

	res := drv.Put(*pq)
	if res.Error != nil {
		t.Error("Error running put query: %s", res.Error)
		t.FailNow()
	}

	gr := drv.Get(*query.NewGetQuery(usersTable).Filter("id", query.In, res.Ids[0], res.Ids[1]))
	if gr.Error != nil {
		t.Error("Error running get query: %s", gr.Error)
	}

	if len(gr.Entities) != len(ents) {
		t.Error("Invalid number of entities returned")
	}

	for i, ent := range gr.Entities {
		if ent.Properties["name"] != ents[i].Properties["name"] {
			t.Errorf("Unmatching names between saved and loaded entities: %s/%s", ent.Properties["name"], ents[i].Properties["name"])
		}

		if ent.Properties["email"] != ents[i].Properties["email"] {
			t.Errorf("Unmatching email between saved and loaded entities: %s/%s", ent.Properties["email"], ents[i].Properties["email"])
		}

		if m, found := ent.Get("mip"); !found {
			t.Errorf("Could not find mip map in entity")
		} else {
			mp := m.(schema.Map)
			if len(mp) != 1 {
				t.Errorf("Invalid map size: %d", len(mp))
			}

			if mp["foo"] != mipmap["foo"] {
				t.Errorf("Invalid map value. expected %s got %s", mipmap["foo"], mp["foo"])
			}
		}

	}

	// Test getting with specific entities
	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("id", query.In, res.Ids[0], res.Ids[1]).Fields("name", "non-existing"))
	if gr.Error != nil {
		t.Error("Error running get query: %s", gr.Error)
	}

	if len(gr.Entities) != len(ents) {
		t.Errorf("Invalid number of entities returned: %d", len(gr.Entities))
	}

	for _, ent := range gr.Entities {
		if _, found := ent.Properties["name"]; !found {
			t.Errorf("Property 'name' not found when query limited to it")
		}

		for _, prop := range []string{"email", "mip", "score", "non-existing"} {
			if _, found := ent.Properties[prop]; found {

				t.Errorf("Property '%s' found when query limited not to fetch it", prop)
			}
		}
	}

	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("name", query.Eq, ents[0].Properties["name"]))
	if gr.Error != nil {
		t.Errorf("Error running get query: %s", gr.Error)
	}
	if len(gr.Entities) != 1 {
		t.Error("Wrong number of entities loaded (%d)", len(gr.Entities))
		t.FailNow()
	}

	if gr.Entities[0].Properties["name"] != ents[0].Properties["name"] {
		t.Errorf("Unmatching names between saved and loaded entities: %s/%s", gr.Entities[0].Properties["name"], ents[0].Properties["name"])
	}

	// test multi indexing
	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("name", query.Eq, ents[0].Properties["name"]).Filter("email", query.Eq, ents[0].Properties["email"]))
	if gr.Error != nil {
		t.Errorf("Error running get query on compound index: %s", gr.Error)
	}
	if len(gr.Entities) != 1 {
		t.Errorf("Wrong number of entities loaded (%d)", len(gr.Entities))
	}

	if gr.Entities[0].Properties["name"] != ents[0].Properties["name"] {
		t.Errorf("Unmatching names between saved and loaded entities: %s/%s", gr.Entities[0].Properties["name"], ents[0].Properties["name"])
	}
	if gr.Entities[0].Properties["email"] != ents[0].Properties["email"] {
		t.Errorf("Unmatching names between saved and loaded entities: %s/%s", gr.Entities[0].Properties["name"], ents[0].Properties["name"])
	}

	// now we select on the same index but with a combo that shouldn't yield anything
	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("name", query.Eq, ents[0].Properties["name"]).Filter("email", query.Eq, ents[1].Properties["email"]))
	if gr.Error != nil {
		t.Error("Error running get query on compound index: %s", gr.Error)
	}

	if len(gr.Entities) != 0 || gr.Total != 0 {
		t.Error("This query shouldn't have brought any results")
	}
	conn.Do("FLUSHDB")
}

func TestUpdate(t *testing.T) {
	//t.SkipNow()
	defer conn.Do("FLUSHDB")

	pq := query.NewPutQuery(usersTable).AddEntity(ents[0]).AddEntity(ents[1])

	res := drv.Put(*pq)
	if res.Error != nil {
		t.Error("Error running put query: %s", res.Error)
	}

	uq := query.NewUpdateQuery(usersTable).Set("name", schema.Text("zoink")).Increment("score", 100).DelProperty("email").
		Where(schema.IdKey, query.In, res.Ids[0], res.Ids[1])
	ur := drv.Update(*uq)
	if ur.Error != nil {
		t.Error("Error performing update query")
	}
	if ur.Num != 2 {
		t.Error("Wrong number of entities updated, expecte 2, got %d", ur.Num)
	}

	gr := drv.Get(*query.NewGetQuery(usersTable).Filter("id", query.In, res.Ids[0], res.Ids[1]))
	if gr.Error != nil {
		t.Error(gr.Error)
	}

	if len(gr.Entities) != 2 {
		t.Errorf("Got %d entities, expected 2", len(gr.Entities))
	}

	for _, ent := range gr.Entities {
		if ent.Properties["name"] != schema.Text("zoink") {
			t.Error("Entity not updated")
		}

		if ent.Properties["score"].(schema.Int) < 100 {
			t.Error("Invalid score: %v", ent.Properties["score"])
		}
	}

	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("name", query.Eq, ents[0].Properties["name"]))
	if gr.Error != nil {
		t.Error(gr.Error)
	}
	if len(gr.Entities) != 0 || gr.Total != 0 {
		t.Error("Got entities for old indexed key")
	}

	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("name", query.Eq, schema.Text("zoink")))
	if gr.Error != nil {
		t.Error(gr.Error)
	}
	if len(gr.Entities) != 2 || gr.Total != 2 {
		t.Error("Got wrong number of entities: ", len(gr.Entities), gr.Total)
	}

	conn.Do("FLUSHDB")

}

func TestDelete(t *testing.T) {
	//t.SkipNow()
	defer conn.Do("FLUSHDB")

	pq := query.NewPutQuery(usersTable).AddEntity(ents[0]).AddEntity(ents[1])

	res := drv.Put(*pq)
	if res.Error != nil {
		t.Error("Error running put query: %s", res.Error)
	}

	ent1Id := res.Ids[0]

	dq := query.NewDelQuery(usersTable).Where("name", query.Eq, ents[0].Properties["name"])
	dr := drv.Delete(*dq)
	if dr.Error != nil {
		t.Error(dr.Error)
	}
	if dr.Num != 1 {
		t.Error("Wrong number of entities deleted: ", dr.Num)

	}

	if dr.Time == 0 {
		t.Error("Time is not set")
	}

	gr := drv.Get(*query.NewGetQuery(usersTable).Filter("id", query.In, ent1Id))
	if gr.Total != 1 || len(gr.Entities) > 0 {
		t.Error("Retrieved entities that should have been deleted: ", gr.Entities, gr.Total)
	}

	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("id", query.In, ent1Id, ents[1].Id))
	if gr.Total != 1 || len(gr.Entities) != 1 || gr.Entities[0].Id != ents[1].Id {
		t.Error("Retrieved entities that should have been deleted: ", gr.Entities)
	}

	N := 200

	q := query.NewPutQuery(usersTable)
	for i := 0; i < N; i++ {
		q.AddEntity(*schema.NewEntity("").Set("name", schema.Text(fmt.Sprintf("User: %d", i))).
			Set("email", schema.Text("user@domain.com")).
			Set("time", schema.Timestamp(time.Now())))
	}

	res = drv.Put(*q)
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	gr = drv.Get(*query.NewGetQuery(usersTable).FilterIn(schema.IdKey, res.Ids[0], res.Ids[1], res.Ids[2]))
	assert.Equal(t, N+1, gr.Total)
	assert.Equal(t, 3, len(gr.Entities))

	tbl := drv.(*Driver).tables[usersTable]
	for i := range res.Ids {
		if i%2 == 0 {
			conn.Do("DEL", tbl.idKey(res.Ids[i]))
		}

	}

	dr = drv.Delete(*query.NewDelQuery(usersTable).Where("id", query.All))
	if dr.Error != nil {
		t.Fatal(dr.Error)
	}
	assert.Equal(t, N/2+1, dr.Num)

}

func TestCompoundPrimary(t *testing.T) {
	//t.SkipNow()
	defer conn.Do("FLUSHDB")

	pq := query.NewPutQuery(appsTable)
	pq.AddEntity(*schema.NewEntity("", schema.NewText("packageId", "me.everything"), schema.NewText("locale", "en"),
		schema.NewText("name", "EverythingMe")))
	pq.AddEntity(*schema.NewEntity("", schema.NewText("packageId", "me.everything"), schema.NewText("locale", "es"),
		schema.NewText("name", "HaoklAni")))
	pq.AddEntity(*schema.NewEntity("", schema.NewText("packageId", "com.facebook"), schema.NewText("locale", "en"),
		schema.NewText("name", "Fazebook")))

	res := drv.Put(*pq)
	if res.Error != nil {
		t.Error("Error running put query: %s", res.Error)
		t.FailNow()
	}
	if len(res.Ids) != 3 {
		t.Errorf("Wrong number of ids returned: %d", len(res.Ids))
	}
	fmt.Println(res.Ids)

	expectedIds := []schema.Key{"me.everything|en|", "me.everything|es|", "com.facebook|en|"}
	for i, id := range res.Ids {
		if id != expectedIds[i] {
			t.Errorf("Mismatch with expected id %s: got %s", expectedIds[i], id)
		}
	}

	// test by raw ids
	gr := drv.Get(*query.NewGetQuery(appsTable).FilterIn("id", res.Ids[0], res.Ids[1], res.Ids[2]))
	if gr.Error != nil {
		t.Errorf("Error running get query: %s", gr.Error)
	}
	if len(gr.Entities) != 3 {
		t.Errorf("Wrong number of entities returned: %d", len(gr.Entities))
	}

	// test by id composition from parameters
	gr = drv.Get(*query.NewGetQuery(appsTable).FilterEq("packageId", "me.everything").FilterIn("locale", "en", "es"))
	if gr.Error != nil {
		t.Errorf("Error running get query: %s", gr.Error)
	}
	if len(gr.Entities) != 2 {
		t.Errorf("Wrong number of entities returned: %d", len(gr.Entities))
	}

	expectedIds = []schema.Key{"me.everything|en|", "me.everything|es|"}
	for i, ent := range gr.Entities {
		if ent.Id != expectedIds[i] {
			t.Errorf("Mismatch with expected id %s: got %s", expectedIds[i], ent.Id)
		}
	}

}

func TestConvert(t *testing.T) {

	//t.SkipNow()

	encoder := Encoder{}
	decoder := Decoder{}

	s := schema.NewSet("foo", "bar", "baz")

	encoded, err := encoder.Encode(s)
	if err != nil {
		t.Errorf("Could not encode set: %s", err)
	}

	if len(encoded) != 39 {
		t.Errorf("Invalid encoded length :%d", len(encoded))
	}

	decoded, err := decoder.Decode(encoded, schema.UnknownType)
	if err != nil {
		t.Errorf("Error decodeing set: %s", err)
	}
	s2, ok := decoded.(schema.Set)
	if !ok {
		t.Errorf("Decoded struct is not a set")
	}

	if len(s2) != len(s) {
		t.Errorf("Unmatching size of decoded set, expected %d got %d", len(s), len(s2))
	}

	for k := range s {
		if _, found := s2[k]; !found {
			t.Errorf("%s not found in decoded set", k)
		}
	}

	m := schema.NewMap().Set("Foo", "Bar").Set("Bar", 123)

	if encoded, err = encoder.Encode(m); err != nil {
		t.Errorf("Error encoding map: %s", err)
	}

	v2, err := decoder.Decode(encoded, schema.UnknownType)
	if err != nil {
		t.Errorf("Error decodeing map: %s", err)
	}
	m2, ok := v2.(schema.Map)
	if !ok {
		t.Errorf("Decoded struct is not a map")
	}

	if len(m2) != len(m) {
		t.Errorf("Unmatching size of decoded set, expected %d got %d", len(m), len(m2))
	}

	for k, v := range m {
		if v2, found := m[k]; !found {
			t.Errorf("%s not found in decoded map", k)
		} else if v != v2 {
			t.Errorf("Unmatching values %v/%v", v, v2)
		}
	}

}

func BenchmarkCompress(b *testing.B) {
	encoder := Encoder{}

	v := schema.Text(`{"timestamp": 1431860197.618, "package_id": "com.leo.appmaster", "deviceid": "7fe24b7d3363ba38f01ba60689dc6399a5af9ef6ac546edf90c098074873a3d8", "original_event": {"settodefault": "", "num_widgets_retrieved": "", "lengthinseconds": "", "ctx_wifi_name": "'Alks.24-14'_e8:de:27:d7:8a:f4", "_logfile_seq": "399292", "results": "", "prefix": "", "_campaignid_raw": "", "query": "", "num_redirects": "", "mnc": "", "speed": "", "originatingrequestid": "", "xysize": "", "stats_locale": "", "hint": "", "ctx_net_signal": "52", "ctx_batt_pct": "43", "rowidx": "", "keyboardvisible": "", "istablet": "", "platform": "android", "_is_blacklisted": "", "location": "Monino,,Moskovskaya,Russia", "stats_source": "datalead_int", "networktype": "", "ctx_screen_active": "True", "details": "", "_zip": "", "ctx_locale": "", "stats_network": "", "videostartedafter": "", "stats_isretained": "true", "stats_campaign": "ru_45_newads", "packagename": "com.leo.appmaster", "stats_sessioninitcause": "", "_limit": "", "stats_sessioninitsrc": "", "totalcols": "", "exact": "", "numitems": "", "fromscreen": "", "num_shortcuts_retrieved": "", "_country": "Russia", "userid": "", "flags_extendednativeexperiences": "", "maxidx": "", "token": "", "appid": "", "videoview": "", "page": "", "_activity_signal": "False", "maxid": "", "stats_gondorservice": "stats/1.0", "locale": "ru", "servertime": "2015-05-17 11:54:32.860", "result": "", "flags_mockads": "", "stats_firstsession": "", "ctx_motion_state_confidence": "", "event": "UserStats", "latlon": "55.8359068,38.2059487", "errors": "", "network": "wifi", "json_data": "", "_app_type": "", "appstore": "com.android.vending", "predictionbarapps": "", "_local_timestamp": "2015-05-17 14:56:37.618", "_hostname": "pumba-ie1-edge-stats-api-i-78e0203b", "ctx_bt_on": "False", "ctx_dock": "", "osversion": "", "num_iconless_weblinks": "", "stats_gmt": "", "ctx_travelling": "False", "anchor_type": "", "ctx_loc_accy": "31.0", "screen": "", "screenheight": "", "algoid": "", "_indextime": "2015-05-17 11:56:56.484", "requestid": "", "ctx_known_loc_id": "ef9ff48e-215f-47b8-a086-e3e8e82bd704", "ctx_mnc": "", "client_api": "android production", "_city": "Monino", "fbappuserid": "", "deeplinked_apps": "", "src": "", "ctx_sim_country": "RU", "language": "", "anchor_token": "", "icons": "", "label": "", "experience": "", "ctx_headphones": "False", "num_shortcuts": "", "suggestion": "", "action": "", "suggestions": "", "stats_retrynum": "", "ctx_timeofday": "3", "num_folders_retrieved": "", "first": "", "flags_promotednativeapps": "", "num_icons": "", "totalrows": "", "app": "", "scores": "", "sfname": "", "stats_evmedevice": "false", "_unknown_fields": "{\"extraclusters\": \"\"}", "widgetpackagename": "", "_hour": "10", "num_retries": "", "guid": "f437f1bc-ec75-46e1-b4e8-52a9cd7ab0e0", "stats_inlandscapemode": "", "roaming": "", "screenname": "", "non_tagges_apps": "", "stats_useragent": "", "appname": "Privacy Guard", "_geoid": "", "apps": "", "q_app": "", "ctx_mcc": "", "extraclusters": "", "carriername": "", "keyboard_locale": "ru", "notificationid": "", "ctx_carrier": "", "type": "", "_details_long": "", "more": "", "clickid": "", "ctx_batt_state": "2", "contextdata": "", "certainity": "", "ctx_motion_state": "", "yearclass": "2011", "publisher": "", "stats_cookiesenabled": "", "mcc": "", "tokens": "", "sessionid": "de0290c5a99f4b7", "devicetype": "", "_source_timestamp": "2015-05-17 10:56:37.618", "_client_version_code": "380790256", "totaltime": "", "idx": "", "campaignid": "", "stats_settodefault": "true", "colidx": "", "bucket": "", "web_results_hint": "", "deviceid": "7fe24b7d3363ba38f01ba60689dc6399a5af9ef6ac546edf90c098074873a3d8", "_type_error_fields": "{\"ctx_mnc\": \"integer\", \"ctx_mcc\": \"integer\"}", "_adid_raw": "", "screenwidth": "", "_deviceid_hash": "b18ad0c1cdf0f71eba5b11319056a2f0", "ip": "93.123.241.236", "topics": "", "anchor_entity": "", "_state": "Moskovskaya", "num_folders": "", "cluster": "pumba-ie1", "_unknown_fields_old": "", "_apps_full": "", "timezone": "4.0", "stats_experiments": "{'folder_icon_grid':'default','super_duper_experiment':'A','hide_webapps':'default','remove_app_rec':'C','reset_home_for_none_default':'default2','reboot_uninstalled_launcher':'full_cling','new_app_share_or_open':'new_app_share','system_fonts':'use_roboto','use-url-redirect-handler':'default','keep_in_memory':'default','new-app-added-hook':'default','quick-contacts-default-on':'default','app_wall_hook_button_experiment':'round','discovery-scoring-decay-by-impression':'off','non_default_new_app_hook_options':'open_folder','import-previous-homescreen':'default','workspace_layout_fb_wa_fixed':'default','contextual-new-prediction':'default','recommended-apps-alternative-icon':'default','rate_us_experiment':'A','boarding-set-as-default-all-devices':'default','boarding_wide_brush_test_v2':'new_welcome_no_boarding_no_folder_selection','app_wall_install_button_experiment':'green','lollipop_set_as_default_1':'home_set','metrics_reporting_enabled':'default','discovery-preview-card-thumbnails':'default','default_new_app_hook_options':'share_app','show-preview-card-for-discovery':'default','contact_frecency_predictor_param':'threemonth','search_card_experiment':'C','hide-evme-icon':'default','quick_contacts_feature_activation':'no_info_pop','realtime_stats':'default','reset_home_none_default_v2':'popup_reset','boarding-experiences':'boarding_test_b','walkthrough_feature_activation':'activation','boarding_wide_brush_test':'E','rate_us_timing':'C','app_wall_experiment':'default','keep_in_memory_high':'default'}", "stats_localstorageenabled": "", "native": "True", "fromsfname": "", "num_weblinks": "", "appids": "", "num_widgets": "", "apptype": "", "type_hint": "", "ctx_manufacturer": "Sony", "stats_requestid": "0RrdgnhmZQo", "feature": "", "_query_language": "", "ctx_net_type": "wifi", "ctx_roaming": "False", "userevent": "appInstall", "stats_medium": "", "_logfile": "/var/log/doat/core/core.STATS/core.STATS_399292", "status": "", "ctx_missedcall": "", "context": "", "ctx_weekend": "True", "num_apps_retrieved": "", "_rowhash": "db74d315091ec3d3fde8a2fa30a6724d", "num_apps": "", "tosfname": "", "num_tokens": "", "adid": "", "related_packagename": "", "toscreen": "", "spelling": "", "elapsed": "61899", "url_old": "", "_date": "2015-05-17", "placementid": "", "ctx_loc_known": "1.0", "_timestamp": "2015-05-17 10:56:37.618", "url": "", "author": "", "native_apps_hint": "", "ctx_name": "C2105", "disambiguations": "", "videostoppedat": "", "title": "", "client_version": "3.1452.9823", "stats_deviceid": "f7edd2118f35f270", "ctx_btdevices": ""}}`)

	for i := 0; i < b.N; i++ {
		encoder.encodeCompressedText(v)
	}

}

func TestCompression(t *testing.T) {
	//t.SkipNow()
	encoder := Encoder{
		TextCompressThreshold: 0,
	}
	dec := Decoder{}

	//v := schema.Text(`"{\"timestamp\": 1431471472.58, \"package_id\": \"drug.vokrug\", \"deviceid\": \"25e790dbd25b7978bd21fc6b24f1f79cc8d294b0e09ee836cd1e0c22064e9a7d\"}`)
	v := schema.Text(`{"timestamp": 1431860197.618, "package_id": "com.leo.appmaster", "deviceid": "7fe24b7d3363ba38f01ba60689dc6399a5af9ef6ac546edf90c098074873a3d8", "original_event": {"settodefault": "", "num_widgets_retrieved": "", "lengthinseconds": "", "ctx_wifi_name": "'Alks.24-14'_e8:de:27:d7:8a:f4", "_logfile_seq": "399292", "results": "", "prefix": "", "_campaignid_raw": "", "query": "", "num_redirects": "", "mnc": "", "speed": "", "originatingrequestid": "", "xysize": "", "stats_locale": "", "hint": "", "ctx_net_signal": "52", "ctx_batt_pct": "43", "rowidx": "", "keyboardvisible": "", "istablet": "", "platform": "android", "_is_blacklisted": "", "location": "Monino,,Moskovskaya,Russia", "stats_source": "datalead_int", "networktype": "", "ctx_screen_active": "True", "details": "", "_zip": "", "ctx_locale": "", "stats_network": "", "videostartedafter": "", "stats_isretained": "true", "stats_campaign": "ru_45_newads", "packagename": "com.leo.appmaster", "stats_sessioninitcause": "", "_limit": "", "stats_sessioninitsrc": "", "totalcols": "", "exact": "", "numitems": "", "fromscreen": "", "num_shortcuts_retrieved": "", "_country": "Russia", "userid": "", "flags_extendednativeexperiences": "", "maxidx": "", "token": "", "appid": "", "videoview": "", "page": "", "_activity_signal": "False", "maxid": "", "stats_gondorservice": "stats/1.0", "locale": "ru", "servertime": "2015-05-17 11:54:32.860", "result": "", "flags_mockads": "", "stats_firstsession": "", "ctx_motion_state_confidence": "", "event": "UserStats", "latlon": "55.8359068,38.2059487", "errors": "", "network": "wifi", "json_data": "", "_app_type": "", "appstore": "com.android.vending", "predictionbarapps": "", "_local_timestamp": "2015-05-17 14:56:37.618", "_hostname": "pumba-ie1-edge-stats-api-i-78e0203b", "ctx_bt_on": "False", "ctx_dock": "", "osversion": "", "num_iconless_weblinks": "", "stats_gmt": "", "ctx_travelling": "False", "anchor_type": "", "ctx_loc_accy": "31.0", "screen": "", "screenheight": "", "algoid": "", "_indextime": "2015-05-17 11:56:56.484", "requestid": "", "ctx_known_loc_id": "ef9ff48e-215f-47b8-a086-e3e8e82bd704", "ctx_mnc": "", "client_api": "android production", "_city": "Monino", "fbappuserid": "", "deeplinked_apps": "", "src": "", "ctx_sim_country": "RU", "language": "", "anchor_token": "", "icons": "", "label": "", "experience": "", "ctx_headphones": "False", "num_shortcuts": "", "suggestion": "", "action": "", "suggestions": "", "stats_retrynum": "", "ctx_timeofday": "3", "num_folders_retrieved": "", "first": "", "flags_promotednativeapps": "", "num_icons": "", "totalrows": "", "app": "", "scores": "", "sfname": "", "stats_evmedevice": "false", "_unknown_fields": "{\"extraclusters\": \"\"}", "widgetpackagename": "", "_hour": "10", "num_retries": "", "guid": "f437f1bc-ec75-46e1-b4e8-52a9cd7ab0e0", "stats_inlandscapemode": "", "roaming": "", "screenname": "", "non_tagges_apps": "", "stats_useragent": "", "appname": "Privacy Guard", "_geoid": "", "apps": "", "q_app": "", "ctx_mcc": "", "extraclusters": "", "carriername": "", "keyboard_locale": "ru", "notificationid": "", "ctx_carrier": "", "type": "", "_details_long": "", "more": "", "clickid": "", "ctx_batt_state": "2", "contextdata": "", "certainity": "", "ctx_motion_state": "", "yearclass": "2011", "publisher": "", "stats_cookiesenabled": "", "mcc": "", "tokens": "", "sessionid": "de0290c5a99f4b7", "devicetype": "", "_source_timestamp": "2015-05-17 10:56:37.618", "_client_version_code": "380790256", "totaltime": "", "idx": "", "campaignid": "", "stats_settodefault": "true", "colidx": "", "bucket": "", "web_results_hint": "", "deviceid": "7fe24b7d3363ba38f01ba60689dc6399a5af9ef6ac546edf90c098074873a3d8", "_type_error_fields": "{\"ctx_mnc\": \"integer\", \"ctx_mcc\": \"integer\"}", "_adid_raw": "", "screenwidth": "", "_deviceid_hash": "b18ad0c1cdf0f71eba5b11319056a2f0", "ip": "93.123.241.236", "topics": "", "anchor_entity": "", "_state": "Moskovskaya", "num_folders": "", "cluster": "pumba-ie1", "_unknown_fields_old": "", "_apps_full": "", "timezone": "4.0", "stats_experiments": "{'folder_icon_grid':'default','super_duper_experiment':'A','hide_webapps':'default','remove_app_rec':'C','reset_home_for_none_default':'default2','reboot_uninstalled_launcher':'full_cling','new_app_share_or_open':'new_app_share','system_fonts':'use_roboto','use-url-redirect-handler':'default','keep_in_memory':'default','new-app-added-hook':'default','quick-contacts-default-on':'default','app_wall_hook_button_experiment':'round','discovery-scoring-decay-by-impression':'off','non_default_new_app_hook_options':'open_folder','import-previous-homescreen':'default','workspace_layout_fb_wa_fixed':'default','contextual-new-prediction':'default','recommended-apps-alternative-icon':'default','rate_us_experiment':'A','boarding-set-as-default-all-devices':'default','boarding_wide_brush_test_v2':'new_welcome_no_boarding_no_folder_selection','app_wall_install_button_experiment':'green','lollipop_set_as_default_1':'home_set','metrics_reporting_enabled':'default','discovery-preview-card-thumbnails':'default','default_new_app_hook_options':'share_app','show-preview-card-for-discovery':'default','contact_frecency_predictor_param':'threemonth','search_card_experiment':'C','hide-evme-icon':'default','quick_contacts_feature_activation':'no_info_pop','realtime_stats':'default','reset_home_none_default_v2':'popup_reset','boarding-experiences':'boarding_test_b','walkthrough_feature_activation':'activation','boarding_wide_brush_test':'E','rate_us_timing':'C','app_wall_experiment':'default','keep_in_memory_high':'default'}", "stats_localstorageenabled": "", "native": "True", "fromsfname": "", "num_weblinks": "", "appids": "", "num_widgets": "", "apptype": "", "type_hint": "", "ctx_manufacturer": "Sony", "stats_requestid": "0RrdgnhmZQo", "feature": "", "_query_language": "", "ctx_net_type": "wifi", "ctx_roaming": "False", "userevent": "appInstall", "stats_medium": "", "_logfile": "/var/log/doat/core/core.STATS/core.STATS_399292", "status": "", "ctx_missedcall": "", "context": "", "ctx_weekend": "True", "num_apps_retrieved": "", "_rowhash": "db74d315091ec3d3fde8a2fa30a6724d", "num_apps": "", "tosfname": "", "num_tokens": "", "adid": "", "related_packagename": "", "toscreen": "", "spelling": "", "elapsed": "61899", "url_old": "", "_date": "2015-05-17", "placementid": "", "ctx_loc_known": "1.0", "_timestamp": "2015-05-17 10:56:37.618", "url": "", "author": "", "native_apps_hint": "", "ctx_name": "C2105", "disambiguations": "", "videostoppedat": "", "title": "", "client_version": "3.1452.9823", "stats_deviceid": "f7edd2118f35f270", "ctx_btdevices": ""}}`)
	//v := schema.Text("\xe5\x9c\x8b\xe8\xbb\x8d\xe6\xa1\x83\xe5\x9c\x92\xe7\xb8\xbd\xe9\x86\xab\xe9\x99\xa2\xe7\x9a\x84\xe7\xb6\x93\xe7\x87\x9f\xe7\x90\x86\xe5\xbf\xb5\xef\xbc\x8c\xe4\xb8\x80\xe7\x9b\xb4\xe4\xbb\xa5\xe3\x80\x8c\xe6\x8f\x90\xe4\xbe\x9b\xe4\xba\xba\xe6\x80\xa7\xe5\x8c\x96\xe7\x9a\x84\xe9\xab\x98\xe5\x93\x81\xe8\xb3\xaa\xe9\x86\xab\xe7\x99\x82\xe6\x9c\x8d\xe5\x8b\x99\xe3\x80\x8d\xe7\x82\xba\xe5\x8e\x9f\xe5\x89\x87\xef\xbc\x8c\xe6\x9c\xac\xe8\x91\x97\xe6\x9c\x8d\xe5\x8b\x99\xe7\xac\xac\xe4\xb8\x80\xe7\x9a\x84\xe7\xb2\xbe\xe7\xa5\x9e\xef\xbc\x8c\xe5\x9c\xa8\xe9\x86\xab\xe9\x99\xa2\xe5\x85\xa8\xe9\xab\x94\xe5\x90\x8c\xe4\xbb\x81\xe7\x9a\x84\xe5\x8a\xaa\xe5\x8a\x9b\xe4\xb9\x8b\xe4\xb8\x8b\xef\xbc\x8c\xe4\xbb\xa5\xe4\xbf\x9d\xe8\xad\xb7\xe8\xbb\x8d\xe6\xb0\x91\xe4\xb9\x8b\xe5\x81\xa5\xe5\xba\xb7\xe7\x82\xba\xe5\xae\x97\xe6\x97\xa8\xe3\x80\x82\n\xe5\x9c\x8b\xe8\xbb\x8d\xe6\xa1\x83\xe5\x9c\x92\xe7\xb8\xbd\xe9\x86\xab\xe9\x99\xa2\xe7\x9a\x84\xe9\x86\xab\xe7\x99\x82\xe6\xb0\xb4\xe6\xba\x96\xe5\x8d\x93\xe8\xb6\x8a\xef\xbc\x8c\xe6\x87\x89\xe8\xa8\xba\xe9\x86\xab\xe7\x99\x82\xe7\x9a\x84\xe5\xb0\x88\xe7\xa7\x91\xe5\xae\x8c\xe5\x82\x99\xef\xbc\x8c\xe5\x8c\x85\xe6\x8b\xac\xe5\x85\xa7\xe3\x80\x81\xe5\xa4\x96\xe3\x80\x81\xe9\xaa\xa8\xe3\x80\x81\xe5\xa9\xa6\xe3\x80\x81\xe5\x85\x92\xe3\x80\x81\xe7\xb2\xbe\xe7\xa5\x9e\xe3\x80\x81\xe8\x80\xb3\xe9\xbc\xbb\xe5\x96\x89\xe3\x80\x81\xe7\x9c\xbc\xe3\x80\x81\xe7\x89\x99\xe3\x80\x81\xe5\xbe\xa9\xe5\x81\xa5\xe3\x80\x81\xe7\x9a\xae\xe8\x86\x9a\xe7\xad\x8920\xe9\xa4\x98\xe7\xa8\xae\xe5\xb0\x88\xe7\xa7\x91\xe5\x8f\x8a\xe6\xac\xa1\xe5\xb0\x88\xe7\xa7\x91\xe3\x80\x82\xe6\x98\xaf\xe8\xa1\x9b\xe7\x94\x9f\xe7\xbd\xb2\xe6\xa0\xb8\xe5\xae\x9a\xe7\x9a\x84\xe5\x8d\x80\xe5\x9f\x9f\xe6\x95\x99\xe5\xad\xb8\xe9\x86\xab\xe9\x99\xa2\xef\xbc\x8c\xe4\xb9\x9f\xe6\x98\xaf\xe5\x9c\x8b\xe9\x98\xb2\xe9\x83\xa8\xe8\xbb\x8d\xe9\x86\xab\xe5\xb1\x80\xe6\x89\x80\xe5\xb1\xac\xe5\x8c\x97\xe9\x83\xa8\xe6\x88\xb0\xe5\x8d\x80\xe8\xb2\xac\xe4\xbb\xbb\xe4\xb8\xad\xe5\xbf\x83\xe9\x86\xab\xe9\x99\xa2\xef\xbc\x8c\xe4\xb8\x8d\xe4\xbd\x86\xe4\xbb\xa5\xe5\xae\x8c\xe5\x96\x84\xe7\x9a\x84\xe9\x86\xab\xe7\x99\x82\xe8\xa8\xad\xe5\x82\x99\xe7\x85\xa7\xe8\xad\xb7\xe4\xb8\x89\xe8\xbb\x8d\xe8\xa2\x8d\xe6\xbe\xa4\xe8\x88\x87\xe7\x9c\xb7\xe5\xb1\xac\xe7\x9a\x84\xe5\x81\xa5\xe5\xba\xb7\xef\xbc\x8c\xe4\xb9\x9f\xe5\x90\x8c\xe6\x99\x82\xe6\x8f\x90\xe4\xbe\x9b\xe6\xa1\x83\xe3\x80\x81\xe7\xab\xb9\xe3\x80\x81\xe8\x8b\x97\xe5\x9c\xb0\xe5\x8d\x80\xe4\xb8\x80\xe8\x88\xac\xe6\xb0\x91\xe7\x9c\xbe\xe5\x81\xa5\xe4\xbf\x9d\xe7\x9a\x84\xe9\x86\xab\xe7\x99\x82\xe7\x85\xa7\xe9\xa1\xa7\xef\xbc\x8c\xe6\x9b\xb4\xe6\x98\xaf\xe7\xb7\x8a\xe6\x80\xa5\xe6\x94\xaf\xe6\x8f\xb4\xe9\x87\x8d\xe5\xa4\xa7\xe7\x81\xbd\xe8\xae\x8a\xe7\x9a\x84\xe5\xbf\xab\xe9\x80\x9f\xe9\x86\xab\xe7\x99\x82\xe6\x95\x91\xe6\x8f\xb4\xe5\x9c\x98\xe9\x9a\x8a\xe3\x80\x82\n\xe3\x80\x8c\xe4\xbb\xa5\xe5\xae\xa2\xe7\x82\xba\xe5\xb0\x8a\xe3\x80\x8d\xe6\x98\xaf\xe6\x9c\xac\xe9\x99\xa2\xe6\x9c\x8d\xe5\x8b\x99\xe7\x9a\x84\xe7\x9b\xae\xe6\xa8\x99\xef\xbc\x8c\xe9\x99\xa4\xe4\xba\x86\xe4\xb8\x8d\xe6\x96\xb7\xe5\x9c\xb0\xe6\x8f\x90\xe5\x8d\x87\xe9\x86\xab\xe7\x99\x82\xe6\x8a\x80\xe8\xa1\x93\xe4\xb9\x8b\xe5\xa4\x96\xef\xbc\x8c\xe6\x88\x91\xe5\x80\x91\xe7\x9a\x84\xe5\x9c\x98\xe9\x9a\x8a\xe4\xb9\x9f\xe6\x8a\xb1\xe6\x8c\x81\xe8\x91\x97\xe8\xaa\x8d\xe7\x9c\x9f\xe3\x80\x81\xe6\x86\x90\xe6\x86\xab\xe3\x80\x81\xe5\xa5\x89\xe7\x8d\xbb\xe8\x88\x87\xe5\x9f\xb7\xe8\x91\x97\xe7\x9a\x84\xe7\xb2\xbe\xe7\xa5\x9e\xe4\xbd\xbf\xe7\x97\x85\xe4\xba\xba\xe8\x88\x87\xe5\x85\xb6\xe5\xae\xb6\xe5\xb1\xac\xe5\xbe\x97\xe5\x88\xb0\xe6\xba\xab\xe9\xa6\xa8\xe7\x85\xa7\xe8\xad\xb7\xef\xbc\x8c\xe6\xad\xa4\xe5\xa4\x96\xe4\xb8\xa6\xe5\xbc\xb7\xe5\x8c\x96\xe8\xb3\x87\xe8\xa8\x8a\xe7\xae\xa1\xe7\x90\x86\xef\xbc\x8c\xe4\xbb\xa5\xe6\x8f\x90\xe4\xbe\x9b\xe9\xab\x98\xe6\xb0\xb4\xe6\xba\x96\xe7\x9a\x84\xe9\x86\xab\xe7\x99\x82\xe7\x85\xa7\xe8\xad\xb7\xe5\x93\x81\xe8\xb3\xaa\xe3\x80\x82\n\xe5\x9c\x8b\xe8\xbb\x8d\xe6\xa1\x83\xe5\x9c\x92\xe7\xb8\xbd\xe9\x86\xab\xe9\x99\xa2\xe5\xb0\x87\xe7\xa7\x89\xe6\x8c\x81\xe4\xb8\x8a\xe8\xbf\xb0\xe7\x9a\x84\xe7\x90\x86\xe5\xbf\xb5\xef\xbc\x8c\xe7\x82\xba\xe5\xa4\xa7\xe5\xae\xb6\xe6\x8f\x90\xe4\xbe\x9b\xe6\x9c\x89\xe6\x95\x88\xe7\x8e\x87\xe3\x80\x81\xe9\xab\x98\xe5\x93\x81\xe8\xb3\xaa\xe3\x80\x81\xe5\xaf\xac\xe6\x95\x9e\xe6\xba\xab\xe9\xa6\xa8\xe7\x9a\x84\xe9\x86\xab\xe7\x99\x82\xe7\x85\xa7\xe8\xad\xb7\xe7\x92\xb0\xe5\xa2\x83\xef\xbc\x8c\xe7\x82\xba\xe5\xa4\xa7\xe5\xae\xb6\xe6\x9c\x8d\xe5\x8b\x99\xe3\x80\x82\xe8\xab\x8b\xe5\xa4\x9a\xe8\xb3\x9c\xe6\x8c\x87\xe6\x95\x99\xe8\x88\x87\xe9\xbc\x93\xe5\x8b\xb5\xef\xbc\x8c\xe8\xae\x93\xe6\x88\x91\xe5\x80\x91\xe6\x9b\xb4\xe9\x80\xb2\xe6\xad\xa5\xe3\x80\x82\n\xe5\xae\x89\xe8\xa3\x9d\xe5\x9c\x8b\xe8\xbb\x8d\xe6\xa1\x83\xe5\x9c\x92\xe7\xb8\xbd\xe9\x86\xab\xe9\x99\xa2APP\xe3\x80\x8c\xe8\xa1\x8c\xe5\x8b\x95\xe8\xb3\x87\xe8\xa8\x8a\xe6\x9c\x8d\xe5\x8b\x99\xe7\xb3\xbb\xe7\xb5\xb1\xe3\x80\x8d\xef\xbc\x8c\xe8\xae\x93\xe6\x82\xa8\xe9\x9a\xa8\xe6\x99\x82\xe9\x9a\xa8\xe5\x9c\xb0\xe4\xb8\x80\xe6\x89\x8b\xe6\x8e\x8c\xe6\x8f\xa1\xe5\xb0\xb1\xe8\xa8\xba\xe8\xb3\x87\xe8\xa8\x8a\xef\xbc\x8c\xe5\x8c\x85\xe6\x8b\xac\xe7\x9c\x8b\xe8\xa8\xba\xe9\x80\xb2\xe5\xba\xa6\xe3\x80\x81\xe7\x9c\x8b\xe8\xa8\xba\xe6\x8f\x90\xe9\x86\x92\xe3\x80\x81\xe8\xa1\x8c\xe5\x8b\x95\xe6\x8e\x9b\xe8\x99\x9f\xe3\x80\x81\xe9\x96\x80\xe8\xa8\xba\xe8\xa1\xa8\xe3\x80\x81\xe8\xa1\x9b\xe6\x95\x99\xe6\x96\xb0\xe7\x9f\xa5\xe3\x80\x81\xe6\x9c\x80\xe6\x96\xb0\xe6\xb6\x88\xe6\x81\xaf\xe3\x80\x81\xe9\x86\xab\xe7\x99\x82\xe5\x9c\x98\xe9\x9a\x8a\xe4\xbb\x8b\xe7\xb4\xb9\xe3\x80\x81\xe4\xba\xa4\xe9\x80\x9a\xe8\xb3\x87\xe8\xa8\x8a\xe3\x80\x81\xe9\x9b\xbb\xe8\xa9\xb1\xe5\xbf\xab\xe6\x92\xa5\xe7\xad\x89\xe6\x9c\x8d\xe5\x8b\x99\xef\xbc\x8c\xe6\xad\xa1\xe8\xbf\x8e\xe6\xb0\x91\xe7\x9c\xbe\xe8\xb8\xb4\xe8\xba\x8d\xe4\xb8\x8b\xe8\xbc\x89\xe4\xbd\xbf\xe7\x94\xa8\xe3\x80\x82\xe6\x88\x91\xe5\x80\x91\xe4\xb9\x9f\xe5\xb0\x87\xe4\xb8\x8d\xe5\xae\x9a\xe6\x9c\x9f\xe6\x8f\x90\xe4\xbe\x9b\xe7\xb3\xbb\xe7\xb5\xb1\xe5\x8a\x9f\xe8\x83\xbd\xe5\x8d\x87\xe7\xb4\x9a\xe8\x88\x87\xe6\x9b\xb4\xe6\x96\xb0\xe7\x89\x88\xe6\x9c\xac\xe3\x80\x82")

	b := encoder.encodeCompressedText(v)
	u := encoder.encodeText(v)

	if len(b) >= len(u) {
		t.Errorf("No compression (%d>=%d)", len(b), len(u))
	}

	fmt.Println("Uncompressed:", len(u), "Compressed: ", len(b), "Ratio: ", float32(len(b))/float32(len(u)))

	compressedLen := len(b)

	if b[0] != CompressedTextPrefixSnappy {
		t.Errorf("Wrong prefix: %c", b[0])
	}

	txt, err := dec.decodeCompressedText(b[1:])
	if err != nil {
		t.Fatal(err)
	}

	if v != txt {
		t.Errorf("Expected %s\ngot %s", v, txt)
	}

	// Now test auto-compress
	encoder.TextCompressThreshold = 1

	b, err = encoder.Encode(v)
	if err != nil {
		t.Fatal(err)
	}

	if len(b) != compressedLen {
		t.Errorf("Wrong re-compressed len %d (expected %d)", len(b), compressedLen)
	}

	if b[0] != CompressedTextPrefixSnappy {
		t.Errorf("Wrong prefix: %c", b[0])
	}

	// Test backwards compat
	b = encoder.encodeCompressedTextLZW(v)
	if len(b) == 0 || len(b) == len(u) {
		t.Fatal(len(b))
	}
	if b[0] != CompressedTextPrefix {
		t.Errorf("Wrong prefix: %c", b[0])
	}

	i, err := decoder.Decode(b, schema.UnknownType)
	if err != nil {
		t.Fatal(err)
	}
	if i != v {
		t.Errorf("Wrong decompressed :%s", i)
	}

}

var numberTests = []struct {
	num     interface{}
	encoded string
	tp      schema.ColumnType
}{
	{100, "100", schema.IntType},
	{0, "0", schema.IntType},
	{-100, "-100", schema.IntType},
	{uint64(math.MaxInt64 + 1), "9223372036854775808", schema.UintType},
	{0.01, "0.010000", schema.FloatType},
	{-0.001, "-0.001000", schema.FloatType},
}

func TestNumberConvert(t *testing.T) {

	//t.SkipNow()

	encoder := Encoder{}
	decoder := Decoder{}

	for _, dat := range numberTests {

		b, err := encoder.Encode(dat.num)
		if err != nil {
			t.Errorf("Could not encode %v: %s", dat.num, err)
		}

		if string(b) != dat.encoded {
			t.Errorf("Wrong encoded number. expected %s, got %s", dat.encoded, string(b))
		}

		decoded, err := decoder.Decode(b, schema.UnknownType)
		if err != nil {
			t.Errorf("Could not decode number %s: %s", string(b), err)
		}

		if internal, _ := schema.InternalType(dat.num); decoded != internal {
			t.Errorf("Wrong number decoded. Expected %v, got %v", internal, decoded)
		}

	}

}

func TestTTL(t *testing.T) {

	//t.SkipNow()
	defer conn.Do("FLSUSHDB")

	pq := query.NewPutQuery(usersTable)

	pq.AddEntity(*schema.NewEntity("").Set("name", "expiry mcspire").Expire(20 * time.Millisecond))

	pr := drv.Put(*pq)
	if pr.Err() != nil {
		t.Fatal(pr.Err())
	}
	if len(pr.Ids) != 1 {
		t.Fatalf("Got %d ids from query, expected just one", pr.Ids)
	}

	gr := drv.Get(*query.NewGetQuery(usersTable).FilterEq("id", pr.Ids[0]))
	if gr.Err() != nil {
		t.Fatal(gr.Err)
	}

	if len(gr.Entities) != 1 {
		t.Errorf("Got invalid number of entities")
	}

	if gr.Entities[0].Id != pr.Ids[0] {
		t.Errorf("Unmatchind ids returned %s/%s", gr.Entities[0].Id, pr.Ids[0])
	}

	// wait a bit for expiration to work
	time.Sleep(21 * time.Millisecond)

	if gr = drv.Get(*query.NewGetQuery(usersTable).FilterEq("id", pr.Ids[0])); gr.Err() != nil {
		t.Fatal(gr.Err())
	}

	if len(gr.Entities) != 0 {
		t.Errorf("We should have gotten nothing! got %s", gr.Entities)
	}

	tbl, _ := drv.(*Driver).getTable(usersTable)
	//	if n, _ := redis.Int(conn.Do("ZCARD", tbl.primary.(randomPrimaryIndex).RedisKey())); n != 1 {
	//		t.Errorf("There should be one entry in the model's primary now, got %d", n)
	//	}

	go drv.(*Driver).repairTables(10 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	if n, _ := redis.Int(conn.Do("ZCARD", tbl.primary.(randomPrimaryIndex).RedisKey())); n != 0 {
		t.Errorf("There should be no entries in the model's primary now, got %d", n)
	}

	// Test expiration with Update
	if pr = drv.Put(*query.NewPutQuery(usersTable).AddEntity(*schema.NewEntity("").Set("name", "expiry mcspire"))); pr.Err() != nil {
		t.Fatal(pr.Err())
	}

	if len(pr.Ids) != 1 {
		t.Fatalf("Got %d ids from query, expected just one", pr.Ids)
	}
	time.Sleep(100 * time.Millisecond)

	if gr = drv.Get(*query.NewGetQuery(usersTable).FilterEq("id", pr.Ids[0])); gr.Err() != nil {
		t.Fatal(gr.Err())
	}
	if len(gr.Entities) != 1 {
		t.Fatal("Got no entities")
	}

	uq := query.NewUpdateQuery(usersTable).Expire(20 * time.Millisecond).WhereId(pr.Ids[0])
	ur := drv.Update(*uq)
	if ur.Err() != nil {
		t.Error(ur.Err())
	}

	time.Sleep(50 * time.Millisecond)

	if gr = drv.Get(*query.NewGetQuery(usersTable).FilterEq("id", pr.Ids[0])); gr.Err() != nil {
		t.Fatal(gr.Err())
	}
	if len(gr.Entities) != 0 {
		t.Fatal("Got entities when we shouldn't have")
	}

}

func TestMultiPut(t *testing.T) {
	//t.SkipNow()
	defer conn.Do("FLUSHDB")
	N := 10000
	var err error
	ents := make([]schema.Entity, N)
	for i := 0; i < N; i++ {
		ents[i] = *schema.NewEntity("").Set("name", schema.Text(fmt.Sprintf("User: %d", i))).
			Set("email", schema.Text("user@domain.com")).
			Set("foo", schema.Int(int64(i+1000))).
			Set("bar", schema.Text("baz")).
			Set("time", schema.Timestamp(time.Now()))
		if err != nil {
			t.Fatal(err)
		}
	}
	st := time.Now()
	res := drv.Put(query.PutQuery{Table: usersTable, Entities: ents})
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	elapsed := time.Since(st)
	fmt.Printf("Creating %d users took %s. %.02frows/sec\n", N, elapsed, float64(N)/(float64(elapsed)/float64(time.Second)))

}

func TestScan(t *testing.T) {
	//t.SkipNow()
	N := 100
	defer conn.Do("FLUSHDB")

	ents := make([]schema.Entity, N)
	for i := 0; i < N; i++ {
		ents[i] = *schema.NewEntity("").Set("name", schema.Text(fmt.Sprintf("User: %d", i)))
	}
	res := drv.Put(query.PutQuery{Table: usersTable, Entities: ents})
	if res.Error != nil {
		t.Errorf("Failed putting entities: %s", res.Error)
	}

	if len(res.Ids) != N {
		t.Errorf("Wrong number of ids.expected :%d, got %d", N, len(res.Ids))
	}

	// get a sorted list of the ids for comparison with paging
	ids := propertyList{}
	for _, id := range res.Ids {
		ids = append(ids, string(id))
	}
	ids = ids.sorted()

	offset := 0
	limit := 10

	for offset+limit < N {

		gr := drv.Get(*query.NewGetQuery(usersTable).All().Page(offset, limit))

		if gr.Error != nil {
			t.Errorf("Error paging: %s", gr.Error)
			break
		}

		if len(gr.Entities) != limit {
			t.Errorf("Wrong number of entities: %d", len(gr.Entities))
		}

		for i := range gr.Entities {
			if ids[offset+i] != string(gr.Entities[i].Id) {
				t.Errorf("Wrong id returned for paging: %s/%s", ids[offset+i], gr.Entities[i].Id)
			}
		}

		offset += limit

	}

}
func TestRepair(t *testing.T) {
	//t.SkipNow()

	conn.Do("FLUSHDB")

	pq := query.NewPutQuery(usersTable).AddEntity(ents[0]).AddEntity(ents[1])

	res := drv.Put(*pq)
	if res.Error != nil {
		t.Error("Error running put query: %s", res.Error)
		t.FailNow()
	}

	ids := res.Ids

	gr := drv.Get(*query.NewGetQuery(usersTable).Filter("name", query.Eq, ents[0].Properties["name"]))
	if gr.Err() != nil {
		t.Fatal("Could not get entities", gr.Err())
	}

	if len(gr.Entities) != 1 {
		t.Fatalf("Did not get entities completely: got %d", len(gr.Entities))
	}

	tbl, _ := drv.(*Driver).getTable(usersTable)

	for _, idx := range tbl.indexes {
		if _, err := conn.Do("DEL", idx.(*CompoundIndex).RedisKey()); err != nil {
			t.Fatal(err)
		}
	}
	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("name", query.Eq, ents[0].Properties["name"]))
	if gr.Err() != nil {
		t.Fatal("Could not get entities", gr.Err())
	}

	if len(gr.Entities) != 0 {
		t.Fatalf("Did not get entities completely: got %d", len(gr.Entities))
	}

	// let us repair the index
	go drv.(*Driver).repairEntities(10 * time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	gr = drv.Get(*query.NewGetQuery(usersTable).Filter("name", query.Eq, ents[0].Properties["name"]))
	if gr.Err() != nil {
		t.Fatal("Could not get entities", gr.Err())
	}

	if len(gr.Entities) != 1 {
		t.Fatalf("Did not get entities completely: got %d", len(gr.Entities))
	}

	if _, err := conn.Do("DEL", tbl.idKey(ids[0]), tbl.idKey(ids[1])); err != nil {
		t.Fatal(err)
	}

	l, _ := redis.Int(conn.Do("ZCARD", tbl.primary.(randomPrimaryIndex).RedisKey()))
	if l != 2 {
		t.Fatalf("Wrong number of entries in primary key: %d", l)
	}

	go drv.(*Driver).repairTables(10 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	l, _ = redis.Int(conn.Do("ZCARD", tbl.primary.(randomPrimaryIndex).RedisKey()))
	if l != 0 {
		t.Fatalf("Wrong number of entries in primary key: %d", l)
	}
}

func TestStats(t *testing.T) {
	//t.SkipNow()
	conn.Do("FLUSHDB")
	N := 10000
	var err error
	ents := make([]schema.Entity, N)
	for i := 0; i < N; i++ {
		ents[i] = *schema.NewEntity("").Set("name", schema.Text(fmt.Sprintf("User: %d", i))).
			Set("email", schema.Text("user@domain.com")).
			Set("foo", schema.Int(int64(i+1000))).
			Set("bar", schema.Text("baz")).
			Set("time", schema.Timestamp(time.Now()))
		if err != nil {
			t.Fatal(err)
		}
	}
	res := drv.Put(query.PutQuery{Table: usersTable, Entities: ents})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	st, err := drv.(*Driver).tables[usersTable].Stats(100)
	assert.NoError(t, err)
	assert.NotNil(t, st)
	assert.True(t, int(st.NumRows) == N)
	assert.True(t, st.EstimatedDataSize > 0)
	assert.True(t, st.EstimatedKeysSize > 0)
	t.Logf("Sampled table data: %#v", st)

	globalStats, err := drv.Stats()
	assert.NoError(t, err)
	assert.NotNil(t, globalStats)
	assert.Equal(t, 2, len(globalStats.Tables))
	t.Logf("Sampled driver data: %#v", globalStats)

}

func TestDump(t *testing.T) {
	////t.SkipNow()
	conn.Do("FLUSHDB")
	N := 1000
	var err error
	ents := make([]schema.Entity, N)
	for i := 0; i < N; i++ {
		ents[i] = *schema.NewEntity("").Set("name", schema.Text(fmt.Sprintf("User: %d", i))).
			Set("email", schema.Text("user@domain.com")).
			Set("foo", schema.Int(int64(i+1000))).
			Set("bar", schema.Text("baz")).
			Set("time", schema.Timestamp(time.Now()))
		if err != nil {
			t.Fatal(err)
		}
	}
	res := drv.Put(query.PutQuery{Table: usersTable, Entities: ents})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	ch, errch, _, err := drv.Dump(usersTable)
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	ok := true
	st := time.Now()
	for ok {
		select {
		case ent := <-ch:
			if ent.Id == "" {
				t.Error("Got empty id")
			}
			if len(ent.Properties) == 0 {
				t.Error("Got empty properties")
			}
			i++
		case err := <-errch:
			if err != nil {
				t.Fatal(err)
			}
			ok = false
		}
	}
	fmt.Println("dumping rate:", float32(N)/(float32(time.Since(st))/float32(time.Second)), "ents/sec")

	if i != N {
		t.Error("Expected %d ents, got %d", N, i)
	}

}

func TestDelProperty(t *testing.T) {

	//t.SkipNow()

	conn.Do("FLUSHDB")

	ent := *schema.NewEntity("").Set("Foo", "Bar").Set("Bar", "Baz")

	res := drv.Put(*query.NewPutQuery(usersTable).AddEntity(ent))
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	gr := drv.Get(*query.NewGetQuery(usersTable).FilterEq("id", res.Ids[0]))
	assert.NoError(t, gr.Err())
	assert.Len(t, gr.Entities, 1)

	_, found := gr.Entities[0].Properties["Foo"]
	assert.True(t, found)
	assert.EqualValues(t, "Bar", gr.Entities[0].Properties["Foo"])

	ur := drv.Update(*query.NewUpdateQuery(usersTable).WhereId(res.Ids[0]).DelProperty("Foo"))
	assert.NoError(t, ur.Err())
	assert.Equal(t, ur.Num, 1)

	gr = drv.Get(*query.NewGetQuery(usersTable).FilterEq("id", res.Ids[0]))
	assert.NoError(t, gr.Err())
	assert.Len(t, gr.Entities, 1)

	_, found = gr.Entities[0].Properties["Foo"]
	assert.False(t, found)

}
