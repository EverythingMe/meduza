package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/EverythingMe/meduza/query"

	"gopkg.in/yaml.v2"

	"github.com/dvirsky/go-pylog/logging"
)

// ListenCtl starts the conrtol server, panics if it can't be started
func ListenCtl(addr string) {

	mux := http.NewServeMux()
	mux.HandleFunc("/deploy", HandleDeploySchema)
	mux.HandleFunc("/status", HandleStatus)
	mux.HandleFunc("/confdump", HandleDumpConfig)
	mux.HandleFunc("/stats", HandleStats)
	mux.HandleFunc("/schemadump", HandleDumpSchema)
	mux.HandleFunc("/dump", HandleDumpData)
	mux.HandleFunc("/load", HandleLoadDump)
	mux.HandleFunc("/drop", HandleDrop)

	go func() {
		logging.Info("Starting ctl server on %s", addr)
		err := http.ListenAndServe(addr, mux)
		if err != nil {
			panic(err)
		}
	}()
}

func HandleDumpSchema(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	sch := r.FormValue("schema")

	for _, s := range meduzaServer.sp.Schemas() {
		if s.Name == sch {

			b, err := yaml.Marshal(s)
			if err != nil {
				http.Error(w, "Could not dump schema: "+err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "text/yaml")
			w.Write(b)
			return
		}
	}
	http.Error(w, "Could not find schema "+sch, http.StatusNotFound)
}

func HandleDumpData(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	sch := r.FormValue("schema")
	tbl := r.FormValue("table")

	err := meduzaServer.Dump(sch, tbl, w)
	if err != nil {
		fmt.Fprintf(w, "\nError dumping data: %s\n ", err)
	}
}

func HandleLoadDump(w http.ResponseWriter, r *http.Request) {

	sch := r.URL.Query().Get("schema")
	tbl := r.URL.Query().Get("table")

	err := meduzaServer.LoadDump(sch, tbl, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		fmt.Fprintln(w, "OK")
	}

}

func HandleDrop(w http.ResponseWriter, r *http.Request) {

	sch := r.URL.Query().Get("schema")
	tbl := r.URL.Query().Get("table")

	dq := query.NewDelQuery(fmt.Sprintf("%s.%s", sch, tbl)).Where("id", query.All)
	dr := meduzaServer.drv.Delete(*dq)
	if dr.Err() != nil {
		http.Error(w, dr.Err().Error(), http.StatusInternalServerError)
	} else {
		fmt.Fprintf(w, "Deleted %d records", dr.Num)
	}

}

func HandleDeploySchema(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	var data string
	uri := r.FormValue("uri")
	if uri == "" {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading form data", 400)
			return
		}

		data = string(b)

	}

	if uri == "" && data == "" {
		http.Error(w, "No data URI or raw data given", 400)
		return
	}

	var err error
	if uri != "" {
		logging.Info("Deploying schema from uri %s", uri)
		err = meduzaServer.sd.DeployUri(uri)
	} else {
		if len(data) > 10 {
			logging.Info("Deploying raw schema data '%s...' (%d bytes)", data[:10], len(data))
		}

		err = meduzaServer.sd.Deploy(strings.NewReader(data))
	}

	if err == nil {
		logging.Info("Successfully written schema")
		w.Write([]byte("OK"))
	} else {
		logging.Error("Error deploying schema: %s", err)
		http.Error(w, fmt.Sprintf("Error deploying schema: %s", err), 500)
	}

}

func HandleStatus(w http.ResponseWriter, r *http.Request) {

	var err error
	if meduzaServer == nil {
		err = errors.New("No meduza node running")
	}

	if err == nil {
		err = meduzaServer.Status()
	}

	if err != nil {
		logging.Error("Status check failed: %s", err)
		http.Error(w, err.Error()+"\r\n", 500)

	} else {
		w.Write([]byte("OK\r\n"))
	}

}

func HandleDumpConfig(w http.ResponseWriter, r *http.Request) {

	b, err := yaml.Marshal(config)
	if err != nil {
		http.Error(w, "Error dumping configs: "+err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "text/yaml")
	w.Write(b)

}

func HandleStats(w http.ResponseWriter, r *http.Request) {

	defer func() {
		err := recover()
		if err != nil {
			logging.Error("PANIC dumping stats: %s", err)
		}
	}()

	stats, err := meduzaServer.Stats()

	if err != nil {
		http.Error(w, "Error dumping stats: "+err.Error(), 500)
		return
	}

	b, err := yaml.Marshal(stats)
	if err != nil {
		http.Error(w, "Error dumping stats: "+err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "text/yaml")
	w.Write(b)

}
