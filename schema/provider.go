package schema

import (
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/EverythingMe/meduza/errors"
	"gopkg.in/fsnotify.v1"
)

// SchemaProvider is an interface for drivers, letting them access schema definitions,
// and notifying them about schema changes in real time
type SchemaProvider interface {
	Init() error
	Schemas() []*Schema
	Updates() (<-chan *Schema, error)
	Stop()
}

type Deployer interface {
	Deploy(r io.Reader) error
	DeployUri(uri string) error
}

// FilesProvider is a simple schema provider that reads static files in a directory, and monitors this directory
// for changes in files. If it finds a changed file, it re-reads it and issues an update
type FilesProvider struct {
	rootDir  string
	schemas  map[string]*Schema
	stopchan chan bool
}

// NewFilesProvider creates a files based provider listening on schema files inside root
func NewFilesProvider(root string) *FilesProvider {
	prov := &FilesProvider{
		rootDir: root,
		schemas: make(map[string]*Schema),
	}
	return prov
}

const expectedSuffix = ".schema.yaml"

// Init reads all the schema files in the root directory.
//
// NOTE: It raises an error only if reading the directory is empty. It can behave as if
// everything is normal even if ALL the schema files in the directory are bad
func (p *FilesProvider) Init() error {
	files, err := ioutil.ReadDir(p.rootDir)

	loaded := 0
	errs := 0
	var lastError error = nil

	if err != nil {
		return logging.Errorf("Could not read path %s: %s", p.rootDir, err)
	}

	for _, file := range files {

		if !file.IsDir() && strings.HasSuffix(file.Name(), expectedSuffix) {
			logging.Info("Trying to load schema file %s", file.Name())

			fullpath := filepath.Join(p.rootDir, file.Name())

			sc, err := LoadFile(fullpath)
			if err != nil {
				logging.Error("Could not load schema in %s: %s", fullpath, sc)
				lastError = err
				errs++
				continue
			}

			logging.Info("Loaded schema in file %s", file.Name())
			loaded++

			p.schemas[file.Name()] = sc

		}
	}

	if loaded > 0 {
		logging.Info("Loaded %d/%d of schema files in %s", loaded, loaded+errs, p.rootDir)
		return nil
	}
	logging.Error("Failed loading schemas: %s", lastError)
	return nil
}

// Schemas returns a list of all the schemas in the root directory
func (p *FilesProvider) Schemas() []*Schema {

	ret := make([]*Schema, 0, len(p.schemas))
	for _, sc := range p.schemas {
		ret = append(ret, sc)
	}
	return ret
}

// Stop stops litening for changes
func (p *FilesProvider) Stop() {
	logging.Info("stopping file provider")
	if p.stopchan != nil {
		close(p.stopchan)
	}
}

type FilesDeployer struct {
	rootDir string
}

func NewFileDeployer(rootDir string) FilesDeployer {
	return FilesDeployer{
		rootDir: rootDir,
	}
}

func (p FilesDeployer) Deploy(r io.Reader, name string) error {

	if !strings.HasSuffix(name, expectedSuffix) {
		name = name + expectedSuffix
	}

	fullpath := path.Join(p.rootDir, name)

	fp, err := os.Create(fullpath)
	if err != nil {
		logging.Errorf("Failed creating schema file: %s", err)
	}
	defer fp.Close()

	if _, err = io.Copy(fp, r); err != nil {
		return errors.NewError("Error writing to disk: %s", err)
	}

	return nil
}
func (p FilesDeployer) DeployUri(uri string, name string) error {

	u, err := url.Parse(uri)
	if err != nil {
		return errors.NewError("Could not parse schema uri: %s", err)
	}

	if u.Scheme == "file" {

		fp, err := os.Open(path.Join(u.Host, u.Path))
		if err != nil {
			return errors.NewError("Could not open schema file %s: %s", u.Path, err)
		}

		defer fp.Close()
		return p.Deploy(fp, name)
	} else {

		return errors.NewError("Illegal Uri scheme: %s", uri)
	}

}

// Updates starts monitoring the root dir for changes. Any changed or new schema is returned through
// the updates channel. Drivers can diff it by name with its older version to tell what changes had been
// made to it
func (p *FilesProvider) Updates() (<-chan *Schema, error) {

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logging.Error("Could not create fs notifier: %s", err)
		return nil, err
	}

	p.stopchan = make(chan bool)
	updatec := make(chan *Schema)

	go func() {
		defer func() {
			err := recover()
			if err != nil {
				panic(err)
			}
			logging.Info("Updates exiting")
			close(updatec)
			watcher.Close()
			p.stopchan = nil
		}()

		logging.Info("Starting to monitor directory %s", p.rootDir)
		for {
			select {
			case event := <-watcher.Events:
				logging.Debug("Got event: %v", event)
				switch {

				case event.Op&fsnotify.Create == fsnotify.Create:
					logging.Info("File %s created", event.Name)
				case event.Op&fsnotify.Write == fsnotify.Write:

					if strings.HasSuffix(event.Name, expectedSuffix) {
						logging.Info("Detected change in schema file %s: %v", event.Name, event.String())
						fullpath := filepath.Join(p.rootDir, event.Name)

						sc, err := LoadFile(fullpath)
						if err != nil {
							logging.Error("Could not load modified file %s: %s", fullpath, err)

						} else {

							p.schemas[event.Name] = sc
							updatec <- sc
						}

					}

				case event.Op&fsnotify.Remove == fsnotify.Rename:

					logging.Info("Deleted schema file %s", event.Name)
					// TODO: Handle this somehow
					logging.Warning("SCHEMA REMOVALS NOT HANDLED CURRENTLY. PLEASE RESTART")

				}

			case err := <-watcher.Errors:
				logging.Error("error watching schema dir:", err)
			case <-p.stopchan:
				logging.Info("Stopping schema watcher for %s", p.rootDir)
				return
			}
		}

	}()

	err = watcher.Add(p.rootDir)
	if err != nil {

		p.Stop()
		return nil, err

	}

	return updatec, nil
}

// StringProvider wraps a schema string (mainly used for testing) in a provider.
// It does NOT provide any updates and does not support multiple schemata
type StringProvider struct {
	schemaString string
	sc           []*Schema
}

// NewStringProvider wraps the schema in sc in a provider
func NewStringProvider(sc string) *StringProvider {
	return &StringProvider{
		schemaString: sc,
	}
}

// Init loads the schema string into the provider, reporting an error if it is a bad one
func (p *StringProvider) Init() error {

	if sc, err := Load(strings.NewReader(p.schemaString)); err != nil {
		return err
	} else {
		p.sc = []*Schema{sc}
	}

	return nil
}

// Schemas returns a one-sized slice of schemas containing our parsed string schema
func (p StringProvider) Schemas() []*Schema {
	return p.sc
}

// Updates does nothing in this provider and returns nil
func (StringProvider) Updates() (<-chan *Schema, error) {
	return nil, errors.NewError("Updates not supported on static string schema")
}

// Stop does nothing in this provider
func (StringProvider) Stop() {}
