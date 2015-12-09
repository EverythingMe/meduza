package schema

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/dvirsky/go-pylog/logging"

	"github.com/EverythingMe/meduza/errors"
	"gopkg.in/yaml.v2"
)

type IndexType string

type ColumnType string
type IndexState int8

type Key string

func (k Key) IsNull() bool {
	return k == ""
}

const (
	IdKey = "id"

	// Index type specds
	SimpleIndex   IndexType = "simple"
	CompoundIndex IndexType = "compound"
	SortedIndex   IndexType = "sorted"
	GeoIndex      IndexType = "geo"
	FullTextIndex IndexType = "fulltext"

	// primary index types
	PrimaryRandom   IndexType = "random"
	PrimaryCompound IndexType = "compound"
)

var allowedIndexTypes = map[string][]IndexType{
	RedisEngine:     {SimpleIndex, CompoundIndex, SortedIndex, GeoIndex, FullTextIndex, PrimaryRandom, PrimaryCompound},
	MysqlEngine:     {SimpleIndex, CompoundIndex, SortedIndex},
	CassandraEngine: {SimpleIndex, CompoundIndex, SortedIndex},
}

var allowedTypes = []ColumnType{IntType, FloatType, TextType, BoolType, TimestampType, BinaryType, SetType, ListType, MapType}

// IsAllowedType returns true if a given type string is in the our allowed types list
func IsAllowedType(c ColumnType) bool {
	for _, t := range allowedTypes {
		if c == t {
			return true
		}
	}

	return false
}

const (
	IndexReady   IndexState = 1
	IndexPending IndexState = 2
	IndexGarbage IndexState = 3
)

// Column describes a column in a table, if we are talking about a strict schema
type Column struct {
	Name       string                 `yaml:"name"`
	ClientName string                 `yaml:"clientName"`
	Type       ColumnType             `yaml:"type"`
	Default    interface{}            `yaml:"default,omitempty"`
	Comment    string                 `yaml:"comment,omitempty"`
	Options    map[string]interface{} `yaml:"options,omitempty"`
	// Admin Options
	AdminOptions struct {
		// If true - we do not show this column in forms
		Hidden bool `yaml:"hidden,omitempty"`
		// if true - we set a readonly property in the admin for this column
		ReadOnly bool `yaml:"readonly,omitempty"`
		// If set - mark the form ordering priority of this column in the admin form
		Priority int `yaml:"priority,omitempty"`
		// If set - specify a custom format for the admin (date, location, markdown, etc)
		// see https://github.com/jdorn/json-editor#format
		Format string `yaml:"format,omitempty"`
	} `yaml:"adminOptions,omitempty"`
}

func (c Column) HasDefault() bool {
	return c.Default != nil
}

func (c Column) GoName() string {
	return strings.Title(c.ClientName)
}

func (c *Column) Validate() error {

	if !IsAllowedType(c.Type) {
		return errors.NewError("Invalid type in schema: '%s' for column %s", c.Type, c.Name)
	}

	if !nameRx.Match([]byte(c.Name)) {
		return errors.NewError("Invalid column name '%s'. Names must be %s", nameRx.String())
	}

	if c.ClientName == "" {
		c.ClientName = c.Name
	}

	//TODO: Validate the default value
	return nil

}

// Equals checks if two columns are practically identical (between 2 tables)
func (c Column) Equals(other *Column) bool {
	return c.Name == other.Name && c.Type == other.Type && c.Default == other.Default
}

func (c Column) BoolOption(key string) (b bool, found bool) {

	var v interface{}
	if v, found = c.Options[key]; found {
		b, found = v.(bool)
	}

	return
}

func (c Column) StringOption(key string) (s string, found bool) {

	var v interface{}
	if v, found = c.Options[key]; found {
		s, found = v.(string)
	}

	return
}

func (c Column) IntOption(key string) (i int, found bool) {
	var v interface{}
	if v, found = c.Options[key]; found {

		switch x := v.(type) {

		case int:
			i = x
		case int32:
			i = int(x)
		case int64:
			i = int(x)
		default:
			found = false
		}

	}

	return
}

const (
	RedisEngine     = "redis"
	MysqlEngine     = "mysql"
	CassandraEngine = "cassandra"
)

// Table describes a table in the database, with its columns (optional) and indexes
type Table struct {
	Name     string   `yaml:"-"`
	BaseName string   `yaml:"name"`
	Comment  string   `yaml:"comment,omitempty"`
	Class    string   `yaml:"class,omitempty"` // the generated class name, leave empty for the same as Name
	Engines  []string `yaml:"engines,omitempty"`

	// If the table is not strict, we do not enforce a schema, and are just aware
	// of indexed columns.
	// You could use the schema for reference only
	Strict bool

	Columns map[string]*Column `yaml:"columns,omitempty"`

	Indexes []*Index `yaml:"indexes,omitempty"`

	Primary *Index `yaml:"primary"`

	AdminOptions struct {
		ListColumns []string `yaml:"listColumns,omitempty"`
		SearchBy    []string `yaml:"searchBy,omitempty"`
	} `yaml:"adminOptions,omitempty"`
}

// Validates makes sure that the table data is sane
func (t *Table) Validate() error {

	if t.Engines == nil || len(t.Engines) == 0 {
		return errors.NewError("No engines specified for table %s", t.Name)
	}

	if t.Name == "" {
		return errors.NewError("Tables with empty names are not allowed")
	}

	if !tableRx.Match([]byte(t.Name)) {
		return errors.NewError("Invalid table name '%s'. Names must be %s", nameRx.String())
	}

	if t.Class == "" {
		t.Class = t.BaseName
	}

	if t.AdminOptions.ListColumns == nil || len(t.AdminOptions.ListColumns) == 0 {
		t.AdminOptions.ListColumns = make([]string, 0, len(t.Columns))
		for c := range t.Columns {
			t.AdminOptions.ListColumns = append(t.AdminOptions.ListColumns, c)
			sort.Strings(t.AdminOptions.ListColumns)
		}
	}
	//TODO: Validate that the engines are sane

	// validate all columns
	for name, col := range t.Columns {
		if col.Name == "" {
			col.Name = name
		}
		if err := col.Validate(); err != nil {
			return err
		}

	}

	// validate all indexes
	for _, idx := range t.Indexes {
		if err := idx.Validate(t); err != nil {
			return err
		}
	}

	if t.Primary == nil {
		t.Primary = &Index{
			Name: "PRIMARY",
			Type: PrimaryRandom,
		}
	} else {
		if err := t.Primary.Validate(t); err != nil {
			return err
		}
	}

	return nil

}

var nameRx = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_]+$")
var tableRx = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_]+\\.[a-zA-Z][a-zA-Z0-9_]+$")

// Validate validates each table of the schema for sane values, and makes sure the internal name
// is the same as the external name in the schema's dictionary
func (s *Schema) Validate() error {
	if s.Name == "" {
		return errors.NewError("Schema has no name!")
	}

	if !nameRx.Match([]byte(s.Name)) {
		return errors.NewError("Invalid name '%s'. Names must be %s", nameRx.String())
	}

	// validate and fix the schema a bit
	for name, tbl := range s.Tables {
		tbl.BaseName = name
		tbl.Name = fmt.Sprintf("%s.%s", s.Name, name)

		// validate each table or fail if it has some weird shit in it
		if e := tbl.Validate(); e != nil {
			return e
		}

	}

	return nil
}

// LoadFile loads a schema from a file path. This is basically a wrapper for Load
func LoadFile(pth string) (*Schema, error) {

	r, e := os.Open(pth)
	if e != nil {
		return nil, errors.NewError("Could not open schema file: %s", e)
	}
	return Load(r)

}

// Load reads a schema from a reader and validates it. Returns the loaded schema if it was okay
// or an error if it wasn't
func Load(reader io.Reader) (*Schema, error) {

	sc := new(Schema)
	b, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, errors.NewError("Error reading schema file: %s", e)
	}

	e = yaml.Unmarshal(b, sc)
	if e != nil {
		return nil, errors.NewError("Error parsing schema YAML: %s", e)
	}

	// Validate the schema recursively for invalid values
	if e := sc.Validate(); e != nil {
		logging.Error("Error validating schema %s: %v", sc.Name, e)
		return nil, e
	}

	logging.Debug("Successfully loaded schema %s, %d tables", sc.Name, len(sc.Tables))
	return sc, nil

}

func NewTable(name string, strict bool) *Table {
	return &Table{
		Name:    name,
		Strict:  strict,
		Columns: make(map[string]*Column),
		Indexes: make([]*Index, 0),
	}
}

// AddColumn adds a column to the table, or returns an error if this column already exists
func (t *Table) AddColumn(name string, tp ColumnType, def interface{}, comment string) error {

	if _, exists := t.Columns[name]; exists {
		return errors.NewError("Column already exists")
	}

	col := &Column{
		Name:    name,
		Type:    tp,
		Default: def,
		Comment: comment,
	}

	t.Columns[name] = col
	return nil
}

// Index provides a description of an index, to be used by the specific driver
type Index struct {
	Name        string                 `yaml:"name"`
	Columns     []string               `yaml:"columns,omitempty"`
	Type        IndexType              `yaml:"type"`
	State       IndexState             `yaml:"-"`
	ExtraParams map[string]interface{} `yaml:"options,omitempty"`
}

const (
	OptSubType  = "subtype"
	OptRequired = "required"
	OptMaxLen   = "max_len"
)

func (i Index) Equals(other *Index) bool {
	return i.Name == other.Name && other.Type == i.Type
}

// Validate makes sure the that the index configuration is sane in the context
// of the table and the storage engine
func (i *Index) Validate(t *Table) error {

	i.SetName(t.Name)

	// check that all the indexe's columns are in the table spec
	for _, col := range i.Columns {
		if _, found := t.Columns[col]; !found {
			return errors.NewError("Table %s does not contain column %s required for index %s %s", t.Name, col, i.Type, i.Columns)
		}
	}

	// check that the type is sane and is supported by the table's engine
	for _, eng := range t.Engines {
		if allowed, found := allowedIndexTypes[eng]; found {
			indexAllowed := false
			for _, it := range allowed {
				if it == i.Type {
					indexAllowed = true
					break
				}
			}

			if !indexAllowed {
				return errors.NewError("Index type '%s' is not allowed for engine %s", i.Type, eng)
			}
		} else {
			return errors.NewError("No allowed indexes configured for engine '%s'", eng)
		}
	}

	return nil

}

func (i *Index) SetName(table string) {
	i.Name = fmt.Sprintf("%s__%s_%s", table, strings.Join(i.Columns, ","), i.Type)

}

func (t *Table) AddIndex(tp IndexType, columns ...string) error {

	idx := &Index{
		Columns:     columns,
		Type:        tp,
		State:       IndexPending,
		ExtraParams: make(map[string]interface{}),
	}

	idx.SetName(t.Name)

	t.Indexes = append(t.Indexes, idx)
	return nil
}

// AddColumn adds a column to the table, or returns an error if this column already exists
func (t *Table) AddColumnQuick(name string, tp ColumnType) error {
	return t.AddColumn(name, tp, nil, "")
}

// Schema describes a database and all its tables
type Schema struct {
	Name   string            `yaml:"schema"`
	Tables map[string]*Table `yaml:"tables"`

	// TODO: Add ACLs here
}

func NewSchema(name string) *Schema {
	return &Schema{
		Name:   name,
		Tables: make(map[string]*Table),
	}

}

func (s *Schema) AddTable(t *Table) {
	s.Tables[t.Name] = t
}

type SchemaChange struct {
	Table *Table
}

type TableAddedChange struct {
	SchemaChange
}

type TableDeletedChange struct {
	SchemaChange
}

type ColumnAddedChange struct {
	SchemaChange
	Column *Column
}

type ColumnAlterChange struct {
	SchemaChange
	Column *Column
}

type ColumnDeletedChange struct {
	SchemaChange
	Column *Column
}

type IndexAddedChange struct {
	SchemaChange
	Index *Index
}

type IndexRemovedChange struct {
	SchemaChange
	Index *Index
}

func (sc *Schema) Diff(other *Schema) ([]interface{}, error) {

	ret := make([]interface{}, 0)
	if other == nil {
		logging.Info("comparing schema with deleted one")
		for name, tbl := range sc.Tables {

			logging.Info("Table not in other schema: %s", name)
			ret = append(ret, TableDeletedChange{SchemaChange{tbl}})

		}

		return ret, nil
	}

	// check for deleted tables (not in other)
	for name, tbl := range sc.Tables {
		if otherTbl, found := other.Tables[name]; !found {
			logging.Info("Table not in other schema: %s", name)
			ret = append(ret, TableDeletedChange{SchemaChange{tbl}})
			continue
		} else {

			// if found - check for column changes
			for name, col := range tbl.Columns {

				// check for missing columns in other
				if otherCol, found := otherTbl.Columns[name]; !found {
					logging.Info("Column %s not in other table %s", name, otherTbl.Name)

					ret = append(ret, ColumnDeletedChange{SchemaChange{tbl}, col})

				} else {
					// check for column definition change
					if !col.Equals(otherCol) {
						logging.Info("Change in defintion of column %s.%s", tbl.Name, col.Name)
						ret = append(ret, ColumnAlterChange{SchemaChange{tbl}, otherCol})

					}
				}

			}

			// Check back for missing columns in the current table
			for name, col := range otherTbl.Columns {
				if _, found := tbl.Columns[name]; !found {
					logging.Info("Column %s.%s does not exist in original table", tbl.Name, col.Name)
					ret = append(ret, ColumnAddedChange{SchemaChange{tbl}, col})
				}
			}

			// Check for index changes

			// 1. check for deleted indexes (appear in current, not appear in other)
			for _, idx := range tbl.Indexes {

				found := false
				for _, otherIdx := range otherTbl.Indexes {
					if idx.Equals(otherIdx) {
						found = true
						break
					}
				}

				if !found {
					logging.Info("Index %s.%s does not exist in other table, deleting", tbl.Name, idx.Name)
					ret = append(ret, IndexRemovedChange{SchemaChange{tbl}, idx})
				}
			}

			// 2. Check for new indexes (appear in other, not in current)
			for _, otherIdx := range otherTbl.Indexes {
				found := false
				for _, idx := range tbl.Indexes {
					if idx.Equals(otherIdx) {
						found = true
						break
					}
				}
				if !found {
					logging.Info("Index %s.%s does not exist in current table, adding", tbl.Name, otherIdx.Name)
					ret = append(ret, IndexAddedChange{SchemaChange{tbl}, otherIdx})
				}
			}
		}

	}

	//check for added table in other schema
	for name, otherTbl := range other.Tables {
		if _, found := sc.Tables[name]; !found {
			logging.Info("Table %s not in current schema, adding it!", otherTbl.Name)
			ret = append(ret, TableAddedChange{SchemaChange{otherTbl}})
		}
	}

	logging.Info("Detected %d schema changes!", len(ret))
	return ret, nil
}
