package schema

import (
	"reflect"
	"strings"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/EverythingMe/meduza/errors"
)

// fieldSpec represents a single field in a struct, u
type fieldSpec struct {
	defaultValue string
	name         string
	index        []int
	typ          reflect.Type
}

type structSpec struct {
	fields      map[string]*fieldSpec
	fieldsIndex []*fieldSpec
	primary     *fieldSpec
}

var specs = map[reflect.Type]structSpec{}

type tag struct {
	ignored      bool
	name         string
	defaultValue string
	primary      bool
}

const (
	Tag        = "db"
	DefaultTag = "default"
)

func readTag(field reflect.StructField) tag {

	p := strings.Split(field.Tag.Get(Tag), ",")
	defaultTag := field.Tag.Get(DefaultTag)
	name := field.Name
	ret := &tag{
		name: name,
	}
	if len(p) > 0 {
		if p[0] == "-" {
			ret.ignored = true
		}
		if len(p[0]) > 0 {
			ret.name = p[0]
		}

		for _, s := range p[1:] {
			switch s {
			case "primary":
				ret.primary = true
			default:
				logging.Warning("unknown field flag '" + s + "' in mapper")
			}
		}
	}

	if defaultTag != "" {
		ret.defaultValue = defaultTag
	}
	return *ret
}

func compileStructSpec(t reflect.Type) structSpec {

	spec := structSpec{
		fields:      map[string]*fieldSpec{},
		fieldsIndex: make([]*fieldSpec, t.NumField()),
		primary:     nil,
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		switch {
		case f.PkgPath != "":
		// Ignore unexported fields.
		case f.Anonymous:
		// ignore anonymous structs
		default:

			tag := readTag(f)

			// don't do anything with ignored tags
			if tag.ignored {
				continue
			}

			// create the field spec
			fspec := &fieldSpec{
				index:        f.Index,
				name:         tag.name,
				defaultValue: tag.defaultValue,
				typ:          f.Type,
			}

			// make sure there aren't duplicate primary tags
			if tag.primary {
				if spec.primary != nil {
					panic("Error building mapper: Duplicate primary fields!")
				}

				spec.primary = fspec
			} else {
				spec.fieldsIndex[i] = fspec
				spec.fields[tag.name] = fspec
			}

			logging.Debug("Added mapping of field %s(%s), type %s, index %s", f.Name, fspec.name, fspec.typ, fspec.index)

		}
	}

	if spec.primary == nil {
		panic("Model " + t.Name() + " is without primary field!")
	}

	logging.Info("Compiled struct spec for %s", t.Name())

	specs[t] = spec

	return spec
}

func spec(t reflect.Type) structSpec {
	spec, found := specs[t]
	if !found {
		spec = compileStructSpec(t)
	}
	return spec
}

// MapEntity takes an entity and a pointer to a mapped object, and maps the entity's properties into the object's
// fields. Note that dst must be a pointer to a struct, anything else will fail
func DecodeEntity(e Entity, dst interface{}) error {

	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.NewError("Cannot map a nil or non pointer type")
	}
	v = v.Elem()
	t := v.Type()
	if t.Kind() != reflect.Struct {
		return errors.NewError("Mapping can only be done on structs")
	}

	spec := spec(t)

	primaryField := v.FieldByIndex(spec.primary.index)
	primaryField.SetString(string(e.Id))
	for k, p := range e.Properties {

		if fspec, found := spec.fields[k]; found {

			//logging.Debug("Assigning value %s from entity to field %s", p, k)
			field := v.FieldByIndex(fspec.index)
			val := reflect.ValueOf(p)

			switch {

			case !field.CanSet():
				return errors.NewError("Could not map field %s: destination field unassignable")

			case val.Type().AssignableTo(fspec.typ):
				field.Set(reflect.ValueOf(p))

			case val.Type().ConvertibleTo(fspec.typ):
				field.Set(val.Convert(fspec.typ))

			default:
				return errors.NewError("Cannot assign %s: incompatible types %s in entity and %s in type", k, val.Type(), fspec.typ)
			}
		} else {
			logging.Error("Could not find mappable field %s in %s", k, t.Name())
		}
	}

	return nil
}

// SetPrimary puts a primary id into a model object's primary field. dst must be a non nil pointer to
// a struct for this to work
func SetPrimary(id Key, dst interface{}) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.NewError("Cannot map a nil or non pointer type")
	}
	v = v.Elem()
	t := v.Type()
	if t.Kind() != reflect.Struct {
		return errors.NewError("Mapping can only be done on structs")
	}
	spec := spec(t)

	primaryField := v.FieldByIndex(spec.primary.index)
	primaryField.SetString(string(id))
	return nil
}

// EncodeStruct takes a model struct and encodes it into an entity.
// The struct must have a primary field, and cannot be nil
func EncodeStruct(src interface{}) (*Entity, error) {

	v := reflect.ValueOf(src)

	// do not allow nil/empty structs
	if src == nil || !v.IsValid() {
		return nil, errors.NewError("Cannot encode a nil object")
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	//only allow structs
	if v.Kind() != reflect.Struct {
		return nil, errors.NewError("Encoding entities is only possible from structs")
	}

	sp := spec(v.Type())

	if sp.primary == nil {
		return nil, errors.NewError("Struct %s does not have a primary field", v.Type())
	}

	ent := NewEntity("")

	// set the entity id
	primary := v.FieldByIndex(sp.primary.index)
	ent.Id = Key(primary.String())

	for i := 0; i < v.NumField(); i++ {

		fspec := sp.fieldsIndex[i]

		// skip ignored/primary field
		if fspec == nil {
			continue
		}

		field := v.Field(i)
		name := sp.fieldsIndex[i].name

		// convert
		val, err := InternalType(field.Interface())
		if err != nil {
			return nil, err
		}

		ent.Set(name, val)
	}

	return ent, nil

}
