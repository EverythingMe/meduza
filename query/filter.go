package query

import (
	"reflect"

	"github.com/dvirsky/go-pylog/logging"
	"github.com/EverythingMe/meduza/errors"
	"github.com/EverythingMe/meduza/schema"
)

const (
	In      = "IN"
	Eq      = "="
	Gt      = ">"
	Lt      = "<"
	Between = "><"
	All     = "ALL"
)

type Filter struct {
	Property string        `bson:"property"`
	Operator string        `bson:"op"`
	Values   []interface{} `bson:"values"`
}

// internal converts a value to its internal type in an unsafe manner, without
// raising an error if the conversion is impossible. We just log it and return the original value
func internal(value interface{}) interface{} {

	ret, err := schema.InternalType(value)
	if err != nil {
		logging.Error("Could not convert %s to internal type", reflect.TypeOf(value))
		return value
	}
	return ret
}

// NewFilter creates a new filter for a property with a given operator and comparison values
func NewFilter(field, operator string, values ...interface{}) Filter {

	for i := 0; i < len(values); i++ {
		values[i] = internal(values[i])
	}

	return Filter{
		Property: field,
		Operator: operator,
		Values:   values,
	}
}

func Equals(property string, value interface{}) Filter {
	return NewFilter(property, Eq, value)

}

func Range(property string, min, max interface{}) Filter {
	return NewFilter(property, Between, min, max)

}

func Within(property string, values ...interface{}) Filter {
	return NewFilter(property, In, values...)
}

// Fitlers is an abstraction over a map of filters for queries
type Filters map[string]Filter

func NewFilters(filters ...Filter) Filters {

	ret := make(Filters)
	for _, f := range filters {
		ret[f.Property] = f
	}

	return ret

}

// If the map is just one filter, we return it and true. otherwise a zero filter and false
func (f Filters) One() (Filter, bool) {
	if len(f) == 1 {
		for _, rf := range f {
			return rf, true
		}
	}
	return Filter{}, false

}

// Validate checks the filter for validity and returns an error if something is wrong with the filter
func (f Filter) Validate() error {

	//check that we have a property
	if len(f.Property) == 0 {
		return errors.NewError("No property given for filter")
	}

	// check that we have values
	if len(f.Values) == 0 && f.Operator != All {
		return errors.NewError("No values given for filter")
	}

	// do operator specific checks
	switch f.Operator {
	case Eq:
		if len(f.Values) > 1 {
			return errors.NewError("Too many values for equality filter")
		}
	case Between:
		if len(f.Values) != 2 {
			return errors.NewError("BETWEEN filters must have exactly 2 values, %d given", len(f.Values))
		}
	case In:
		if len(f.Values) < 1 {
			return errors.NewError("IN filters must have at least one value")
		}
	case All:
		if f.Property != schema.IdKey {
			return errors.NewError("ALL is allowed only on primary keys")
		}
	default:
		//TODO: Support more operators
		return errors.NewError("Unsupported operator: '%s'", f.Operator)
	}

	return nil
}
