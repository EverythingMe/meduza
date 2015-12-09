package errors

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/EverythingMe/bson/bson"
	"github.com/EverythingMe/bson/bytes2"
)

type trace struct {
	file     string
	line     int
	funcName string
}

const maxLevels = 8

type Error struct {
	err   error
	stack []trace
}

func isError(err error) bool {

	switch err.(type) {
	case *Error:
		return true
	}
	return false
}

func newError(err error) *Error {

	if isError(err) {
		return err.(*Error)
	}

	fpcs := make([]uintptr, maxLevels)

	n := runtime.Callers(3, fpcs)
	stack := make([]trace, n)

	for i, pc := range fpcs {
		if i >= n {
			break
		}
		fun := runtime.FuncForPC(pc - 1)

		if fun == nil {
			stack = stack[:i]
			break
		}

		file, line := fun.FileLine(pc - 1)

		stack[i] = trace{filepath.Base(file), line, fun.Name()}
	}

	ret := &Error{
		err:   err,
		stack: stack[:n],
	}

	return ret
}

func (err Error) GetStack() []string {

	ret := make([]string, len(err.stack))
	for i, t := range err.stack {
		ret[i] = fmt.Sprintf("%s:%d @ %s()", t.file, t.line, t.funcName)
	}
	return ret

}

func (err *Error) Error() string {
	if err.err != nil {
		return err.err.Error()
	}
	return "<NIL>"

}

func (err *Error) String() string {
	if err.err != nil {
		return fmt.Sprintf("%s\n  Stack Trace:\n  - %s", err.Error(), strings.Join(err.GetStack(), "\n  - "))
	}
	return "<NIL>"
}

func Sprint(e error) string {
	if !isError(e) {
		return e.Error()
	}

	return e.(*Error).String()
}

func Print(e error) {
	fmt.Println(Sprint(e))
}

func NewError(msg string, args ...interface{}) error {
	return newError(fmt.Errorf(msg, args...))
}

var (
	InvalidTableError = NewError("Invalid Table").(*Error)
	DuplicateKeyError = NewError("Duplicate Key").(*Error)
	NoIndexError      = NewError("No index matches the filter").(*Error)
	OpNotSupported    = NewError("Operation not supported by the index").(*Error)
	EmptyResult       = NewError("No results found for query").(*Error)
)

// Context wraps the error with an underlying "contextual" error object containing
// a stack trace
func Context(err error) error {
	if err == nil {
		return nil
	}
	return newError(err)
}

func Wrap(err error) *Error {
	if err == nil {
		return nil
	}
	return newError(err)
}

// MarshalBson overrides the vitess library's marshaling of strings that
// encodes strings as binary
func (e Error) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	msg := e.err.Error()
	bson.EncodePrefix(buf, bson.String, key)
	binary.LittleEndian.PutUint32(buf.Reserve(4), uint32(len(msg)+1))
	buf.WriteString(string(msg))
	buf.WriteByte(0)
}

func (e *Error) UnmarshalBson(buf *bytes.Buffer, kind byte) {

	if kind == bson.String {
		l := binary.LittleEndian.Uint32(buf.Next(4))
		e.err = fmt.Errorf("Server returned error: %s", string(buf.Next(int(l-1))))
	}
}
