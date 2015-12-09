package errors

import (
	"fmt"
	"strings"
	"testing"
)

func foo() error {
	return Context(fmt.Errorf("WAT"))
}

func bar() error {
	return foo()
}

func TestError(t *testing.T) {
	e := bar()

	if E, ok := e.(*Error); !ok {
		t.Fatal(e, "not an error")
	} else {

		fmt.Println(e.(*Error).String())
		if len(E.stack) != 5 {
			t.Fatal("Wrong stack count")
		}

		s := Sprint(e)
		fmt.Println(s)
		if strings.Count(s, "\n") < len(E.stack) {
			t.Fatal("Too little line breaks. not a stack trace?")
		}
	}

	e = Context(nil)
	fmt.Println(e)
	if e != nil {
		t.Error("Nil error not nil")
	}

}
