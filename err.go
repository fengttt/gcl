package gcl

import (
	"errors"
	"fmt"
)

// NYI returns an error indicating that the feature is not yet implemented.
func NYI(what string) error {
	return fmt.Errorf("not yet implemented: %s", what)
}

// Must panics if err is not nil, otherwise returns val.
func Must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

// MustOK panics if err is not nil.
func MustOK(err error) {
	if err != nil {
		panic(err)
	}
}

// MustCheck panics if b is false.
func MustCheck(b bool, msg string) {
	if !b {
		panic(errors.New(msg))
	}
}

// RecoverErr recovers from a panic and sets the error pointed to by err.
func RecoverErr(err *error) {
	if r := recover(); r != nil {
		*err = r.(error)
	}
}
