package common

import (
	"fmt"
)

func NYI(what string) error {
	return fmt.Errorf("not yet implemented: %s", what)
}

func Must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

func MustOK(err error) {
	if err != nil {
		panic(err)
	}
}
