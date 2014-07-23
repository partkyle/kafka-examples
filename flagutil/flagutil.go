package flagutil

import (
	"fmt"
	"strconv"
	"strings"
)

type MultiString []string

func (m *MultiString) String() string {
	return fmt.Sprintf("%v", *m)
}

func (m *MultiString) Set(s string) error {
	*m = strings.Split(s, ",")
	return nil
}

type MultiInt32 []int32

func (m *MultiInt32) String() string {
	return fmt.Sprintf("%v", *m)
}

func (m *MultiInt32) Set(s string) error {
	values := strings.Split(s, ",")

	slice := make([]int32, len(values))
	for i, s := range values {
		value, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		slice[i] = int32(value)
	}

	*m = slice
	return nil
}
