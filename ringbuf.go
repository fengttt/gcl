package gcl

import "fmt"

type RingBuffer[T any] struct {
	buf        []T
	begin, end int
}

func NewRingBuffer[T any](size int) *RingBuffer[T] {
	if size <= 0 {
		return nil
	}
	return &RingBuffer[T]{buf: make([]T, size+1)}
}

func (rb *RingBuffer[T]) advance(n int) int {
	ret := n + 1
	if ret == len(rb.buf) {
		ret = 0
	}
	return ret
}

func (rb *RingBuffer[T]) Len() int {
	n := rb.end - rb.begin
	if n < 0 {
		n += len(rb.buf)
	}
	return n
}

// Put puts v into the ring buffer, will overwrite the oldest element if the buffer is full.
func (rb *RingBuffer[T]) Push(v T) {
	rb.buf[rb.end] = v
	rb.end = rb.advance(rb.end)
	if rb.end == rb.begin {
		rb.begin = rb.advance(rb.begin)
	}
}

func (rb *RingBuffer[T]) ReplaceLast(v T) {
	if rb.Len() == 0 {
		rb.Push(v)
		return
	}

	// replacement position
	pos := rb.end - 1
	if rb.end == 0 {
		pos = len(rb.buf) - 1
	}
	rb.buf[pos] = v
}

func (rb *RingBuffer[T]) PopFront() (T, error) {
	if rb.Len() == 0 {
		var zero T
		return zero, fmt.Errorf("RingBuffer is empty")
	}

	var v = rb.buf[rb.begin]
	rb.begin = rb.advance(rb.begin)
	return v, nil
}

func (rb *RingBuffer[T]) PopBack() (T, error) {
	if rb.Len() == 0 {
		var zero T
		return zero, fmt.Errorf("RingBuffer is empty")
	}
	var v = rb.buf[rb.end]
	if rb.end == 0 {
		rb.end = len(rb.buf) - 1
	} else {
		rb.end--
	}
	return v, nil
}

func (rb *RingBuffer[T]) Get(n int) (T, error) {
	if n >= rb.Len() {
		var zero T
		return zero, fmt.Errorf("RingBuffer index out of range: %d", n)
	}

	var i = rb.begin + n
	if i >= len(rb.buf) {
		i -= len(rb.buf)
	}
	return rb.buf[i], nil
}

func (rb *RingBuffer[T]) MustGet(n int) T {
	if n >= rb.Len() {
		panic(fmt.Errorf("RingBuffer index out of range: %d", n))
	}

	var i = rb.begin + n
	if i >= len(rb.buf) {
		i -= len(rb.buf)
	}
	return rb.buf[i]
}
