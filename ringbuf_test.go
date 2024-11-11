package common

import "testing"

func getAll(rb *RingBuffer[int]) []int {
	var ret []int
	for i := 0; i < rb.Len(); i++ {
		ret = append(ret, rb.MustGet(i))
	}
	return ret
}

func check(t *testing.T, a, b []int) {
	if len(a) != len(b) {
		t.Fatal("len(a) != len(b)")
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			t.Fatal("a[i] != b[i]")
		}
	}
}

func TestChat(t *testing.T) {
	rb := NewRingBuffer[int](3)
	rb.Push(1)
	rb.Push(2)
	check(t, []int{1, 2}, getAll(rb))

	rb.Push(3)
	check(t, []int{1, 2, 3}, getAll(rb))

	rb.Push(4)
	check(t, []int{2, 3, 4}, getAll(rb))

	rb.PopBack()
	check(t, []int{2, 3}, getAll(rb))

	rb.Push(4)
	rb.PopFront()
	check(t, []int{3, 4}, getAll(rb))

	rb.ReplaceLast(100)
	check(t, []int{3, 100}, getAll(rb))
}
