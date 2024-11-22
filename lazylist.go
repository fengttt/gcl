package gcl

import (
	"sync"
	"sync/atomic"
)

//
// A sorted list, no duplicate keys.   See the LazyList described
// in "The Art of Multiprocessor Programming" by Maurice Herlihy
//

type lzNode[K any, V any] struct {
	key    K
	value  V
	next   atomic.Pointer[lzNode[K, V]]
	marked atomic.Bool
	sync.Mutex
}

func (n *lzNode[K, V]) isMarked() bool {
	return n.marked.Load()
}

func (n *lzNode[K, V]) isNotMarked() bool {
	return !n.marked.Load()
}

// LzIter is an iterator for LazyList
type LzIter[K any, V any] struct {
	// range [ka, kz)
	ka, kz K
	curr   *lzNode[K, V]
}

// GetKey returns the key of the current node of the iterator
func (it *LzIter[K, V]) GetKey() K {
	return it.curr.key
}

// GetValue returns the value of the current node of the iterator
func (it *LzIter[K, V]) GetValue() V {
	return it.curr.value
}

type LazyList[K any, V any] struct {
	// sentinel nodes
	head, tail *lzNode[K, V]
	less       func(a, b K) bool
	eq         func(a, b K) bool
}

func NewLazyList[K any, V any](less func(a, b K) bool, eq func(a, b K) bool) *LazyList[K, V] {
	head := &lzNode[K, V]{}
	tail := &lzNode[K, V]{}
	head.next.Store(tail)
	return &LazyList[K, V]{head: head, tail: tail, less: less, eq: eq}
}

// Validate that pred and curr are still in the list and adjacent
func (l *LazyList[K, V]) validate(pred, curr *lzNode[K, V]) bool {
	return pred.isNotMarked() && curr.isNotMarked() && pred.next.Load() == curr
}

// return if pred, curr are still valid, and if valid, the result of add
func (l *LazyList[K, V]) lockAdd(pred, curr *lzNode[K, V], key K, val V) (bool, bool) {
	pred.Lock()
	defer pred.Unlock()
	curr.Lock()
	defer curr.Unlock()
	if l.validate(pred, curr) {
		if l.eq(curr.key, key) {
			// already exists.   return valid but not added
			return true, false
		} else {
			// added kv
			newNode := &lzNode[K, V]{key: key, value: val}
			newNode.next.Store(curr)
			pred.next.Store(newNode)
			return true, true
		}
	}
	return false, false
}

// Add key, val to the list.  Return true if added, false if already exists
func (l *LazyList[K, V]) Add(key K, val V) bool {
	for {
		pred := l.head
		curr := pred.next.Load()
		// walk the list
		for curr != l.tail {
			// if curr is marked for deletion, move to next.
			marked := curr.isMarked()
			if marked {
				pred = curr
				curr = pred.next.Load()
			} else {
				if l.less(curr.key, key) {
					pred = curr
					curr = pred.next.Load()
				} else {
					break
				}
			}
		}

		valid, ret := l.lockAdd(pred, curr, key, val)
		if valid {
			return ret
		}
	} // for loop
}

// return if pred, curr are still valid, and if valid, the result of remove
func (l *LazyList[K, V]) lockRemove(pred, curr *lzNode[K, V], key K) (bool, bool) {
	pred.Lock()
	defer pred.Unlock()
	curr.Lock()
	defer curr.Unlock()

	// still valid?
	if l.validate(pred, curr) {
		if !l.eq(curr.key, key) {
			// not found
			return true, false
		} else {
			// logical remove, mark
			curr.marked.Store(true)
			// physical remove
			pred.next.Store(curr.next.Load())
			return true, true
		}
	}
	return false, false
}

// Remove key from the list.  Return true if removed, false if not found
func (l *LazyList[K, V]) Remove(key K) bool {
	for {
		pred := l.head
		curr := pred.next.Load()
		// walk the list
		for curr != l.tail && l.less(curr.key, key) {
			pred = curr
			curr = pred.next.Load()
		}

		valid, ret := l.lockRemove(pred, curr, key)
		if valid {
			return ret
		}
	} // for loop
}

// Lookup
func (l *LazyList[K, V]) Lookup(key K) (V, bool) {
	curr := l.head.next.Load()
	for curr != l.tail && l.less(curr.key, key) {
		curr = curr.next.Load()
	}
	if l.eq(curr.key, key) && curr.isNotMarked() {
		return curr.value, true
	}
	return l.head.value, false
}

// Iterator
func (l *LazyList[K, V]) Iterator(ka, kz K) *LzIter[K, V] {
	curr := l.head.next.Load()
	for curr != l.tail && (curr.isNotMarked() && l.less(curr.key, ka)) {
		curr = curr.next.Load()
	}

	if curr == l.tail || !l.less(curr.key, kz) {
		return nil
	}

	return &LzIter[K, V]{ka: ka, kz: kz, curr: curr}
}

func (l *LazyList[K, V]) Next(it *LzIter[K, V]) *LzIter[K, V] {
	it.curr = it.curr.next.Load()
	for it.curr != l.tail && it.curr.isMarked() {
		it.curr = it.curr.next.Load()
	}

	if it.curr == l.tail || !l.less(it.curr.key, it.kz) {
		return nil
	}
	return it
}
