package skiplist

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
)

const (
	maxHeight    = 12
	branchFactor = 4
)

// Comparator is a function that compares two keys.
// It returns a negative number if a < b, 0 if a == b, and a positive number if a > b.
type Comparator[K any] func(a, b K) int

// Skiplist is a generic skiplist implementation. It is thread safe and supports concurrent reads and writes.
// It allows to multiple readers to access the list simultaneously, but only a single writer. The writer
// does not block the readers, and the readers do not block the writer.
type Skiplist[K any, V any] struct {
	head        *listNode[K, V]
	compareKeys Comparator[K]
	mut         sync.Mutex
	height      int32
	size        int32
}

func New[K any, V any](comparator Comparator[K]) *Skiplist[K, V] {
	head := &listNode[K, V]{}

	return &Skiplist[K, V]{
		compareKeys: comparator,
		head:        head,
	}
}

func (l *Skiplist[K, V]) loadHeight() int {
	return int(atomic.LoadInt32(&l.height))
}

func (l *Skiplist[K, V]) storeHeight(newHeight int) {
	atomic.StoreInt32(&l.height, int32(newHeight))
}

func (l *Skiplist[K, V]) Height() int {
	return l.loadHeight()
}

// Size returns the number of key-value pairs in the list.
func (l *Skiplist[K, V]) Size() int {
	return int(atomic.LoadInt32(&l.size))
}

func (l *Skiplist[K, V]) findLess(key K, searchPath *listNodes[K, V], stopAt int) *listNode[K, V] {
	height := l.loadHeight()
	if height == 0 {
		return nil
	}

	level := height - 1
	node := l.head

	for {
		next := node.loadNext(level)

		if next != nil && l.compareKeys(key, next.key) > 0 {
			node = next
			continue
		}

		if searchPath != nil {
			searchPath[level] = node
		}

		if level == stopAt {
			break
		}

		level--
	}

	return node
}

// will insert an item to list right before the its lesser value from bottom down approach
func (l *Skiplist[K, V]) Insert(key K, value V) {
	l.mut.Lock()
	defer l.mut.Unlock()

	var searchPath listNodes[K, V]

	l.findLess(key, &searchPath, 0)

	fmt.Println("searchpath is", searchPath)
	if searchPath[0] != nil {
		node := searchPath[0].loadNext(0)
		if node != nil && l.compareKeys(key, node.key) == 0 {
			node.storeValue(value)
			return
		}
	}

	newnode := &listNode[K, V]{key: key}
	newnode.storeValue(value)

	height := l.Height()
	fmt.Println("current height is", height)
	newheight := randomHeight()
	fmt.Println("new height is", newheight)

	fmt.Println("head is",l.head)

	if newheight > height {
		for level := height; level < newheight; level++ {
			searchPath[level] = l.head
		}

		l.storeHeight(newheight)
	}

	for level := 0; level < newheight; level++ {
		next := searchPath[level].loadNext(level)
		newnode.storeNext(level, next)
	}

	for level := 0; level < newheight; level++ {
		searchPath[level].storeNext(level, newnode)
	}
	atomic.AddInt32(&l.size, 1)
}

func (l *Skiplist[K, V]) Remove(key K) bool {
	l.mut.Lock()
	defer l.mut.Unlock()

	var searchPath listNodes[K, V]

	l.findLess(key, &searchPath, 0)

	if searchPath[0] == nil {
		return false
	}
	node := searchPath[0].loadNext(0)
	if node == nil || l.compareKeys(key, node.key) != 0 {
		return false
	}

	for level := 0; level < l.loadHeight(); level++ {
		prev := searchPath[level]
		next := prev.loadNext(level)

		if next != node {
			break
		}

		prev.storeNext(level, node.loadNext(level))

	}

	if atomic.AddInt32(&l.size, -1) < 0 {
		panic("skiplist: negative size")
	}

	return true
}
func (l *Skiplist[K, V]) Scan() *Iterator[K, V] {
	return newIterator(l.head.loadNext(0), 0, l.compareKeys, nil)
}
func (l *Skiplist[K, V]) ScanFrom(key K) *Iterator[K, V] {
	var node *listNode[K, V]
	if prev := l.findLess(key, nil, 0); prev != nil {
		node = prev.loadNext(0)
	}

	return newIterator(node, 0, l.compareKeys, nil)
}

// Contains returns true if the list contains the given key.
func (l *Skiplist[K, V]) Contains(key K) bool {
	var node *listNode[K, V]

	if prev := l.findLess(key, nil, 0); prev != nil {
		node = prev.loadNext(0)
	}

	if node == nil || l.compareKeys(key, node.key) != 0 {
		return false
	}

	return true
}

// Get returns the value for the given key. If the key is not found, ErrNotFound is returned.
func (l *Skiplist[K, V]) Get(key K) (ret V, found bool) {
	var node *listNode[K, V]

	if prev := l.findLess(key, nil, 0); prev != nil {
		node = prev.loadNext(0)
	}

	if node == nil || l.compareKeys(key, node.key) != 0 {
		return ret, false
	}

	return node.loadValue(), true
}

// LessOrEqual returns the value for the key that is less than the given key.
func (l *Skiplist[K, V]) LessOrEqual(key K) (retk K, retv V, found bool) {
	node := l.findLess(key, nil, 0)
	if node == nil {
		return retk, retv, false
	}

	for {
		next := node.loadNext(0)

		if next != nil && l.compareKeys(key, next.key) >= 0 {
			node = next
			continue
		}

		break
	}

	if node == l.head {
		return retk, retv, false
	}

	return node.key, node.loadValue(), true
}

func randomHeight() int {
	height := 1

	for height < maxHeight && ((rand.Int() % branchFactor) == 0) {
		height++
	}
	return height
}
