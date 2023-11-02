package skiplist

import (
	"sync/atomic"
	"unsafe"
)

// The unsafe.Pointer type in Go is a special type that allows you to work with pointers in a way that bypasses the type safety provided by the Go programming language. Using unsafe.Pointer gives you the ability to perform low-level pointer operations that are not possible with regular Go code. However, it comes with certain risks and should be used judiciously

type listNodes[K any,V any] [maxHeight]*listNode[K,V]

type listNode[K any,V any] struct {
	key K 
	value atomic.Value
	next [maxHeight]unsafe.Pointer
}

func (n *listNode[K, V]) storeNext(level int,next *listNode[K,V]) {
	atomic.StorePointer(&n.next[level],unsafe.Pointer(next))
}

func (n *listNode[K, V]) loadNext(level int) *listNode[K, V] {
	return (*listNode[K, V])(atomic.LoadPointer(&n.next[level]))
}

func (n *listNode[K, V]) storeValue(value V) {
	n.value.Store(value)
}

func (n *listNode[K, V]) loadValue() (ret V) {
	if val := n.value.Load(); val != nil {
		return val.(V)
	}

	return ret
}