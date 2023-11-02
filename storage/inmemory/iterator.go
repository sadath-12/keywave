package inmemory

import (
	"github.com/sadath-12/keywave/internal/skiplist"
	"github.com/sadath-12/keywave/storage"
)

type Iterator struct {
	it  *skiplist.Iterator[string, []storage.Value]
	val []storage.Value
	key string
}

func (i *Iterator) Next() error {
	if i.it.HasNext() {
		i.key, i.val = i.it.Next()
		return nil
	}

	return storage.ErrNoMoreItems
}

func (i *Iterator) Item() (key string, values []storage.Value) {
	return i.key, i.val
}
