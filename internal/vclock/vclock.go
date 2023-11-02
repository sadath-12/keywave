package vclock

import "github.com/sadath-12/keywave/internal/generic"

type Causality int 

const (
	Before Causality = iota + 1
	Concurrent
	After
	Equal
)

func (c Causality) String() string {
	switch c {
	case Before:
		return "Before"
	case Concurrent:
		return "Concurrent"
	case After:
		return "After"
	case Equal:
		return "Equal"
	default:
		return ""
	}
}

type Version map[uint32]uint64

// Empty returns a new version vector.
func Empty() Version {
	return make(Version)
}
func (vc Version) String() string {
	return Encode(vc)
}

// Copy returns a copy of the vector clock.
func (vc Version) Copy() Version {
	newvec := make(Version, len(vc))
	generic.MapCopy(newvec, vc)
	return newvec
}


func (vc Version) Increment(nodeID uint32) {
	vc[nodeID]++

	if vc[nodeID] == 0 {
		panic("clock value overflow")
	}
}

func Compare(a, b Version) Causality {
	var greater, less bool

	for _, key := range generic.MapKeys(a, b) {
		if a[key] > b[key] {
			greater = true
		} else if a[key] < b[key] {
			less = true
		}
	}

	switch {
	case greater && !less:
		return After
	case less && !greater:
		return Before
	case !less && !greater:
		return Equal
	default:
		return Concurrent
	}
}

func IsEqual(a, b Version) bool {
	return Compare(a, b) == Equal
}

func Merge(a, b Version) Version {
	keys := generic.MapKeys(a, b)
	merged := make(Version, len(keys))

	for _, key := range keys {
		if a[key] > b[key] {
			merged[key] = a[key]
		} else {
			merged[key] = b[key]
		}
	}

	return merged
}
