package service

import (
	"fmt"

	"github.com/sadath-12/keywave/internal/generic"
	"github.com/sadath-12/keywave/internal/vclock"
	"github.com/sadath-12/keywave/membership"
)

type mergeResult struct {
	version       string
	values        []nodeValue
	staleReplicas []membership.NodeID
}

func mergeVersions(values []nodeValue) (mergeResult, error) {
	valueVersion := make([]vclock.Version, len(values))

	// Keep decoded version for each value.
	for i, v := range values {
		version, err := vclock.Decode(v.Version)
		if err != nil {
			return mergeResult{}, fmt.Errorf("invalid version: %w", err)
		}

		valueVersion[i] = version
	}

	// Merge all versions into one.
	mergedVersion := vclock.Empty()
	for i := 0; i < len(values); i++ {
		mergedVersion = vclock.Merge(mergedVersion, valueVersion[i])
	}

	if len(values) < 2 {
		return mergeResult{
			version: vclock.Encode(mergedVersion),
			values:  values,
		}, nil
	}

	var staleNodes []membership.NodeID

	uniqueValues := make(map[string]nodeValue)

	// Identify the highest version among all values.
	highest := valueVersion[0]
	for i := 1; i < len(values); i++ {
		if vclock.Compare(highest, valueVersion[i]) == vclock.Before {
			highest = valueVersion[i]
		}
	}

	for i := 0; i < len(values); i++ {
		value := values[i]

		// Ignore the values that clearly precede the highest version.
		// Keep track of the replicas that returned outdated values.
		if vclock.Compare(valueVersion[i], highest) == vclock.Before {
			staleNodes = append(staleNodes, value.NodeID)
			continue
		}

		// Keep unique values only, based on the version.
		if _, ok := uniqueValues[value.Version]; !ok {
			uniqueValues[value.Version] = value
		}
	}

	fmt.Println("merged version: ",mergedVersion)

	return mergeResult{
		values:        generic.MapValues(uniqueValues),
		version:       vclock.Encode(mergedVersion),
		staleReplicas: staleNodes,
	}, nil
}
