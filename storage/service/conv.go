package service

import (
	"github.com/sadath-12/keywave/internal/vclock"
	"github.com/sadath-12/keywave/storage"
	"github.com/sadath-12/keywave/storage/proto"
)

func toProtoValues(values []storage.Value) []*proto.VersionedValue {
	versionedValues := make(
		[]*proto.VersionedValue, 0, len(values),
	)

	for _, value := range values {
		versionedValues = append(versionedValues, &proto.VersionedValue{
			Version:   vclock.Encode(value.Version),
			Tombstone: value.Tombstone,
			Data:      value.Data,
		})
	}

	return versionedValues
}
