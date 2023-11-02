package grpc

import (
	"context"

	"github.com/sadath-12/keywave/nodeapi"
	storagepb "github.com/sadath-12/keywave/storage/proto"
)

type Client struct {
	storageClient storagepb.StorageServiceClient
	onClose       []func() error
	closed        uint32
}

func (c *Client) addOnCloseHook(f func() error) {
	c.onClose = append(c.onClose, f)
}

func (c *Client) StorageGet(ctx context.Context, key string) (*nodeapi.StorageGetResult, error) {
	resp, err := c.storageClient.Get(ctx, &storagepb.GetRequest{
		Key: key,
	})

	if err != nil {
		return nil, err
	}

	versions := make([]nodeapi.VersionedValue, len(resp.Value))

	for idx, v := range resp.Value {
		versions[idx] = nodeapi.VersionedValue{
			Tombstone: v.Tombstone,
			Version:   v.Version,
			Data:      v.Data,
		}
	}

	return &nodeapi.StorageGetResult{
		Versions: versions,
	}, nil
}

func (c *Client) StoragePut(ctx context.Context, key string, value nodeapi.VersionedValue, primary bool) (*nodeapi.StoragePutResult, error) {
	resp, err := c.storageClient.Put(ctx, &storagepb.PutRequest{
		Key:     key,
		Primary: primary,
		Value: &storagepb.VersionedValue{
			Data:      value.Data,
			Version:   value.Version,
			Tombstone: value.Tombstone,
		},
	})

	if err != nil {
		return nil, err
	}

	return &nodeapi.StoragePutResult{
		Version: resp.Version,
	}, nil
}
