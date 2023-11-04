package grpc

import (
	"context"

	"github.com/sadath-12/keywave/membership/proto"
	"github.com/sadath-12/keywave/nodeapi"
	storagepb "github.com/sadath-12/keywave/storage/proto"
)

type Client struct {
	storageClient    storagepb.StorageServiceClient
	membershipClient proto.MembershipClient
	onClose          []func() error
	closed           uint32
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

func (c *Client) PullPushState(ctx context.Context, nodes []nodeapi.NodeInfo) ([]nodeapi.NodeInfo, error) {
	req := &proto.PullPushStateRequest{
		Nodes: make([]*proto.Node, len(nodes)),
	}
	for idx, n := range nodes {
		req.Nodes[idx] = &proto.Node{
			Id:         uint32(n.ID),
			Name:       n.Name,
			Address:    n.Addr,
			Generation: n.Gen,
			Error:      n.Error,
			RunId:      n.RunID,
		}

		switch n.Status {
		case nodeapi.NodeStatusHealthy:
			req.Nodes[idx].Status = proto.Status_HEALTHY
		case nodeapi.NodeStatusUnhealthy:
			req.Nodes[idx].Status = proto.Status_UNHEALTHY
		case nodeapi.NodeStatusLeft:
			req.Nodes[idx].Status = proto.Status_LEFT
		}
	}

	resp, err := c.membershipClient.PullPushState(ctx, req)
	if err != nil {
		return nil, err
	}
	nodes = make([]nodeapi.NodeInfo, len(resp.Nodes))
	for idx, n := range resp.Nodes {
		nodes[idx] = nodeapi.NodeInfo{
			ID:    nodeapi.NodeID(n.Id),
			Name:  n.Name,
			Gen:   n.Generation,
			Addr:  n.Address,
			RunID: n.RunId,
			Error: n.Error,
		}

		switch n.Status {
		case proto.Status_HEALTHY:
			nodes[idx].Status = nodeapi.NodeStatusHealthy
		case proto.Status_UNHEALTHY:
			nodes[idx].Status = nodeapi.NodeStatusUnhealthy
		case proto.Status_LEFT:
			nodes[idx].Status = nodeapi.NodeStatusLeft
		}
	}

	return nodes, nil
}
