package grpc

import (
	"context"
	"fmt"

	membershippb "github.com/sadath-12/keywave/membership/proto"
	"github.com/sadath-12/keywave/nodeapi"
	replicationpb "github.com/sadath-12/keywave/replication/proto"
	storagepb "github.com/sadath-12/keywave/storage/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

func Dial(ctx context.Context, addr string) (nodeapi.Client, error) {
	creds := insecure.NewCredentials()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial failed: %w", err)
	}

	storageClient := storagepb.NewStorageServiceClient(conn)
	replicationClient := replicationpb.NewReplicationClient(conn)
	membershipClient := membershippb.NewMembershipClient(conn)

	c := &Client{
		storageClient:     storageClient,
		replicationClient: replicationClient,
		membershipClient:  membershipClient,
	}

	c.addOnCloseHook(conn.Close)

	return c, nil
}
