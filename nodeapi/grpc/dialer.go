package grpc

import (
	"context"
	"fmt"

	storagepb "github.com/sadath-12/keywave/storage/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

func Dial(ctx context.Context, addr string) (storagepb.StorageServiceClient, error) {
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

	c := &Client{

		storageClient: storageClient,
	}

	c.addOnCloseHook(conn.Close)

	return storageClient, nil
}
