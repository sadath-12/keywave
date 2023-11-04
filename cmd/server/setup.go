package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"

	"github.com/sadath-12/keywave/membership"

	"github.com/sadath-12/keywave/api"
	membershippb "github.com/sadath-12/keywave/membership/proto"
	membershipsvc "github.com/sadath-12/keywave/membership/service"
	nodeapigrpc "github.com/sadath-12/keywave/nodeapi/grpc"

	"github.com/sadath-12/keywave/storage"
	"github.com/sadath-12/keywave/storage/inmemory"
	storagepb "github.com/sadath-12/keywave/storage/proto"
	storagesvc "github.com/sadath-12/keywave/storage/service"
)

type shutdownFunc func(ctx context.Context) error

var noopShutdown = func(ctx context.Context) error { return nil }

func setupLogger() (kitlog.Logger, shutdownFunc) {
	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))

	if !opts.Verbose {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	return logger, noopShutdown
}

func setupCluster(logger kitlog.Logger) (*membership.SWIMCluster, shutdownFunc) {
	conf := membership.DefaultConfig()
	conf.NodeID = membership.NodeID(opts.Node.ID)
	conf.NodeName = opts.Node.Name
	conf.LocalAddr = opts.GRPC.LocalAddr
	conf.PublicAddr = opts.GRPC.PublicAddr
	conf.ProbeTimeout = time.Millisecond * time.Duration(opts.Cluster.ProbeTimeout)
	conf.ProbeInterval = time.Millisecond * time.Duration(opts.Cluster.ProbeInterval)
	conf.IndirectNodes = opts.Cluster.ProbeIndirectNodes
	conf.Dialer = nodeapigrpc.Dial
	conf.Logger = logger

	cluster := membership.NewSWIM(conf)
	cluster.Start()

	shutdown := func(ctx context.Context) error {
		logger.Log("msg", "leaving cluster")

		if err := cluster.Leave(ctx); err != nil {
			return fmt.Errorf("failed to leave cluster: %w", err)
		}

		return nil
	}

	return cluster, shutdown
}

func setupAPIServer(wg *sync.WaitGroup, cluster membership.Cluster, logger kitlog.Logger) (*http.Server, shutdownFunc) {
	restAPI := &http.Server{
		Addr:    opts.RestAPI.BindAddr,
		Handler: api.CreateRouter(cluster),
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := restAPI.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				panic(fmt.Sprintf("failed to start REST API server: %v", err))
			}
		}
	}()

	shutdown := func(ctx context.Context) error {
		logger.Log("msg", "shutting down API server")

		if err := restAPI.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown REST API server: %w", err)
		}

		return nil
	}

	return restAPI, shutdown
}

func setupGRPCServer(
	wg *sync.WaitGroup,
	engine storage.Engine,
	cluster membership.Cluster,

	logger kitlog.Logger,
) (*grpc.Server, shutdownFunc) {
	grpcServer := grpc.NewServer()

	storageService := storagesvc.New(engine, opts.Node.ID)
	storagepb.RegisterStorageServiceServer(grpcServer, storageService)

	membershipService := membershipsvc.NewMembershipService(cluster)
	membershippb.RegisterMembershipServer(grpcServer, membershipService)

	

	wg.Add(1)

	go func() {
		defer wg.Done()

		listener, err := net.Listen("tcp", opts.GRPC.BindAddr)
		if err != nil {
			panic(fmt.Sprintf("failed to create GRPC listener: %v", err))
		}

		if err := grpcServer.Serve(listener); err != nil {
			panic(fmt.Sprintf("failed to start GRPC server: %v", err))
		}
	}()

	shutdown := func(ctx context.Context) error {
		logger.Log("msg", "shutting down GRPC server")
		grpcServer.GracefulStop()
		return nil
	}

	return grpcServer, shutdown
}

func setupEngine(logger kitlog.Logger) (storage.Engine, shutdownFunc) {
	fmt.Println("opts is", opts)
	// if opts.Storage.InMemory {

	fmt.Println("using memory true------------")
	level.Info(logger).Log("msg", "using in-memory storage engine")
	return inmemory.New(), noopShutdown
	// }

	// config := lsmtree.DefaultConfig()
	// config.MaxMemtableSize = opts.Storage.MemtableSize
	// config.DataRoot = opts.Storage.DataRoot
	// config.UseMmap = true
	// config.Logger = logger

	// lsmt, err := lsmtree.Create(config)
	// if err != nil {
	// 	panic(fmt.Sprintf("failed to create LSM tree: %v", err))
	// }

	// shutdown := func(ctx context.Context) error {
	// 	logger.Log("msg", "closing LSM tree")
	// 	return lsmt.Close()
	// }

	// engine := lsmtengine.New(lsmt)

	// return engine, shutdown
}
