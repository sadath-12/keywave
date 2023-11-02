package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/sadath-12/keywave/api"
	"github.com/sadath-12/keywave/storage"
	"github.com/sadath-12/keywave/storage/inmemory"
	storagepb "github.com/sadath-12/keywave/storage/proto"
	storagesvc "github.com/sadath-12/keywave/storage/service"
	"google.golang.org/grpc"
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

func setupEngine(logger kitlog.Logger) (storage.Engine, shutdownFunc) {
	fmt.Println("opts is", opts)

	level.Info(logger).Log("msg", "using in-memory storage engine")
	return inmemory.New(), noopShutdown

}

func setupAPIServer(wg *sync.WaitGroup, logger kitlog.Logger) (*http.Server, shutdownFunc) {
	restAPI := &http.Server{
		Addr:    opts.RestAPI.BindAddr,
		Handler: api.CreateRouter(),
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
	logger kitlog.Logger,
) (*grpc.Server, shutdownFunc) {

	grpcServer := grpc.NewServer()

	storageService := storagesvc.New(engine, 1)
	storagepb.RegisterStorageServiceServer(grpcServer, storageService)

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
