package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-kit/log/level"
	"github.com/jessevdk/go-flags"
)

func main() {
	p := flags.NewParser(&opts, flags.Default)

	if _, err := p.Parse(); err != nil {
		if err.(*flags.Error).Type != flags.ErrHelp {
			fmt.Println("cli error:", err)
		}

		os.Exit(2)
	}

	wg := sync.WaitGroup{}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Initialize all components.
	logger, closeLogger := setupLogger()
	engine, closeEngine := setupEngine(logger)
	_, closeGRPCServer := setupGRPCServer(&wg, engine, logger)

	// Components must be shut down in a particular order.
	shutdownOrder := []shutdownFunc{
		closeGRPCServer,
		closeEngine,
		closeLogger,
	}

	// if opts.RestAPI.Enabled {
		_, closeAPIServer := setupAPIServer(&wg, logger)
		shutdownOrder = append([]shutdownFunc{closeAPIServer}, shutdownOrder...)
	// }

	// Block until we receive a signal to shut down.
	<-interrupt
	level.Info(logger).Log("msg", "received interrupt signal, shutting down")

	// Shutdown all components.
	for _, f := range shutdownOrder {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		if err := f(ctx); err != nil {
			level.Error(logger).Log("msg", "failed to shutdown component", "err", err)
		}

		cancel()
	}

	// Wait for all components to finish background tasks.
	wg.Wait()
}
