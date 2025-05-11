package main

import (
	"context"
	pb "go_c-_test_work/api/proto"
	"go_c-_test_work/grpc_service/config"
	mainbroker "go_c-_test_work/main_broker"
	"go_c-_test_work/subpub"
	syslog "log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	log.Info().Msg("main broker: Starting Core Broker Process (using PubSub interface)...")

	appCfg, err := config.LoadConfig(".")
	if err != nil {
		syslog.Fatalf("main broker: Failed to load configuration: %v", err)
	}

	brokerListenAddress := appCfg.Server.GRPCAddress
	if brokerListenAddress == "" {
		brokerListenAddress = "localhost:8080"
		log.Info().Msg("main broker: main broker adress not in config, using default: localhost:8080")
	}

	bus := subpub.NewSubPub()

	log.Info().Msg("main broker: main SubPub bus initialized.")

	lis, err := net.Listen("tcp", brokerListenAddress)
	if err != nil {
		log.Info().Msgf("main broker: Failed to listen on %s: %v", brokerListenAddress, err)
	}

	brokerPubSubServer := mainbroker.NewBrokerPubSubImpl(bus)

	grpcBrokerServer := grpc.NewServer()

	pb.RegisterPubSubServer(grpcBrokerServer, brokerPubSubServer)
	reflection.Register(grpcBrokerServer)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info().Msgf("main broker: gRPC Broker listening on %s", brokerListenAddress)
		if err := grpcBrokerServer.Serve(lis); err != nil {
			syslog.Fatalf("main broker: gRPC Broker service failed: %v", err)
		}
	}()

	// gracefull shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Info().Msgf("main broker: Received signal %s. Shutting down broker...", sig)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Info().Msg("main broker: Stopping gRPC Broker server...")
	grpcBrokerServer.GracefulStop()
	log.Info().Msg("main broker: gRPC Broker server stopped.")

	log.Info().Msg("main broker: Closing core SubPub bus...")
	if err := bus.Close(shutdownCtx); err != nil {
		log.Error().Msgf("main broker: Error closing subpub bus: %v", err)
	}
	log.Info().Msg("main broker: Core SubPub bus closed.")

	wg.Wait()
	log.Info().Msg("main broker: Core Broker Process shut down gracefully.")
}
