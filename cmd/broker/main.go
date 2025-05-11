package main

import (
	"context"
	pb "go_c-_test_work/api/proto"
	"go_c-_test_work/grpc_service/config"
	mainbroker "go_c-_test_work/main_broker"
	"go_c-_test_work/subpub"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	log.Println("BROKER_MAIN: Starting Core Broker Process (using PubSub interface)...")

	appCfg, err := config.LoadConfig(".")
	if err != nil {
		log.Fatalf("BROKER_MAIN: Failed to load configuration: %v", err)
	}

	brokerListenAddress := appCfg.Server.GRPCAddress
	if brokerListenAddress == "" {
		brokerListenAddress = "localhost:8080"
	}

	bus := subpub.NewSubPub()

	log.Println("BROKER_MAIN: main SubPub bus initialized.")

	lis, err := net.Listen("tcp", brokerListenAddress)
	if err != nil {
		log.Fatalf("BROKER_MAIN: Failed to listen on %s: %v", brokerListenAddress, err)
	}

	brokerPubSubServer := mainbroker.NewBrokerPubSubImpl(bus)

	grpcBrokerServer := grpc.NewServer()

	pb.RegisterPubSubServer(grpcBrokerServer, brokerPubSubServer)
	reflection.Register(grpcBrokerServer)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("BROKER_MAIN: gRPC Broker listening on %s", brokerListenAddress)
		if err := grpcBrokerServer.Serve(lis); err != nil {
			log.Fatalf("BROKER_MAIN: gRPC Broker service failed: %v", err)
		}
	}()

	// gracefull shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("BROKER_MAIN: Received signal %s. Shutting down broker...", sig)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Println("BROKER_MAIN: Stopping gRPC Broker server...")
	grpcBrokerServer.GracefulStop()
	log.Println("BROKER_MAIN: gRPC Broker server stopped.")

	log.Println("BROKER_MAIN: Closing core SubPub bus...")
	if err := bus.Close(shutdownCtx); err != nil {
		log.Printf("BROKER_MAIN: Error closing subpub bus: %v", err)
	}
	log.Println("BROKER_MAIN: Core SubPub bus closed.")

	wg.Wait()
	log.Println("BROKER_MAIN: Core Broker Process shut down gracefully.")
}
