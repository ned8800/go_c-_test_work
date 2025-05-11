package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	pb "go_c-_test_work/api/proto"
	"go_c-_test_work/grpc_service/config"
	"io"
	syslog "log"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	log.Info().Msg("PUBSUBSVC_MAIN: Starting External PubSub Service Process (using PubSub interface for broker)...")

	cfg, err := config.LoadConfig(".")
	if err != nil {
		syslog.Fatalf("PUBSUBSVC_MAIN: Failed to load configuration: %v", err)
	}
	// externalListenAddress := cfg.Server.ClientGRPCAddress
	brokerServiceAddress := cfg.Server.GRPCAddress

	// if externalListenAddress == "" {
	// 	externalListenAddress = "localhost:50051"
	// 	log.Printf("PUBSUBSVC_MAIN: grpc_address not in config, using default %s", externalListenAddress)
	// }
	// if brokerServiceAddress == "" {
	// 	brokerServiceAddress = "localhost:60061"
	// 	log.Printf("PUBSUBSVC_MAIN: broker_internal_address not in config, using default %s", brokerServiceAddress)
	// }

	// externalSvcImpl, err := grpcservice.NewExternalPubSubImpl(brokerServiceAddress)
	// if err != nil {
	// 	log.Fatalf("PUBSUBSVC_MAIN: Failed to create ExternalPubSubImpl (connector to broker): %v", err)
	// }
	// // defer externalSvcImpl.CloseClientConnection() // If you implement this

	// lis, err := net.Listen("tcp", externalListenAddress)
	// if err != nil {
	// 	log.Fatalf("PUBSUBSVC_MAIN: Failed to listen on %s: %v", externalListenAddress, err)
	// }

	// externalGrpcServer := grpc.NewServer()
	// pb.RegisterPubSubServer(externalGrpcServer, externalSvcImpl) // Registering PubSubService
	// reflection.Register(externalGrpcServer)

	// var wg sync.WaitGroup
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	log.Printf("PUBSUBSVC_MAIN: External PubSub gRPC service listening on %s", externalListenAddress)
	// 	if err := externalGrpcServer.Serve(lis); err != nil {
	// 		log.Fatalf("PUBSUBSVC_MAIN: External PubSub gRPC service failed: %v", err)
	// 	}
	// }()

	// quit := make(chan os.Signal, 1)
	// signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// sig := <-quit
	// log.Printf("PUBSUBSVC_MAIN: Received signal %s. Shutting down...", sig)

	// log.Println("PUBSUBSVC_MAIN: Stopping external gRPC server...")
	// externalGrpcServer.GracefulStop()
	// log.Println("PUBSUBSVC_MAIN: External gRPC server stopped.")

	// wg.Wait()
	// log.Println("PUBSUBSVC_MAIN: External PubSub Service Process shut down gracefully.")

	log.Info().Msg("Trying to connect to main broker")
	conn, err := grpc.NewClient(
		brokerServiceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		syslog.Fatalf("error couldnt connect to grpc: %v", err)
	}
	log.Info().Msg("main broker connection opened successfully")

	defer func() {
		if clErr := conn.Close(); clErr != nil {
			log.Error().Msg("couldn't close auth main broker grpc connection")
		}
	}()

	logKey := "abracadabra"
	brokerClient := pb.NewPubSubClient(conn)
	logReq := &pb.SubscribeRequest{Key: logKey}

	// Subscribe
	streamClient, err := brokerClient.Subscribe(context.Background(), logReq)
	if err != nil {
		syslog.Fatalf("error couldnt connect to grpc: %v", err)
	}
	log.Info().Msgf("MAIN_DIRECT_LOGGER: Subscribed to broker for key '%s'. Listening for events...", logKey)

	// Listen for events
	go func(strCl *pb.PubSub_SubscribeClient) {
		for {
			event, err := (*strCl).Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Info().Msgf("MAIN_DIRECT_LOGGER: Broker stream for key '%s' ended (EOF). Exiting.", logKey)
				} else if status.Code(err) == codes.Canceled {
					log.Info().Msgf("MAIN_DIRECT_LOGGER: Broker stream for key '%s' canceled. Exiting.", logKey)
				} else {
					log.Info().Msgf("MAIN_DIRECT_LOGGER: Error receiving from broker stream for key '%s': %v. Exiting.", logKey, err)
				}
				break
			}
			log.Info().Msgf("MAIN_DIRECT_LOGGER: [BROKER EVENT via main] Key: %s, Data: %s", logKey, event.GetData())
		}
	}(&streamClient)

	cReader := bufio.NewReader(os.Stdin)
	for {
		input, err := cReader.ReadString('\n')
		if err != nil {
			break
		}
		input = strings.TrimSpace(input)
		fmt.Println("You entered:", input)
		newPublishRequest := pb.PublishRequest{
			Key:  logKey,
			Data: input,
		}
		brokerClient.Publish(context.Background(), &newPublishRequest)
	}
}
