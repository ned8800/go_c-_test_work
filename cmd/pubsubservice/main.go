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
	log.Info().Msg("broker client: Starting")

	cfg, err := config.LoadConfig(".")
	if err != nil {
		syslog.Fatalf("broker client: Failed to load configuration: %v", err)
	}

	brokerServiceAddress := cfg.Server.GRPCAddress

	if brokerServiceAddress == "" {
		brokerServiceAddress = "localhost:8080"
		log.Info().Msg("broker client: main broker adress not in config, using default: localhost:8080")
	}

	// connection
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

	logKey := cfg.Server.ClientSubject
	brokerClient := pb.NewPubSubClient(conn)
	logReq := &pb.SubscribeRequest{Key: logKey}

	// Subscribe
	streamClient, err := brokerClient.Subscribe(context.Background(), logReq)
	if err != nil {
		syslog.Fatalf("error couldnt connect to grpc: %v", err)
	}
	log.Info().Msgf("broker client: Subscribed to main broker for key '%s'. Listening for events...", logKey)

	// Listen for events
	go func(strCl *pb.PubSub_SubscribeClient) {
		for {
			event, err := (*strCl).Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Info().Msgf("broker client: main broker stream for key '%s' ended (EOF). Exiting.", logKey)
				} else if status.Code(err) == codes.Canceled {
					log.Info().Msgf("broker client: main broker stream for key '%s' canceled. Exiting.", logKey)
				} else {
					log.Info().Msgf("broker client: error receiving from main broker stream for key '%s': %v. Exiting.", logKey, err)
				}
				return
			}
			log.Info().Msgf("broker client: main broker stream: Subject: %s, Data: %s", logKey, event.GetData())
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
