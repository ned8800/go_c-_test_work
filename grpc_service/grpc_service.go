package grpcservice

import (
	"context"
	pb "go_c-_test_work/api/proto"
	"io"

	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ExternalPubSubImpl struct {
	pb.UnimplementedPubSubServer
	brokerClient pb.PubSubClient
}

func NewExternalPubSubImpl(brokerServiceAddr string) (*ExternalPubSubImpl, error) {
	conn, err := grpc.NewClient(
		":8081",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "GRPC Client: Failed to connect to broker PubSub main service at %s", brokerServiceAddr)
	}
	log.Info().Msg("GRPC Client: connection opened successfully")

	defer func() {
		if clErr := conn.Close(); clErr != nil {
			log.Error().Msg("couldn't close auth grpc connection")
		}
	}()

	client := pb.NewPubSubClient(conn)
	log.Printf("GRPC Client: Successfully connected to broker PubSub main service at %s", brokerServiceAddr)

	return &ExternalPubSubImpl{
		brokerClient: client,
	}, nil
}

func (s *ExternalPubSubImpl) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	if req.GetKey() == "" {
		return status.Error(codes.InvalidArgument, "external: key cannot be empty")
	}
	externalClientKey := req.GetKey()
	log.Printf("GRPC Client: External client subscribed to key: %s", externalClientKey)

	brokerReq := &pb.SubscribeRequest{Key: externalClientKey}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	brokerStream, err := s.brokerClient.Subscribe(ctx, brokerReq)
	if err != nil {
		log.Printf("GRPC Client: Failed to call broker.Subscribe for key %s: %v", externalClientKey, err)
		st, _ := status.FromError(err)
		return status.Error(st.Code(), "external: failed to subscribe via broker: "+st.Message())
	}
	log.Printf("GRPC Client: Broker subscription stream established for key: %s", externalClientKey)

	for {
		eventFromBroker, err := brokerStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("GRPC Client: Broker stream for key %s ended (EOF).", externalClientKey)
				return nil
			}
			st, _ := status.FromError(err)
			if st.Code() == codes.Canceled {
				log.Printf("GRPC Client: Broker stream for key %s canceled.", externalClientKey)
				return status.Error(codes.Canceled, "external: subscription canceled by upstream")
			}
			log.Printf("GRPC Client: Error receiving from broker stream for key %s: %v (code: %s)", externalClientKey, err, st.Code())
			return status.Error(st.Code(), "external: error from broker: "+st.Message())
		}

		// // Send the received pb.Event directly to the external client
		// if err := stream.Send(eventFromBroker); err != nil {
		// 	log.Printf("GRPC Client: Error sending event to external client for key %s: %v", externalClientKey, err)
		// 	if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled || status.Code(err) == codes.Unavailable {
		// 		return nil
		// 	}
		// 	return status.Errorf(codes.Internal, "external: failed to send event to client: %v", err)
		// }
		log.Info().Msg(eventFromBroker.Data)
	}
}

func (s *ExternalPubSubImpl) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "external: key cannot be empty")
	}
	log.Printf("GRPC Client: External client publishing to key: %s, Data: %s", req.GetKey(), req.GetData())

	// The request to the broker is the same PublishRequest
	brokerReq := &pb.PublishRequest{
		Key:  req.GetKey(),
		Data: req.GetData(),
	}

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Call broker's Publish method
	_, err := s.brokerClient.Publish(publishCtx, brokerReq)
	if err != nil {
		log.Printf("GRPC Client: Failed to call broker.Publish for key %s: %v", req.GetKey(), err)
		st, _ := status.FromError(err)
		return nil, status.Error(st.Code(), "external: failed to publish via broker: "+st.Message())
	}

	return &emptypb.Empty{}, nil
}
