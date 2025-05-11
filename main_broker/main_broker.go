package mainbroker

import (
	"context"
	"errors"
	pb "go_c-_test_work/api/proto"
	"go_c-_test_work/subpub"
	"io"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// BrokerPubSubImpl implements the pb.PubSubServiceServer for the core broker.
type BrokerPubSubImpl struct {
	pb.UnimplementedPubSubServer
	bus subpub.SubPub
}

func NewBrokerPubSubImpl(bus subpub.SubPub) *BrokerPubSubImpl {
	return &BrokerPubSubImpl{
		bus: bus,
	}
}

func (s *BrokerPubSubImpl) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	if req.GetKey() == "" {
		return status.Error(codes.InvalidArgument, "broker: key cannot be empty")
	}
	subscribeKey := req.GetKey()

	log.Info().Msgf("main broker: Client (external_svc) subscribed to key: %s", subscribeKey)

	msgChan := make(chan interface{}, 64)

	handler := func(msg interface{}) {
		data, ok := msg.(string)
		if !ok {
			log.Error().Msgf("main broker: Bus sent non-string message for key %s: %T", subscribeKey, msg)
			return
		}
		select {
		case msgChan <- data:
		default:
			log.Error().Msg("main broker: channel full")
		}
	}

	subscription, err := s.bus.Subscribe(subscribeKey, handler)
	if err != nil {
		log.Info().Msgf("main broker: Failed to subscribe to local bus for key %s: %v", subscribeKey, err)
		if errors.Is(err, subpub.ErrSubPubClosed) {
			return status.Errorf(codes.Unavailable, "broker: service is shutting down")
		}
		return status.Errorf(codes.Internal, "broker: failed to subscribe to local bus: %v", err)
	}
	defer subscription.Unsubscribe()
	log.Info().Msgf("main broker: Successfully subscribed to local bus for key: %s", subscribeKey)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			log.Info().Msgf("main broker: Subscriber stream for key %s ended. Error: %v", subscribeKey, ctx.Err())
			return nil
		case busMsg := <-msgChan:
			data, _ := busMsg.(string)
			event := &pb.Event{Data: data}
			if err := stream.Send(event); err != nil {
				log.Info().Msgf("main broker: Error sending event for key %s: %v", subscribeKey, err)
				if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled || status.Code(err) == codes.Unavailable {
					return nil
				}
				return status.Errorf(codes.Internal, "broker: failed to send event: %v", err)
			}
		}
	}
}

func (s *BrokerPubSubImpl) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "broker: key cannot be empty")
	}
	log.Info().Msgf("main broker: Publish request for key: %s, Data: %s", req.GetKey(), req.GetData())

	err := s.bus.Publish(req.GetKey(), req.GetData())
	if err != nil {
		log.Info().Msgf("main broker: Failed to publish to local bus for key %s: %v", req.GetKey(), err)
		if errors.Is(err, subpub.ErrSubPubClosed) {
			return nil, status.Errorf(codes.Unavailable, "broker: service is shutting down")
		}
		return nil, status.Errorf(codes.Internal, "broker: failed to publish to local bus: %v", err)
	}
	return &emptypb.Empty{}, nil
}
