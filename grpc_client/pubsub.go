package client

import (
	pb "go_c-_test_work/api/proto"

	"google.golang.org/protobuf/types/known/emptypb"
)

type PubSubClientInterface interface {
	Subscribe(pb.SubscribeRequest) (stream pb.Event)
	Publish(pb.PublishRequest) *emptypb.Empty
}

type PubSubClient struct {
	pubSub pb.PubSubClient
}

func NewPubSubClient(psService pb.PubSubClient) *PubSubClient {
	return &PubSubClient{
		pubSub: psService,
	}
}

// // CreateSession creates new session
// func (cl *AuthClient) CreateSession(ctx context.Context, username string) (string, error) {

// 	resp, err := cl.authMicroService.CreateSession(ctx, &auth.CreateSessionRequest{UserID: username})

// 	if err != nil {
// 		return "", err
// 	}

// 	return resp.Cookie, nil
// }

// // DestroySession destroys session
// func (cl *AuthClient) DeleteSession(ctx context.Context, cookie string) error {
// 	_, err := cl.authMicroService.DeleteSession(ctx, &auth.DestroySessionRequest{Cookie: cookie})

// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// // Session checks active session
// func (cl *AuthClient) GetSession(ctx context.Context, cookie string) (string, error) {

// 	resp, err := cl.authMicroService.GetSession(ctx, &auth.GetSessionRequest{Cookie: cookie})

// 	if err != nil {
// 		return "", err
// 	}

// 	return resp.UserID, nil
// }
