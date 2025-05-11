package subpub

import (
	"context"
	"errors"
)

const (
	defaultMessagesBufferSize = 64
)

var (
	ErrSubPubClosed = errors.New("subPublisher is closed")
)

// MessageHandler is a callback function that processes messages delivered to subscribers.
type MessageHandler func(msg interface{})

type Subscription interface {
	// Unsubscribe will remove interest in the current subject subscription is for.
	Unsubscribe()
}

type SubPub interface {
	// Subscribe creates an asynchronous queue subscriber on the given subject.
	Subscribe(subject string, cb MessageHandler) (Subscription, error)

	// Publish publishes the msg argument to the given subject.
	Publish(subject string, msg interface{}) error

	// Close will shutdown sub-pub system.
	// May be blocked by data delivery until the context is canceled.
	Close(ctx context.Context) error
}

func NewSubPub() SubPub {
	return &SubPubImpl{
		subscribers: make(map[string][]*subImpl),
		shutdown:    make(chan struct{}),
	}
}
