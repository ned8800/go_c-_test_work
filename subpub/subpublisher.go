package subpub

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

type SubPubImpl struct {
	mu          sync.RWMutex
	subscribers map[string][]*subImpl
	wg          sync.WaitGroup
	shutdown    chan struct{}
}

func (sp *SubPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	select {
	case <-sp.shutdown:
		return nil, ErrSubPubClosed
	default:
		break
	}

	newSubscriprion := subImpl{
		cb:    cb,
		msgCh: make(chan interface{}, defaultMessagesBufferSize),
	}

	sp.subscribers[subject] = append(sp.subscribers[subject], &newSubscriprion)

	sp.wg.Add(1)
	go func(subscription *subImpl, subject string) {
		defer sp.wg.Done()

		for msg := range subscription.msgCh {
			subscription.cb(msg)
		}
	}(&newSubscriprion, subject)

	return &SubscriptionImpl{
		sp:      sp,
		subject: subject,
		sub:     &newSubscriprion,
	}, nil
}

func (sp *SubPubImpl) Publish(subject string, msg interface{}) error {

	sp.mu.RLock()
	defer sp.mu.RUnlock()
	select {
	case <-sp.shutdown:
		return ErrSubPubClosed
	default:
	}

	subscribers, ok := sp.subscribers[subject]
	if !ok {
		return nil
	}

	subsCopy := make([]*subImpl, len(subscribers))
	copy(subsCopy, subscribers)

	for _, val := range subsCopy {
		func(sub *subImpl, msg interface{}) {
			select {
			case sub.msgCh <- msg:
				break
			case <-sp.shutdown:
				return
			}
		}(val, msg)
	}

	return nil
}

func (sp *SubPubImpl) Close(ctx context.Context) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	select {
	case <-sp.shutdown:
		return ErrSubPubClosed
	default:
		close(sp.shutdown)
	}

	subsToCLose := []*subImpl{}
	for _, val := range sp.subscribers {
		subsToCLose = append(subsToCLose, val...)
	}

	sp.subscribers = make(map[string][]*subImpl)

	for _, val := range subsToCLose {
		val.close()
	}

	goroutinesFinish := make(chan interface{})
	go func() {
		sp.wg.Wait()
		close(goroutinesFinish)
	}()

	select {
	case <-goroutinesFinish:
		return nil

	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "subPublisher Close() interrupted by context cancel")
	}
}
