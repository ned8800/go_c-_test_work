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

// func (sp *SubPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
// 	sp.mu.Lock()
// 	defer sp.mu.Unlock()

// 	select {
// 	case <-sp.shutdown:
// 		return nil, ErrSubPubClosed
// 	default:
// 		break
// 	}

// 	newSubscriprion := subImpl{
// 		cb:    cb,
// 		msgCh: make(chan interface{}, defaultMessagesBufferSize),
// 	}

// 	sp.subscribers[subject] = append(sp.subscribers[subject], &newSubscriprion)

// 	sp.wg.Add(1)
// 	go func(subscription *subImpl, subject string) {
// 		defer sp.wg.Done()

// 		for msg := range subscription.msgCh {
// 			subscription.cb(msg)
// 		}
// 	}(&newSubscriprion, subject)

// 	return &SubscriptionImpl{
// 		sp:      sp,
// 		subject: subject,
// 		sub:     &newSubscriprion,
// 	}, nil
// }

// Subscribe creates an asynchronous queue subscriber on the given subject.
func (sp *SubPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	// Check if system is shutting down before acquiring lock
	select {
	case <-sp.shutdown:
		return nil, ErrSubPubClosed
	default:
	}

	s := &subImpl{
		cb:    cb,
		msgCh: make(chan interface{}, defaultMessagesBufferSize),
	}

	sp.mu.Lock()
	// Double check after acquiring lock to handle race condition
	select {
	case <-sp.shutdown:
		sp.mu.Unlock()
		return nil, ErrSubPubClosed
	default:
	}
	sp.subscribers[subject] = append(sp.subscribers[subject], s)
	sp.mu.Unlock()

	sp.wg.Add(1)
	go func(sub *subImpl, subSubject string) { // Pass sub and subject to avoid closure capturing issues
		defer sp.wg.Done()
		// defer func() {
		// 	if r := recover(); r != nil {
		// 		// Log panics from MessageHandler to prevent subscriber goroutine death from affecting others.
		// 		// This specific subscriber will stop processing.
		// 		log.Printf("subpub: MessageHandler panicked for subject '%s': %v\nStack: %s", subSubject, r, string(debug.Stack()))
		// 	}
		// }()

		for msg := range sub.msgCh { // Loop terminates when sub.msgCh is closed
			sub.cb(msg)
		}
	}(s, subject)

	return &SubscriptionImpl{sp: sp, subject: subject, sub: s}, nil
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
