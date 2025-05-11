package subpub

import (
	"sync"
)

type subImpl struct {
	cb        MessageHandler
	msgCh     chan interface{}
	onceClose sync.Once
}

func (s *subImpl) close() {
	s.onceClose.Do(func() {
		close(s.msgCh)
	})
}

type SubscriptionImpl struct {
	sp      *SubPubImpl
	subject string
	sub     *subImpl
}

func (subscr *SubscriptionImpl) Unsubscribe() {
	subscr.sp.mu.Lock()
	defer subscr.sp.mu.Unlock()

	select {
	case <-subscr.sp.shutdown:
		subscr.sub.close()
		return
	default:
	}

	subs, ok := subscr.sp.subscribers[subscr.subject]
	if !ok {
		return
	}

	foundIdx := -1
	for i, s := range subs {
		if s == subscr.sub {
			foundIdx = i
			break
		}
	}

	if foundIdx != -1 {
		subscr.sp.subscribers[subscr.subject] = append(subs[:foundIdx], subs[foundIdx+1:]...)
		if len(subscr.sp.subscribers[subscr.subject]) == 0 {
			delete(subscr.sp.subscribers, subscr.subject)
		}
		subscr.sub.close()
	}
}
