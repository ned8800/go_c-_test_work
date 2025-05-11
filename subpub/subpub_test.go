package subpub

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateSubPub(t *testing.T) {
	s := NewSubPub()

	assert.NotEqual(t, nil, s, "NewSubPub s must not be nill")
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return true
	case <-time.After(timeout):
		return false
	}
}

func Test_NewSubPub(t *testing.T) {
	sp := NewSubPub()
	assert.NotNil(t, sp, "NewSubPub() should return a non-nil instance")
	assert.IsType(t, (*SubPubImpl)(nil), sp, "NewSubPub() should return a *SubPubImpl")
}

func Test_SubscribeAndPublish_SingleSubscriber(t *testing.T) {
	tests := []struct {
		name        string
		subject     string
		message     interface{}
		expectedMsg interface{}
	}{
		{"string message", "test.subject.string", "hello world", "hello world"},
		{"int message", "test.subject.int", 123, 123},
		{"struct message", "test.subject.struct", struct{ Data string }{Data: "data"}, struct{ Data string }{Data: "data"}},
		{"nil message", "test.subject.nil", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := NewSubPub()
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			defer func() {
				err := sp.Close(closeCtx)
				if err != nil && !errors.Is(err, ErrSubPubClosed) {
					assert.NoError(t, err, "Deferred sp.Close in %s failed unexpectedly", tt.name)
				}
			}()

			var receivedMsg interface{}
			var wg sync.WaitGroup
			wg.Add(1)

			handler := func(msg interface{}) {
				receivedMsg = msg
				wg.Done()
			}

			_, err := sp.Subscribe(tt.subject, handler)
			assert.NoError(t, err, "Subscribe failed")

			err = sp.Publish(tt.subject, tt.message)
			assert.NoError(t, err, "Publish failed")

			assert.True(t, waitTimeout(&wg, time.Second*1), "Timed out waiting for message")
			assert.Equal(t, tt.expectedMsg, receivedMsg, "Message content mismatch")
		})
	}
}

func Test_SubscribeAndPublish_MultipleSubscribersSameSubject(t *testing.T) {
	testCases := []struct {
		name           string
		numSubscribers int
		message        interface{}
	}{
		{"one subscriber", 1, "multi-cast-0"},
		{"few subscribers", 3, "multi-cast-1"},
		{"many subscribers", 10, "multi-cast-2"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sp := NewSubPub()
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			defer sp.Close(closeCtx)

			subject := "test.subject.multi." + tc.name
			var wg sync.WaitGroup
			wg.Add(tc.numSubscribers)

			receivedMsgs := make([][]interface{}, tc.numSubscribers)
			var mu sync.Mutex

			for i := 0; i < tc.numSubscribers; i++ {
				idx := i
				receivedMsgs[idx] = make([]interface{}, 0, 1)
				handler := func(msg interface{}) {
					mu.Lock()
					receivedMsgs[idx] = append(receivedMsgs[idx], msg)
					mu.Unlock()
					wg.Done()
				}
				_, err := sp.Subscribe(subject, handler)
				assert.NoError(t, err, "Subscribe failed for subscriber %d", idx)
			}

			err := sp.Publish(subject, tc.message)
			assert.NoError(t, err, "Publish failed")

			timeoutDuration := time.Second * (1 + time.Duration(tc.numSubscribers/5))
			assert.True(t, waitTimeout(&wg, timeoutDuration), "Timed out waiting for messages")

			for i := 0; i < tc.numSubscribers; i++ {
				mu.Lock()
				assert.Len(t, receivedMsgs[i], 1, "Subscriber %d did not receive exactly one message", i)
				if len(receivedMsgs[i]) == 1 {
					assert.Equal(t, tc.message, receivedMsgs[i][0], "Subscriber %d message content mismatch", i)
				}
				mu.Unlock()
			}
		})
	}
}

func Test_SubscribeAndPublish_DifferentSubjects(t *testing.T) {
	sp := NewSubPub()
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer sp.Close(closeCtx)

	subject1 := "subject.one"
	subject2 := "subject.two"
	var msg1Actual interface{}
	var msg2Actual interface{}
	var wg sync.WaitGroup
	wg.Add(2)

	var once1 sync.Once
	handler1 := func(msg interface{}) {
		once1.Do(func() {
			msg1Actual = msg
			wg.Done()
		})
	}

	var once2 sync.Once
	handler2 := func(msg interface{}) {
		once2.Do(func() {
			msg2Actual = msg
			wg.Done()
		})
	}

	_, err := sp.Subscribe(subject1, handler1)
	assert.NoError(t, err, "Subscribe to %s failed", subject1)
	_, err = sp.Subscribe(subject2, handler2)
	assert.NoError(t, err, "Subscribe to %s failed", subject2)

	expectedMsg1, expectedMsg2 := "message for one", "message for two"
	assert.NoError(t, sp.Publish(subject1, expectedMsg1), "Publish to %s failed", subject1)
	assert.NoError(t, sp.Publish(subject2, expectedMsg2), "Publish to %s failed", subject2)

	assert.NoError(t, sp.Publish(subject1, "another for one"), "Publish to %s failed", subject1)
	assert.NoError(t, sp.Publish("subject.three", "message for three"), "Publish to subject.three failed")

	assert.True(t, waitTimeout(&wg, time.Second*1), "Timed out waiting for the first messages for subject1 and subject2")
	assert.Equal(t, expectedMsg1, msg1Actual, "Handler1 first message content mismatch")
	assert.Equal(t, expectedMsg2, msg2Actual, "Handler2 first message content mismatch")
}

func Test_Unsubscribe(t *testing.T) {
	sp := NewSubPub()
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer sp.Close(closeCtx)

	subject := "test.unsubscribe"
	var receivedCountAtomic int32
	var wg sync.WaitGroup

	handler := func(msg interface{}) {
		atomic.AddInt32(&receivedCountAtomic, 1)
		wg.Done()
	}

	sub, err := sp.Subscribe(subject, handler)
	assert.NoError(t, err, "Subscribe failed")

	t.Run("receive_before_unsubscribe", func(t *testing.T) {
		wg.Add(1)
		assert.NoError(t, sp.Publish(subject, "message1"), "Publish failed")
		assert.True(t, waitTimeout(&wg, time.Second*1), "Timed out waiting for message1")
		assert.Equal(t, int32(1), atomic.LoadInt32(&receivedCountAtomic), "Message count before unsubscribe")
	})

	t.Run("publish_after_unsubscribe", func(t *testing.T) {
		sub.Unsubscribe()
		time.Sleep(50 * time.Millisecond)

		assert.NoError(t, sp.Publish(subject, "message2_after_unsubscribe"), "Publish after unsubscribe failed")
		time.Sleep(100 * time.Millisecond)

		assert.Equal(t, int32(1), atomic.LoadInt32(&receivedCountAtomic), "Message count after unsubscribe")
	})
}

func Test_FIFOOrder(t *testing.T) {
	testCases := []struct {
		name        string
		numMessages int
	}{
		{"no messages", 0},
		{"one message", 1},
		{"few messages", 5},
		{"many messages", defaultMessagesBufferSize + 10},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sp := NewSubPub()
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			defer sp.Close(closeCtx)

			subject := "test.fifo." + tc.name
			receivedMsgs := make([]interface{}, 0)
			var mu sync.Mutex
			var wg sync.WaitGroup

			if tc.numMessages > 0 {
				wg.Add(tc.numMessages)
			}

			handler := func(msg interface{}) {
				mu.Lock()
				receivedMsgs = append(receivedMsgs, msg)
				mu.Unlock()
				wg.Done()
			}

			if tc.numMessages > 0 {
				_, err := sp.Subscribe(subject, handler)
				assert.NoError(t, err, "Subscribe failed")
			}

			expectedMsgs := make([]interface{}, tc.numMessages)
			for i := 0; i < tc.numMessages; i++ {
				expectedMsgs[i] = fmt.Sprintf("message_%d_for_%s", i, tc.name)
				err := sp.Publish(subject, expectedMsgs[i])
				if !assert.NoError(t, err, "Publish for message %d failed", i) {
					wg.Done()
				}
			}

			if tc.numMessages == 0 {
				time.Sleep(50 * time.Millisecond)
			} else {
				timeoutDuration := time.Second*1 + time.Millisecond*time.Duration(tc.numMessages*20)
				assert.True(t, waitTimeout(&wg, timeoutDuration), "Timed out waiting for messages. Received %d/%d", len(receivedMsgs), tc.numMessages)
			}

			mu.Lock()
			assert.Equal(t, expectedMsgs, receivedMsgs, "Messages not in FIFO order or mismatch")
			mu.Unlock()
		})
	}
}

func Test_SlowSubscriber(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer sp.Close(closeCtx)

	subject := "test.slow"
	var fastReceived, slowReceived bool
	var fastWg, slowWg sync.WaitGroup

	fastWg.Add(1)
	_, err := sp.Subscribe(subject, func(msg interface{}) { fastReceived = true; fastWg.Done() })
	assert.NoError(t, err, "Fast subscribe failed")

	slowWg.Add(1)
	_, err = sp.Subscribe(subject, func(msg interface{}) { time.Sleep(300 * time.Millisecond); slowReceived = true; slowWg.Done() })
	assert.NoError(t, err, "Slow subscribe failed")

	go func() {
		assert.NoError(t, sp.Publish(subject, "message_for_slow_test"), "Publish failed in goroutine")
	}()

	assert.True(t, waitTimeout(&fastWg, 150*time.Millisecond), "Fast subscriber did not receive message in time")
	assert.True(t, fastReceived, "Fast subscriber flag not set")

	assert.True(t, waitTimeout(&slowWg, time.Second*1), "Timed out waiting for slow subscriber")
	assert.True(t, slowReceived, "Slow subscriber flag not set")
}

func Test_Close_Simple(t *testing.T) {
	sp := NewSubPub()

	t.Run("first close", func(t *testing.T) {
		assert.NoError(t, sp.Close(context.Background()), "First Close failed")
	})

	t.Run("idempotent close", func(t *testing.T) {
		err := sp.Close(context.Background())
		assert.ErrorIs(t, err, ErrSubPubClosed, "Expected ErrSubPubClosed on second close")
	})
}

func Test_Close_WithSubscribersProcessing(t *testing.T) {
	sp := NewSubPub()
	subject := "test.close.processing"
	numMessages := defaultMessagesBufferSize / 2
	var receivedCountAtomic int64
	var handlerWg sync.WaitGroup

	handler := func(msg interface{}) {
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&receivedCountAtomic, 1)
		handlerWg.Done()
	}

	_, err := sp.Subscribe(subject, handler)
	assert.NoError(t, err, "Subscribe failed")

	publishedSuccessfully := 0
	for i := 0; i < numMessages; i++ {
		handlerWg.Add(1)
		err := sp.Publish(subject, fmt.Sprintf("msg_%d", i))
		if err != nil {
			handlerWg.Done()
			if errors.Is(err, ErrSubPubClosed) {
				break
			}
			assert.NoError(t, err, "Publish for message failed")
		} else {
			publishedSuccessfully++
		}
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second*3)
	defer cancelClose()

	closeErrChan := make(chan error, 1)
	go func() { closeErrChan <- sp.Close(closeCtx) }()

	var closeErr error
	select {
	case closeErr = <-closeErrChan:
	case <-time.After(4 * time.Second):
		assert.Fail(t, "sp.Close() call itself timed out / deadlocked")
	}

	assert.NoError(t, closeErr, "Close returned an error")

	finalWaitSuccess := waitTimeout(&handlerWg, 500*time.Millisecond)
	finalCount := atomic.LoadInt64(&receivedCountAtomic)

	if closeErr == nil {
		assert.True(t, finalWaitSuccess)
		assert.Equal(t, int64(publishedSuccessfully), finalCount)
	} else {
		assert.LessOrEqual(t, finalCount, int32(publishedSuccessfully))
	}
}

func Test_Close_WithContextCancel(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	subject := "test.close.context"
	var processedCountAtomic int32
	var handlerStarted sync.WaitGroup
	handlerStarted.Add(1)

	_, err := sp.Subscribe(subject, func(msg interface{}) {
		handlerStarted.Done()
		time.Sleep(2 * time.Second)
		atomic.AddInt32(&processedCountAtomic, 1)
	})
	assert.NoError(t, err, "Subscribe failed")

	assert.NoError(t, sp.Publish(subject, "slow_message"), "Publish failed")
	assert.True(t, waitTimeout(&handlerStarted, time.Second), "Handler did not start processing in time")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	closeErr := sp.Close(ctx)

	assert.Error(t, closeErr, "Close should have returned an error due to context cancellation")
	assert.Condition(t, func() bool {
		return errors.Is(closeErr, context.DeadlineExceeded) || errors.Is(closeErr, context.Canceled)
	}, "Expected context.DeadlineExceeded or context.Canceled, got %v", closeErr)

	time.Sleep(200 * time.Millisecond)
	currentProcessed := atomic.LoadInt32(&processedCountAtomic)
	assert.Zero(t, currentProcessed, "Handler should not complete processing if Close was canceled early and handler is slow")

	err = sp.Publish(subject, "another_message")
	assert.ErrorIs(t, err, ErrSubPubClosed, "Publish after Close should return ErrSubPubClosed")
}

func Test_OperationsAfterClose(t *testing.T) {
	sp := NewSubPub()
	initialCloseCtx, initialCloseCancel := context.WithTimeout(context.Background(), time.Second)
	defer initialCloseCancel()
	assert.NoError(t, sp.Close(initialCloseCtx), "Initial sp.Close failed")

	tests := []struct {
		name        string
		operation   func(s SubPub) error
		expectedErr error
	}{
		{"Subscribe after Close", func(s SubPub) error { _, e := s.Subscribe("s", func(m interface{}) {}); return e }, ErrSubPubClosed},
		{"Publish after Close", func(s SubPub) error { return s.Publish("s", "m") }, ErrSubPubClosed},
		{"Second Close", func(s SubPub) error {
			c, ca := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer ca()
			return s.Close(c)
		}, ErrSubPubClosed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation(sp)
			assert.ErrorIs(t, err, tt.expectedErr, "Error mismatch for operation: %s", tt.name)
		})
	}
}

func Test_PublishToNonExistentSubject(t *testing.T) {
	sp := NewSubPub()
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer sp.Close(closeCtx)

	assert.NoError(t, sp.Publish("no.one.listening", "ghost_message"), "Publish to non-existent subject should not error")
}

func Test_ConcurrentSubscribePublishUnsubscribe(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer func() {
		err := sp.Close(closeCtx)
		if err != nil && !errors.Is(err, ErrSubPubClosed) {
			assert.NoError(t, err, "Deferred sp.Close in concurrent test failed unexpectedly")
		}
	}()

	numGoroutines, numOpsPerGoroutine := 10, 200
	subjectPrefix := "concurrent.test."
	var stressWg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		stressWg.Add(1)
		go func(gid int) {
			defer stressWg.Done()
			mySubs := make(map[string]Subscription)

			for i := 0; i < numOpsPerGoroutine; i++ {
				op := (gid + i) % 4
				subject := fmt.Sprintf("%s%d", subjectPrefix, (gid+i)%10)

				switch op {
				// Subscribe
				case 0:
					if _, subExists := mySubs[subject]; !subExists {
						handler := func(msg interface{}) { time.Sleep(time.Microsecond * time.Duration(gid%5)) }
						sub, err := sp.Subscribe(subject, handler)
						if err == nil {
							mySubs[subject] = sub
						} else {
							assert.ErrorIs(t, err, ErrSubPubClosed, "Subscribe in goroutine %d to %s failed with unexpected error", gid, subject)
						}
					}
				// Publish
				case 1:
					msg := fmt.Sprintf("msg_g%d_i%d", gid, i)
					err := sp.Publish(subject, msg)
					if err != nil {
						assert.ErrorIs(t, err, ErrSubPubClosed, "Publish in goroutine %d to %s failed with unexpected error", gid, subject)
					}
				// Unsubscribe
				case 2:
					if sub, exists := mySubs[subject]; exists {
						sub.Unsubscribe()
						delete(mySubs, subject)
					}
				case 3:
					time.Sleep(time.Microsecond)
				}
			}
			for k, sub := range mySubs {
				sub.Unsubscribe()
				delete(mySubs, k)
			}
		}(g)
	}
	assert.True(t, waitTimeout(&stressWg, 10*time.Second))
}

func Test_UnsubscribeDuringPublish(t *testing.T) {
	t.Parallel()
	sp := NewSubPub()
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer sp.Close(closeCtx)

	subject := "unsubscribe.during.publish"
	var receivedMessages []int
	var mu sync.Mutex
	var messageProcessingWg sync.WaitGroup

	handler := func(msg interface{}) {
		mu.Lock()
		receivedMessages = append(receivedMessages, msg.(int))
		mu.Unlock()
		messageProcessingWg.Done()
	}

	sub, err := sp.Subscribe(subject, handler)
	assert.NoError(t, err, "Subscribe failed")

	numMessagesToPublish := (defaultMessagesBufferSize * 2)
	unsubscribeAt := (numMessagesToPublish / 2)

	var publishRoutineWg sync.WaitGroup
	publishRoutineWg.Add(1)

	go func() {
		defer publishRoutineWg.Done()
		for i := 0; i < numMessagesToPublish; i++ {
			shouldProcess := (i <= unsubscribeAt+defaultMessagesBufferSize)
			if shouldProcess {
				messageProcessingWg.Add(1)
			}

			publishErr := sp.Publish(subject, i)
			if publishErr != nil {
				if shouldProcess {
					messageProcessingWg.Done()
				}
				assert.Error(t, publishErr)
			}
			if i == unsubscribeAt {
				sub.Unsubscribe()
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	publishRoutineWg.Wait()
	success := waitTimeout(&messageProcessingWg, 1*time.Second)

	assert.False(t, success)

	mu.Lock()
	finalReceivedCount := len(receivedMessages)
	sort.Ints(receivedMessages)
	finalMessages := make([]int, finalReceivedCount)
	copy(finalMessages, receivedMessages)
	mu.Unlock()

	assert.LessOrEqual(t, finalReceivedCount, numMessagesToPublish)
	assert.LessOrEqual(t, finalMessages[finalReceivedCount-1], unsubscribeAt+defaultMessagesBufferSize+5)
}
