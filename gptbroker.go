package main

import (
	"sync"
)

var _ IBroker = (*GPTBroker)(nil)

var topBlockSize uint16 = 256

type topicBlock struct {
	next *topicBlock
	data [topicBlockSize]string
}

type topicQueue struct {
	mu sync.Mutex

	head    *topicBlock
	tail    *topicBlock
	headPos uint16
	tailPos uint16
}

type GPTBroker struct {
	topics sync.Map // map[string]*topicQueue
}

func NewGPTBroker() *GPTBroker {
	return &GPTBroker{}
}

func (b *GPTBroker) Send(topic, msg string) {
	v, _ := b.topics.LoadOrStore(topic, &topicQueue{})
	q := v.(*topicQueue)

	q.mu.Lock()

	if q.tail == nil {
		block := &topicBlock{}
		q.head = block
		q.tail = block
		q.headPos = 0
		q.tailPos = 0
	} else if q.tailPos == topBlockSize {
		block := &topicBlock{}
		q.tail.next = block
		q.tail = block
		q.tailPos = 0
	}

	q.tail.data[q.tailPos] = msg
	q.tailPos++

	q.mu.Unlock()
}

func (b *GPTBroker) Recv(topic string) (string, error) {
	v, ok := b.topics.Load(topic)
	if !ok {
		return "", ErrTopicNotFound
	}
	q := v.(*topicQueue)

	q.mu.Lock()

	if q.head == nil {
		q.mu.Unlock()
		return "", ErrTopicEmpty
	}

	// head == tail means tailPos is also the number of currently written
	// entries in this block. For older blocks all slots are known to be full.
	if q.head == q.tail && q.headPos == q.tailPos {
		q.mu.Unlock()
		return "", ErrTopicEmpty
	}

	msg := q.head.data[q.headPos]
	q.head.data[q.headPos] = "" // release the backing string for GC
	q.headPos++

	if q.headPos == topBlockSize {
		old := q.head
		q.head = old.next
		old.next = nil
		q.headPos = 0

		// If the consumed block was also the tail, the queue is now empty.
		// Resetting tail avoids retaining the old block and lets the next Send
		// start with a fresh block.
		if q.head == nil {
			q.tail = nil
			q.tailPos = 0
		}
	} else if q.head == q.tail && q.headPos == q.tailPos {
		// Keep the current partially used block. All consumed strings were
		// cleared, so only the small block itself remains attached to topic.
	}

	q.mu.Unlock()
	return msg, nil
}
