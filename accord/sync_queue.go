package accord

import "github.com/beeker1121/goque"

// SyncQueue is responsible for holding all of the messages we've executed and need to be synchronized
// remotely. It must work in a FIFO manner and be thread safe. Our underlying structure is goque
// as it gives us both of these things, but we're wrapping it so that we can limit the kinds of operations
// that can be executed as well as hidding the book keeping serialization and filesystem tasks (it also gives
// us the ability to easily swap out for something different later if we so choose)
type SyncQueue struct {
	//queue is our underlying structure where we actually store and persist our data. Currently we're
	// using goque, which is built atop LevelDB, so simplify our lives and give us a thread safe FIFO data
	// structure
	queue *goque.Queue
}

// OpenSyncQueue opens or creates a FIFO queue stored at the passed in path
func OpenSyncQueue(path string) (*SyncQueue, error) {
	queue, err := goque.OpenQueue(path)
	if err != nil {
		return nil, err
	}

	return &SyncQueue{
		queue: queue,
	}, nil
}

// Peek returns the next Message in the queue but does *not* actually take it out
// of the queue. Returns nil if the queue is empty
func (sync *SyncQueue) Peek() (*Message, error) {
	item, err := sync.queue.Peek()
	if err != nil {
		if err == goque.ErrEmpty {
			return nil, nil
		}
		return nil, err
	}

	msg := Message{}
	err = item.ToObject(&msg)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

// Enqueue adds a new Message to the end of the queue
func (sync *SyncQueue) Enqueue(msg *Message) error {
	bytes, err := msg.Serialize()
	if err != nil {
		return err
	}

	_, err = sync.queue.Enqueue(bytes)
	return err
}

// Dequeue pops the next Message off of the queue in a FIFO manner and returns it.
// Returns nil if the queue is empty
func (sync *SyncQueue) Dequeue() (*Message, error) {
	item, err := sync.queue.Dequeue()
	if err != nil {
		if err == goque.ErrEmpty {
			return nil, nil
		}
		return nil, err
	}

	msg := Message{}
	err = item.ToObject(&msg)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

// Size returns the number of elements currently enqueued
func (sync *SyncQueue) Size() uint64 {
	return sync.queue.Length()
}

// Close closes the underlying connection to our persisted queue
func (sync *SyncQueue) Close() {
	sync.queue.Close()
}
