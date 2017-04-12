package accord

import (
	"sync"

	"github.com/beeker1121/goque"
)

// HistoryStack holds the history of messages we've processed until so that we can mitigate application specific
// message conflicts (such as database update collisions), until such a time that we're confident we don't need
// them anymore. As the name implies, it works as a Stack in a LIFO behavior so that the latest operations appear
// first. HistoryStack has a lot of similarity around SyncQueue in that it's pretty much just a wrapper around another
// library's persisted data structures. However, HistoryStack in particular is something I'm not completely happy with
// as it could potentially grow very large even when Accord processes are in open communication with one another, under
// certain situations (high message rates being the most obvious, although Accord is pretty honest about only really being
// useful in situations where reliability of a few messages is more important than the performant handling of lots of messages)
// But we should always be on the lookout for clever ways we can keep this pruned down to a manageable state. As such, I imagine
// this will be a bit more "full featured" compared to SyncQueue just to accommodate those tricks
type HistoryStack struct {
	// Our main structure that actually holds our stack and persists it to disk using Goque and LevelDB
	stack *goque.Stack

	// We maintain a reference to our path so that we can easily drop and recreate our stack when requested
	path string

	// While goque gives us thread safety for each individual call, to perform our helper functions we may need to perform
	// multiple calls and we don't want to have the data changed under us in the middle of an operation, so we need to
	// perform our own thread synchronization
	stackLock *sync.Mutex
}

// OpenHistoryStack opens or creates our LIFO stack stored at the passed in path
func OpenHistoryStack(path string) (*HistoryStack, error) {
	stack, err := goque.OpenStack(path)
	if err != nil {
		return nil, err
	}

	return &HistoryStack{
		stack:     stack,
		path:      path,
		stackLock: &sync.Mutex{},
	}, nil
}

// peek is a helper for Peek and PeekByOffset
func (history *HistoryStack) peek(offset uint64) (*Message, error) {
	item, err := history.stack.PeekByOffset(offset)
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

// Peek returns the next Message *without* actually taking it off the stack. Returns nil if the stack is empty
func (history *HistoryStack) Peek() (*Message, error) {
	history.stackLock.Lock()
	defer history.stackLock.Unlock()

	return history.peek(0)
}

// Peek returns a Message from the stack at the given offset without actually taking it off the stack. Returns nil if the stack is empty
func (history *HistoryStack) PeekByOffset(offset uint64) (*Message, error) {
	history.stackLock.Lock()
	defer history.stackLock.Unlock()

	return history.peek(offset)
}

// Push adds a new Message to the top of our stack in a LIFO manner
func (history *HistoryStack) Push(msg *Message) error {
	history.stackLock.Lock()
	defer history.stackLock.Unlock()

	bytes, err := msg.Serialize()
	if err != nil {
		return err
	}

	_, err = history.stack.Push(bytes)
	return err
}

// Pop takes the top most Message off of our stack and returns it. Returns nil if the stack is empty
func (history *HistoryStack) Pop() (*Message, error) {
	history.stackLock.Lock()
	defer history.stackLock.Unlock()

	item, err := history.stack.Pop()
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

// Size returns the number of Messages in our stack
func (history *HistoryStack) Size() uint64 {
	history.stackLock.Lock()
	defer history.stackLock.Unlock()

	return history.stack.Length()
}

// Clear drops all data from the history stack
func (history *HistoryStack) Clear() (err error) {
	history.stackLock.Lock()
	defer history.stackLock.Unlock()

	history.stack.Drop()
	history.stack, err = goque.OpenStack(history.path)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the underlying connection to our persisted stack
func (history *HistoryStack) Close() {
	history.stackLock.Lock()
	defer history.stackLock.Unlock()

	history.stack.Close()
}

type HistoryIterator struct {
	stack *HistoryStack
	pos   uint64
	size  uint64
}

// createHistoryIterator creates a new instance of a HistoryIterator for easier navigation of a HistoryStack. This call should *always*
// be followed by a call to HistoryIterator.close
func createHistoryIterator(stack *HistoryStack) *HistoryIterator {
	// We lock the underlying stack to make sure it doesn't change from under us. This lock is only release by the
	// "close" method and it *must* be called
	it := &HistoryIterator{
		stack: stack,
		pos:   0,
		size:  stack.stack.Length(),
	}
	it.stack.stackLock.Lock()
	return it
}

// close unlocks the underlying HistoryStack so that further operations can be performed upon it
func (it *HistoryIterator) close() {
	it.stack.stackLock.Unlock()
}

// Next returns the next element in the stack and moves its pointer forward. If there are no more items available it returns nil
func (it *HistoryIterator) Next() (*Message, error) {
	if it.pos < it.size {
		msg, err := it.stack.peek(it.pos)
		it.pos++
		return msg, err
	}
	return nil, nil
}
