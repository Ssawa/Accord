package accord

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncQueue(t *testing.T) {
	// We're going to test most of the functions of SyncQueue as part of this one test
	// rather than arbitrarily splitting them out just because it seems to make the most sense

	os.RemoveAll("sync.queue")
	defer os.RemoveAll("sync.queue")
	sync, err := OpenSyncQueue("sync.queue")
	assert.Nil(t, err)

	assert.Zero(t, sync.Size())
	err = sync.Enqueue(&Message{Payload: []byte{1}})
	assert.Nil(t, err)
	err = sync.Enqueue(&Message{Payload: []byte{2}})
	assert.Nil(t, err)
	err = sync.Enqueue(&Message{Payload: []byte{3}})
	assert.Nil(t, err)

	msg, err := sync.Peek()
	assert.Nil(t, err)

	assert.Equal(t, []byte{1}, msg.Payload)
	assert.Equal(t, uint64(3), sync.Size())

	msg, err = sync.Dequeue()
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, msg.Payload)
	assert.Equal(t, uint64(2), sync.Size())

	msg, err = sync.Dequeue()
	assert.Nil(t, err)
	assert.Equal(t, []byte{2}, msg.Payload)

	assert.Equal(t, uint64(1), sync.Size())

	msg, err = sync.Dequeue()
	assert.Nil(t, err)
	assert.Equal(t, []byte{3}, msg.Payload)

	assert.Equal(t, uint64(0), sync.Size())

	msg, err = sync.Dequeue()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	msg, err = sync.Peek()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	assert.Equal(t, uint64(0), sync.Size())

}
