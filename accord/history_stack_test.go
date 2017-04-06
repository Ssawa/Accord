package accord

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHistoryStack(t *testing.T) {
	os.RemoveAll("history.stack")
	defer os.RemoveAll("history.stack")
	sync, err := OpenHistoryStack("history.stack")
	assert.Nil(t, err)

	assert.Zero(t, sync.Size())
	err = sync.Push(&Message{Payload: []byte{1}})
	assert.Nil(t, err)
	err = sync.Push(&Message{Payload: []byte{2}})
	assert.Nil(t, err)
	err = sync.Push(&Message{Payload: []byte{3}})
	assert.Nil(t, err)

	msg, err := sync.Peek()
	assert.Nil(t, err)

	assert.Equal(t, []byte{3}, msg.Payload)
	assert.Equal(t, uint64(3), sync.Size())

	msg, err = sync.Pop()
	assert.Nil(t, err)
	assert.Equal(t, []byte{3}, msg.Payload)
	assert.Equal(t, uint64(2), sync.Size())

	msg, err = sync.Pop()
	assert.Nil(t, err)
	assert.Equal(t, []byte{2}, msg.Payload)

	assert.Equal(t, uint64(1), sync.Size())

	msg, err = sync.Pop()
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, msg.Payload)

	assert.Equal(t, uint64(0), sync.Size())

	msg, err = sync.Pop()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	msg, err = sync.Peek()
	assert.Nil(t, err)
	assert.Nil(t, msg)

	assert.Equal(t, uint64(0), sync.Size())

}
