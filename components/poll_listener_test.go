package components

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/Ssawa/accord/accord"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/assert"
)

func TestPollListener(t *testing.T) {
	// Setup
	accord.AccordCleanup()
	defer accord.AccordCleanup()

	listener := PollListener{
		BindAddress:   "inproc://pollListenerTest",
		ListenTimeout: time.Millisecond,
		SendTimeout:   time.Millisecond,
	}
	acrd := accord.DummyAccord()
	err := acrd.Start()
	assert.Nil(t, err)
	defer acrd.Stop()

	// Initialize data into our system
	msg, err := accord.NewMessage([]byte("abc"))
	assert.Nil(t, err)

	err = acrd.HandleNewMessage(msg)
	assert.Nil(t, err)

	assert.Nil(t, err)

	// Start listener
	err = listener.Start(acrd)
	assert.Nil(t, err)
	defer listener.WaitForStop()
	defer listener.Stop(0)

	assert.Nil(t, err)

	// Setup client to test listener with
	client, err := zmq.NewSocket(zmq.PAIR)
	assert.Nil(t, err)

	err = client.Connect("inproc://pollListenerTest")
	assert.Nil(t, err)

	// See if we get the message back
	_, err = client.Send("send", 0)
	assert.Nil(t, err)

	data, err := client.RecvMessageBytes(0)
	assert.Nil(t, err)
	assert.Len(t, data, 2)
	assert.Equal(t, "msg", string(data[0]))

	receivedMsg, err := accord.DeserializeMessage(data[1])
	assert.Equal(t, msg.ID, receivedMsg.ID)
	assert.Equal(t, msg.StateAt, receivedMsg.StateAt)
	assert.Equal(t, msg.Timestamp, receivedMsg.Timestamp)
	assert.Equal(t, msg.Payload, receivedMsg.Payload)

	// Make sure it wasn't actually dequeued
	assert.Equal(t, uint64(1), acrd.Status().ToBeSyncedSize)

	// Dequeue message
	_, err = client.Send("ok", 0)
	assert.Nil(t, err)

	data, err = client.RecvMessageBytes(0)
	assert.Nil(t, err)
	assert.Len(t, data, 1)
	assert.Equal(t, "deleted", string(data[0]))

	assert.Equal(t, uint64(0), acrd.Status().ToBeSyncedSize)

	// Test empty
	_, err = client.Send("send", 0)
	assert.Nil(t, err)
	data, err = client.RecvMessageBytes(0)
	assert.Nil(t, err)
	assert.Len(t, data, 2)
	assert.Equal(t, "empty", string(data[0]))
	assert.Equal(t, acrd.Status().State, binary.LittleEndian.Uint64(data[1]))

	// Test error handling
	acrd.Stop()

	_, err = client.Send("send", 0)
	assert.Nil(t, err)
	data, err = client.RecvMessageBytes(0)
	assert.Nil(t, err)
	assert.Len(t, data, 2)
	assert.Equal(t, "error", string(data[0]))
}
