package components

import (
	"encoding/binary"
	"testing"
	"time"

	zmq "github.com/pebbe/zmq4"

	"github.com/cj-dimaggio/accord/accord"
	"github.com/stretchr/testify/assert"
)

func TestPollRequestor(t *testing.T) {
	// Create our environment
	accord.AccordCleanup()
	defer accord.AccordCleanup()

	requestor := PollRequestor{
		Address:       "inproc://pollRequestorTest",
		Bind:          false,
		ListenTimeout: time.Millisecond,
		SendTimeout:   time.Millisecond,
		WaitOnEmpty:   time.Millisecond,
	}

	manager := accord.DummyManager{ShouldProcessRet: true}

	acrd := accord.DummyAccordManager(&manager)
	err := acrd.Start()
	assert.Nil(t, err)
	defer acrd.Stop()

	// Start requestor
	err = requestor.Start(acrd)
	assert.Nil(t, err)
	defer requestor.WaitForStop()
	defer requestor.Stop(0)

	// Create our custom server
	server, err := zmq.NewSocket(zmq.PAIR)
	assert.Nil(t, err)

	err = server.Bind("inproc://pollRequestorTest")
	assert.Nil(t, err)

	data, err := server.Recv(0)
	assert.Nil(t, err)
	assert.Equal(t, "send", data)

	serializeMessage := func(msg accord.Message) []byte {
		data, err := msg.Serialize()
		assert.Nil(t, err)
		return data
	}

	// Send a new message to the requestor
	_, err = server.SendMessage("msg", serializeMessage(accord.Message{ID: 5, StateAt: 0, Payload: []byte{1}}))
	assert.Nil(t, err)

	data, err = server.Recv(0)
	assert.Nil(t, err)
	assert.Equal(t, "ok", data)

	// Make sure the requestor processed our message
	assert.Equal(t, 1, manager.ProcessCount)
	assert.Equal(t, uint64(1), acrd.Status().HistorySize)
	assert.Equal(t, uint64(5), acrd.Status().State)

	_, err = server.Send("deleted", 0)
	assert.Nil(t, err)

	// Test sending empty
	data, err = server.Recv(0)
	assert.Nil(t, err)
	assert.Equal(t, "send", data)

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, acrd.Status().State)

	_, err = server.SendMessage("empty", buf)
	assert.Nil(t, err)

	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, uint64(0), acrd.Status().HistorySize)

}
