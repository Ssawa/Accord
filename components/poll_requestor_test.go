package components

import (
	"testing"
	"time"

	zmq "github.com/pebbe/zmq4"

	"github.com/Ssawa/accord/accord"
	"github.com/stretchr/testify/assert"
)

func TestPollRequestor(t *testing.T) {
	// Create our environment
	accord.AccordCleanup()
	defer accord.AccordCleanup()

	requestor := PollRequestor{
		ConnectAddress: "inproc://pollRequestorTest",
		ListenTimeout:  time.Millisecond,
		SendTimeout:    time.Millisecond,
		WaitOnEmpty:    time.Millisecond,
	}

	acrd := accord.DummyAccord()
	err := acrd.Start()
	assert.Nil(t, err)
	defer acrd.Stop()

	// Start requestor
	err = requestor.Start(acrd)
	assert.Nil(t, err)
	defer requestor.WaitForStop()
	defer requestor.Stop(0)

	// Create our custom server
	server, err := zmq.NewSocket(zmq.REP)
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
	_, err = server.SendMessage("msg", serializeMessage(accord.Message{}))
	assert.Nil(t, err)

}
