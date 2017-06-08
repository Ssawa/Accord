package components

import (
	"encoding/binary"
	"time"

	"github.com/Ssawa/accord/accord"
	zmq "github.com/pebbe/zmq4"

	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

// PollRequestor is a part of a "polling" scheme, along with PollListener, for use your network demands that you use a polling mechanism
// for communication between clients. PollRequestor is responsible for sending requests to a remote client to see if there are any new
// messages there and, if there are, tries to synchronize with the remote state.
type PollRequestor struct {
	accord.ComponentRunner

	// Address is the ZeroMQ address to use. This must follow the ZMQ addressing schema (transport://endpoint)
	Address string

	// Bind determines whether we should bind the the suplied address or connect
	Bind bool
	// ListenTimeout and SendTimeout is how long we should wait when doing ZMQ receives and sends before giving up. This should be balanced
	// with how much leanancy you want to give your network with how responsive you want your Accord process to be
	// (This effects how long it takes to process shutting down the program)
	ListenTimeout time.Duration
	SendTimeout   time.Duration

	// WaitOnEmpty specifies how long we should wait before requesting again if the remote tells us its queue is empty
	WaitOnEmpty time.Duration

	sock *zmq.Socket
	log  *logrus.Entry

	sendOk bool

	// state helps us represent a very simple and very loose state machine. Essentially, our tick function will only execute whatever this function
	// is. To change your state, simply change the function
	state func(*accord.Accord)

	// If we haven't received anything in awhile we're probably in a hung state and we should reset
	reset int
}

// Start initializes our PollRequestor and creates, configures, and connects our sockets
func (requestor *PollRequestor) Start(accord *accord.Accord) (err error) {
	requestor.log = accord.Logger.WithField("component", "PollRequestor")

	requestor.log.Debug("Entering requestMsgState")
	requestor.state = requestor.requestMsgState

	// Default our timeout to something reasonable
	if requestor.ListenTimeout == 0 {
		requestor.ListenTimeout = 2 * time.Second
	}
	if requestor.SendTimeout == 0 {
		requestor.SendTimeout = 2 * time.Second
	}
	if requestor.WaitOnEmpty == 0 {
		requestor.WaitOnEmpty = time.Second
	}

	requestor.log.WithField("address", requestor.Address).Info("Starting PollRequestor")
	err = requestor.createSocket()
	if err != nil {
		return err
	}

	// I attempted to set the socket to REQ Relaxed and REQ Coralated but it just didn't work.
	// It's worth investigating however. For now we'll just
	requestor.ComponentRunner.Init(accord, requestor.tick, requestor.cleanup, requestor.log)
	return nil
}

func (requestor *PollRequestor) createSocket() (err error) {
	requestor.sock, err = zmq.NewSocket(zmq.PAIR)
	if err != nil {
		requestor.log.WithError(err).Error("Could not create ZeroMQ socket")
		return err
	}

	if requestor.Bind {
		err = requestor.sock.Bind(requestor.Address)
		if err != nil {
			requestor.log.WithError(err).WithField("Address", requestor.Address).Error("Could not bind ZeroMQ socket")
			return err
		}
	} else {
		err = requestor.sock.Connect(requestor.Address)
		if err != nil {
			requestor.log.WithError(err).WithField("Address", requestor.Address).Error("Could not connect ZeroMQ socket")
			return err
		}
	}

	// Make sure our ZeroMQ socket doesn't block us for too long
	err = requestor.sock.SetSndtimeo(requestor.SendTimeout)
	if err != nil {
		requestor.log.WithError(err).Error("Could not set ZeroMQ send timeout")
		return err
	}
	err = requestor.sock.SetRcvtimeo(requestor.ListenTimeout)
	if err != nil {
		requestor.log.WithError(err).Error("Could not set ZeroMQ receive timeout")
		return err
	}

	return nil
}

// cleanup makes sure all of our connections are cleaned up and not left in a hanging state
func (requestor *PollRequestor) cleanup(*accord.Accord) {
	err := requestor.sock.Close()
	if err != nil {
		requestor.log.WithError(err).Warn("Error closing ZeroMQ socket")
	}
}

// The general protocol that PollRequestor follows is to send a message to a PollListener with the string
// "send" to request a new Message from the remote. If it receives one, it will process the message locally
// and then send an "ok" to signify to the remote that it has successfully performed it's operation and that
// the remote can now safely dequeue the message and move on to the next
func (requestor *PollRequestor) tick(acrd *accord.Accord) {
	// Execute our "state machine"
	requestor.state(acrd)
}

// requestMsgState is our initial state where we send a request off to our remote to get a new message
// from their queue
func (requestor *PollRequestor) requestMsgState(acrd *accord.Accord) {
	requestor.reset = 0
	_, err := requestor.sock.Send("send", 0)
	if err != nil {
		requestor.ExpectedOrShutdown(err, ZMQTimeout)
		requestor.log.Debug("Timed out sending. Destroying socket and trying again")
		requestor.sock.Close()
		requestor.createSocket()
		return
	}
	requestor.log.Debug("Sent request, entering receiveState")
	requestor.state = requestor.receiveState
}

// receiveState waits to receive a response from our remote
func (requestor *PollRequestor) receiveState(acrd *accord.Accord) {
	if requestor.reset >= 10 {
		requestor.log.Debug("Timed out listening too many times. Re-entering requestMsgState")
		requestor.state = requestor.requestMsgState
		return
	}

	data, err := requestor.sock.RecvMessageBytes(0)
	if err != nil {
		requestor.ExpectedOrShutdown(err, ZMQTimeout)
		requestor.reset++
		requestor.log.Debug("Timed out listening. Incrementing count: ", requestor.reset)
		return
	}

	// PollListener sends a multipart ZMQ message, let's look at the first part to see what kind of response we got
	switch string(data[0]) {
	case "msg":
		// We received an actual message from the remote and we must now process it
		if len(data) < 2 {
			requestor.log.Error("Received a message from remote that we don't know how to parse")
			break
		}
		msg, err := accord.DeserializeMessage(data[1])
		if err != nil {
			// Not much we can do, let's just log, return and try again I guess
			requestor.log.WithError(err).Error("Error decoding remote message")
			break
		}

		err = acrd.HandleRemoteMessage(msg)
		if err != nil {
			// again, not much recourse here, we just have to give up on this sequence and try again
			// (although if we do get an error from HandleRemoteMessage it probably means Accord will
			// shutdown shortly after this)
			requestor.log.WithError(err).Error("Error handling remote message")
			break
		}

		// We need to send out our "ok" to tell the remote it's okay to clean up
		requestor.log.Debug("Entering sendOKState")
		requestor.state = requestor.sendOKState
		return

	case "empty":
		// If the remote is empty than we should tell accord to check our state against theirs and then wait a bit before
		// sending a new request
		if len(data) < 2 {
			requestor.log.Error("Received an 'empty' from remote that we don't know how to parse")
		} else {
			state := binary.LittleEndian.Uint64(data[1])
			acrd.CheckRemoteState(state)
		}
		time.Sleep(requestor.WaitOnEmpty)

	case "deleted":
		// If the remote just told us it deleted from it's local queue there's not much for us to do besides maybe
		// log it and move on
		requestor.log.Debug("Remote has dequeued")

	case "error":
		// Looks like we received an error from the remote, we need to log it and see if there's anything we should
		// do
		if len(data) >= 2 {
			remoteErr := string(data[1])
			requestor.log.WithField("errorMessage", remoteErr).Error("Received error from remote")

			// You can look at the PollListener code to see why this is such a bad thing, and why our best course
			// of action for this particular error is to panic and shutdown
			if remoteErr == "dequeue" {
				requestor.log.Fatal("Received a dequeue error from remote")
				requestor.Shutdown(errors.New("remote dequeue received"))
			}
		} else {
			requestor.log.Warn("Received an unparsable error from remote")
		}
	default:
		requestor.log.WithField("message", string(data[0])).Warn("Got a message we don't know how to handle")
	}
	// We've received something and handled it, so now let's go back to our request state
	requestor.log.Debug("Entering requestMsgState")
	requestor.state = requestor.requestMsgState

}

// sendOKState sends out an "ok" message to the remote server to signify that
// we've successfully processed the message
func (requestor *PollRequestor) sendOKState(acrd *accord.Accord) {
	_, err := requestor.sock.Send("ok", 0)
	if err != nil {
		requestor.ExpectedOrShutdown(err, ZMQTimeout)
		return
	}
	requestor.log.Debug("Entering receiveState")
	requestor.state = requestor.receiveState
}
