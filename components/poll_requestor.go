package components

import (
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

	// ConnectAddress goes out and connects to a remote address over ZMQ and should be set to the address of a remote PollListener
	ConnectAddress string

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

	state func(*accord.Accord)
}

// Start initializs our PollRequestor and creates, configures, and connects our sockets
func (requestor *PollRequestor) Start(accord *accord.Accord) (err error) {
	requestor.log = accord.Logger.WithField("component", "PollRequestor")

	// Default our timeout to something reasonable
	if requestor.ListenTimeout == 0 {
		requestor.ListenTimeout = 2 * time.Millisecond
	}
	if requestor.SendTimeout == 0 {
		requestor.SendTimeout = 2 * time.Second
	}
	if requestor.WaitOnEmpty == 0 {
		requestor.WaitOnEmpty = time.Second
	}

	requestor.log.Info("Starting PollRequestor")
	requestor.sock, err = zmq.NewSocket(zmq.REQ)
	if err != nil {
		requestor.log.WithError(err).Error("Could not create ZeroMQ socket")
		return err
	}

	err = requestor.sock.Connect(requestor.ConnectAddress)
	if err != nil {
		requestor.log.WithError(err).WithField("BindAddress", requestor.ConnectAddress).Error("Could not bind ZeroMQ socket")
		return err
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

	// ZeroMQ traditionally has a pretty strict REQUEST/REPLY protocol and if that's broken your sockets get into
	// a bad state. These two little options make it so that you can resend messages if they fail without having
	// to restart your socket and adds a little sequence number to messages so that this behavior doesn't cause
	// any issues (http://api.zeromq.org/4-1:zmq-setsockopt)
	err = requestor.sock.SetReqRelaxed(0)
	if err != nil {
		requestor.log.WithError(err).Error("Could not set ZeroMQ REQ socket into a relaxed state")
		return err
	}
	err = requestor.sock.SetReqCorrelate(0)
	if err != nil {
		requestor.log.WithError(err).Error("Could not set ZeroMQ REQ socket into correlate mode")
		return err
	}

	requestor.ComponentRunner.Init(accord, requestor.tick, requestor.cleanup, requestor.log)
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
	// As this is a polling mechanism, we initialize the request to the remote server.

	// sendOk will be set if we tried sending "ok" in response to the remote previously but encountered a timeout.
	// TODO - IMPLEMENT A STATEMACHINE TO HANDLE THIS MORE CLEANLY AND SCALABLY
	if requestor.sendOk {
		if requestor.trySendOk() {
			// We finally were able to get our "ok" to go through. Change our state so that we go back to requesting
			// new messages
			requestor.sendOk = false
		}
	}

	// Here we start our main process by requesting the remote for a new message from its queue
	_, err := requestor.sock.Send("send", 0)
	if err != nil {
		requestor.ExpectedOrShutdown(err, ZMQTimeout)
		return
	}

	// We wait to get a response from our remote
	data, err := requestor.sock.RecvMessageBytes(0)
	if err != nil {
		// If we miss the response due to a timeout that's okay, the remote won't dequeue until we give it an
		// "ok" message and our relaxed and correlated socket configuration should, hopefully, protect us from
		// ZeroMQ weirdness (but please test this!). We should feel safe just returning and starting over again
		requestor.ExpectedOrShutdown(err, ZMQTimeout)
		return
	}

	// PollListener sends a multipart ZMQ message, let's look at the first part to see what kind of response we got
	switch string(data[0]) {
	case "msg":
		// We received an actual message from the remote and we must now process it
		if len(data) < 2 {
			requestor.log.Error("Received a message from remote that we don't know how to parse")
			return
		}
		msg, err := accord.DeserializeMessage(data[1])
		if err != nil {
			// Not much we can do, let's just log, return and try again I guess
			requestor.log.WithError(err).Error("Error decoding remote message")
			return
		}

		err = acrd.HandleRemoteMessage(msg)
		if err != nil {
			// again, not much recourse here, we just have to give up on this sequence and try again
			// (although if we do get an error from HandleRemoteMessage it probably means Accord will
			// shutdown shortly after this)
			requestor.log.WithError(err).Error("Error handling remote message")
			return
		}

		requestor.trySendOk()

	case "empty":
		// If the remote is empty than we should just wait a little bit before trying again later
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
			requestor.log.Error("Received an unparsable error from remote")
		}

	}
}

// trySendOk attempts to send an "ok" message to the remote while handling the case of a possible timeout. If successful we return
// true, otherwise false
func (requestor *PollRequestor) trySendOk() bool {
	requestor.log.Debug("Sending 'ok'")
	t, err := requestor.sock.Send("ok", 0)
	requestor.log.Warn(t)
	if err != nil {

		if !requestor.ExpectedOrShutdown(err, ZMQTimeout) {
			return false
		}

		// TODO - Investigate more elegant solution
		// Not a great solution but the issue is that if we timeout sending our "ok" we can't just return and start over
		// again with a "send" or else will probably process the same message twice. So we need to update our state so that
		// on the next call to tick we try sending "ok" again rather than "send". We should eventually clean this up so that
		// we have a more defined statemachine rather than just trashy "if" statements
		requestor.sendOk = true
		requestor.log.WithError(err).Warn("Timedout sending 'ok', will try sending again")
		return false
	}
	return true
}
