package components

import (
	"encoding/binary"
	"time"

	"github.com/Ssawa/accord/accord"
	zmq "github.com/pebbe/zmq4"
	"github.com/sirupsen/logrus"
)

// PollListener is part of a "polling" scheme of possible Accord components that can be used when your
// network typology best lends itself to a synchronization method that consists of going out and polling
// for changes from a remote Accord instance.
//
// PollListener, specifically, is responsible for listening on a ZeroMQ port for requests for our processed
// Messages from remote clients and relaying them so that they can be synchronized.
type PollListener struct {
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

	sock *zmq.Socket
	log  *logrus.Entry

	state func(*accord.Accord)
	reply []interface{}
}

// Start binds our ZeroMQ socket and gets us ready to start processing incomming requests
func (listener *PollListener) Start(accord *accord.Accord) (err error) {
	listener.log = accord.Logger.WithField("component", "PollListener")

	listener.log.Debug("Entering recvState")
	listener.state = listener.recvState

	// Default our timeout to something reasonable
	if listener.ListenTimeout == 0 {
		listener.ListenTimeout = 500 * time.Millisecond
	}
	if listener.SendTimeout == 0 {
		listener.SendTimeout = 2 * time.Second
	}

	// Can we have a brief talk about golang's error handling? I understand some of the grievances
	// about exceptions but trying to do any kind of error handling just becomes an unreadable mess

	listener.log.Info("Starting PollListener")
	listener.sock, err = zmq.NewSocket(zmq.PAIR)
	if err != nil {
		listener.log.WithError(err).Error("Could not create ZeroMQ socket")
		return err
	}

	if listener.Bind {
		err = listener.sock.Bind(listener.Address)
		if err != nil {
			listener.log.WithError(err).WithField("Address", listener.Address).Error("Could not bind ZeroMQ socket")
			return err
		}
	} else {
		err = listener.sock.Connect(listener.Address)
		if err != nil {
			listener.log.WithError(err).WithField("Address", listener.Address).Error("Could not connect ZeroMQ socket")
			return err
		}
	}

	// Make sure our ZeroMQ socket doesn't block us for too long
	err = listener.sock.SetSndtimeo(listener.SendTimeout)
	if err != nil {
		listener.log.WithError(err).Error("Could not set ZeroMQ send timeout")
		return err
	}
	err = listener.sock.SetRcvtimeo(listener.ListenTimeout)
	if err != nil {
		listener.log.WithError(err).Error("Could not set ZeroMQ receive timeout")
		return err
	}

	// This Component is managed by ComponentRunner, which handles our process loop for us (hopefully)
	listener.ComponentRunner.Init(accord, listener.tick, listener.cleanup, listener.log)
	return nil
}

// cleanup closes our sockets and makes sure we don't have any hanging states that may cause an issue
func (listener *PollListener) cleanup(*accord.Accord) {
	err := listener.sock.Close()
	if err != nil {
		listener.log.WithError(err).Warn("Error closing ZeroMQ socket")
	}
}

// tick is where we perform the crux of our logic (as dictated by the ComponentRunner architecture).
// Our basic protocol is to listen for a request, if it says "send" then we peek at the next guy on our
// queue and send it over. If we get an "ok" than we take it as a confirmation that the message has been
// handled on the remote side and we can safely dequeue the message. Obviously this protocol breaks down
// if multiple clients connect, but one thing at a time for now...
func (listener *PollListener) tick(acrd *accord.Accord) {
	listener.state(acrd)
}

func (listener *PollListener) recvState(acrd *accord.Accord) {
	msg, err := listener.sock.Recv(0)
	if err != nil {
		listener.ExpectedOrShutdown(err, ZMQTimeout)
		return
	}

	switch msg {
	case "send":
		listener.log.Debug("Received 'send'")
		// We have a request to send a new piece of data, let's take a look at what it is but *not*
		// actually take it off our queue yey
		msg, err := acrd.ToBeSynced.Peek()
		if err != nil {
			// This is not good but not necessarily an *unrecoverable* error (although, realistically it
			// probably mean human intervention is needed). In any case, we simply tell our client somethings
			// up but don't take down our application just yet
			listener.log.WithError(err).Error("Error ocurred reading from the queue")
			listener.reply = []interface{}{"error", "queue read"}
			break
		}

		if msg == nil {
			// If our queue is empty, tell the client and also tell it our state
			listener.log.Debug("Sending queue empty and our status")
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, acrd.Status().State)
			listener.reply = []interface{}{"empty", buf}
			break
		}

		data, err := msg.Serialize()
		if err != nil {
			// Like above, this isn't necessarily the end of the world in the sense that we're not screwing up our
			// state. We simply log the error, tell the client, and keep moving
			listener.log.WithError(err).Error("Error serializing message")
			listener.reply = []interface{}{"error", "serialize"}
			break
		}

		// We use ZeroMQ's multi part messaging here to make it easier for the client to parse the response. Essentially
		// our responses have categories, they can be an "error", or a "msg", or a "deleted"
		listener.log.Debug("Sending message")
		listener.reply = []interface{}{"msg", data}
		break

	case "ok":
		listener.log.Debug("Received 'ok'")
		// If we get an "ok" from the client we assume it means that it has processed our previous send and is now synced
		// with that message, so we can take it off our queue.
		//
		// Now this is a pretty dumb implementation. For one thing if we ever have more than one client everything breaks
		// down, for another there's nothing stopping a client from screwing up and sending multiple "ok"s at once. These
		// problems are all solvable, but let's start with getting an MVP going and then try adding that stuff. For now let's
		// put it in the category of TODO

		_, err := acrd.ToBeSynced.Dequeue()
		if err != nil {
			// We're in a bit of a rough spot here if this ever *does* happen (god I hope it doesn't).
			// Without a rollback system (which should we just add?) there's not a whole lot we can do to
			// make sure things stay aligned, essentially all we can do is tell our remote "sorry" so that
			// hopefully he can think of something clever to do and then panic and shutdown (which is what
			// the remote should probably do too so that nothing else bad happens)
			listener.log.WithError(err).Fatal("Error removing from our queue")
			listener.sock.SendMessage("error", "dequeue")
			listener.Shutdown(err)
			return
		}

		// This is a bit unnecessary but ZeroMQ demands we send *something* so we might as well send this
		listener.log.Debug("sending 'deleted'")
		listener.reply = []interface{}{"deleted"}
		break

	default:
		listener.log.WithField("message", msg).Warn("Received unknown request")
		listener.reply = []interface{}{"unknown"}
		break

	}

	listener.log.Debug("Entering sendState")
	listener.state = listener.sendState
}

// sentData sends data over to the client
func (listener *PollListener) sendState(acrd *accord.Accord) {
	_, err := listener.sock.SendMessage(listener.reply...)
	if err != nil {
		listener.ExpectedOrShutdown(err, ZMQTimeout)
		return
	}

	listener.log.Debug("Entering recvState")
	listener.state = listener.recvState
}
