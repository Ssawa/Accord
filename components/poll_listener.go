package components

import (
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

	// BindAddress is the ZeroMQ address to bind to. This must follow the ZMQ addressing schema (transport://endpoint)
	BindAddress string
	// Timeout is how long we should wait when doing ZMQ receives and sends before giving up. This should be balanced
	// with how much leanancy you want to give your network with how responsive you want your Accord process to be
	// (This effects how long it takes to process shutting down the program)
	Timeout time.Duration

	sock *zmq.Socket
	log  *logrus.Entry
}

// Start binds our ZeroMQ socket and gets us ready to start processing incomming requests
func (listener *PollListener) Start(accord *accord.Accord) (err error) {
	listener.log = accord.Logger.WithField("component", "PollListener")

	// Default our timeout to half a second
	if listener.Timeout == 0 {
		listener.Timeout = 500 * time.Millisecond
	}

	// Can we have a brief talk about golang's error handling? I understand some of the grievances
	// about exceptions but trying to do any kind of error handling just becomes an unreadable mess

	listener.log.Info("Starting PollListener")
	listener.sock, err = zmq.NewSocket(zmq.REQ)
	if err != nil {
		listener.log.WithError(err).Error("Could not create ZeroMQ socket")
		return err
	}

	err = listener.sock.Bind(listener.BindAddress)
	if err != nil {
		listener.log.WithError(err).WithField("BindAddress", listener.BindAddress).Error("Could not bind ZeroMQ socket")
		return err
	}

	// Make sure our ZeroMQ socket doesn't block us for too long
	err = listener.sock.SetSndtimeo(listener.Timeout)
	if err != nil {
		listener.log.WithError(err).Error("Could not set ZeroMQ send timeout")
		return err
	}
	err = listener.sock.SetRcvtimeo(listener.Timeout)
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

// tick is where we perform the crux of our logic (as dictated by the ComponentRunner architecture)
func (listener *PollListener) tick(*accord.Accord) {
	msg, err := listener.sock.Recv(0)
	if err != nil {
		listener.ExpectedOrShutdown(err, ZMQTimeout)
		return
	}

	if msg == "send" {

	} else if msg == "ok" {

	}
}
