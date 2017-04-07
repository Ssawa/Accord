package components

import (
	"time"

	"github.com/Ssawa/accord/accord"
	zmq "github.com/pebbe/zmq4"

	"github.com/sirupsen/logrus"
)

type PollRequestor struct {
	accord.ComponentRunner

	ConnectAddress string
	// ListenTimeout and SendTimeout is how long we should wait when doing ZMQ receives and sends before giving up. This should be balanced
	// with how much leanancy you want to give your network with how responsive you want your Accord process to be
	// (This effects how long it takes to process shutting down the program)
	ListenTimeout time.Duration
	SendTimeout   time.Duration

	sock *zmq.Socket
	log  *logrus.Entry
}

func (requestor *PollRequestor) Start(accord *accord.Accord) (err error) {
	requestor.log = accord.Logger.WithField("component", "PollRequestor")

	// Default our timeout to something reasonable
	if requestor.ListenTimeout == 0 {
		requestor.ListenTimeout = 2 * time.Millisecond
	}
	if requestor.SendTimeout == 0 {
		requestor.SendTimeout = 2 * time.Second
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

func (requestor *PollRequestor) cleanup(*accord.Accord) {
	err := requestor.sock.Close()
	if err != nil {
		requestor.log.WithError(err).Warn("Error closing ZeroMQ socket")
	}
}

func (requestor *PollRequestor) tick(acrd *accord.Accord) {
	requestor.sock.Send("send", 0)

	data, _ := requestor.sock.RecvMessageBytes(0)
	switch string(data[0]) {
	case "msg":

	}
}
