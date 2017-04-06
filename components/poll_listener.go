package components

import (
	"syscall"
	"time"

	"github.com/Ssawa/accord/accord"
	zmq "github.com/pebbe/zmq4"
	"github.com/sirupsen/logrus"
)

type PollListener struct {
	accord.ComponentRunner

	BindAddress string
	Timeout     time.Duration

	sock *zmq.Socket
	log  *logrus.Entry
}

func (listener *PollListener) Start(accord *accord.Accord) (err error) {
	listener.log = accord.Logger.WithField("component", "PollListener")

	if listener.Timeout == 0 {
		listener.Timeout = 500 * time.Millisecond
	}

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

	listener.ComponentRunner.Init(accord, listener.tick, listener.cleanup, listener.log)
	return nil
}

func (listener *PollListener) tick(*accord.Accord) {
	_, err := listener.sock.Recv(0)
	if err != nil {
		if err == zmq.Errno(syscall.EAGAIN) {
			return
		} else {
			listener.Shutdown(err)
		}
	}
}

func (listener *PollListener) cleanup(*accord.Accord) {
	err := listener.sock.Close()
	if err != nil {
		listener.log.WithError(err).Warn("Error closing ZeroMQ socket")
	}
}
