package components

import (
	"syscall"

	zmq "github.com/pebbe/zmq4"
)

// ZMQTimeout represents a timeout from ZeroMQ
var ZMQTimeout = zmq.Errno(syscall.EAGAIN)
