package interfaces

import (
	zmq "github.com/go-zeromq/zmq4"
)

// BuffSocket is the interface for sending ZMQ messages, allowing sendResponse
// to be tested without a real ZMQ socket.
type BuffSocket interface {
	Send(msg zmq.Msg) error
}
