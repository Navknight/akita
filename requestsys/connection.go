package requestsys

import (
	"errors"

	"gitlab.com/yaotsu/core/eventsys"
)

// A Sender can send requests to their destinations
type Sender interface {
	CanSend(req *Request) *ConnError
	Send(req *Request) *ConnError
}

// A Receiver can receive requests
type Receiver interface {
	CanRecv(req *Request) *ConnError
	Recv(req *Request) *ConnError
}

// A ConnError is an error from the connection system.
//
// When a component checks if a Sender or a Reveicer CanSend or CanRecv a
// request, if the answer is no, an ConnError will be returned together.
//
// Recoverable determines if a later retry can solve the problem
// EarliestRetry give suggestions on earliest time to retry
type ConnError struct {
	msg           string
	Recoverable   bool
	EarliestRetry eventsys.VTimeInSec
}

func (e *ConnError) Error() string {
	return e.msg
}

// NewConnError creates a new ConnError
func NewConnError(name string, recoverable bool, earliestRetry eventsys.VTimeInSec) *ConnError {
	return &ConnError{name, recoverable, earliestRetry}
}

// A Connectable is an object that an Connection can connect with.
type Connectable interface {
	Connect(portName string, conn Connection) error
	GetConnection(portName string) Connection
	Disconnect(portName string) error

	Receiver
}

// A Connection is responsible for delievering the requests to its destination.
type Connection interface {
	Sender

	Register(s Connectable) error
	Unregister(s Connectable) error
}

// BasicConn is dummy implementation of the connection providing some utilities
// that all other type of connections can use
type BasicConn struct {
	connectables map[Connectable]bool
}

// NewBasicConn creates a basic connection object
func NewBasicConn() *BasicConn {
	c := BasicConn{make(map[Connectable]bool)}
	return &c
}

// Register adds a Connectable object in the connected list
func (c *BasicConn) Register(s Connectable) error {
	c.connectables[s] = true
	return nil
}

// Unregister removes a Connectable object from the connected list
func (c *BasicConn) Unregister(s Connectable) error {
	delete(c.connectables, s)
	return nil
}

// getDest provides a simple utility function for determine the request
func (c *BasicConn) getDest(req *Request) (Component, error) {
	if req.To != nil {
		if _, ok := c.connectables[req.To]; ok {
			return req.To, nil
		}

		return nil, errors.New("Destination " + req.To.Name() + ", which " +
			"is specified in the request, is not connected via connection.")
	}

	if len(c.connectables) != 2 {
		return nil, errors.New("cannot get the destination, since the " +
			"connection has more than 2 end")
	}

	for connectable := range c.connectables {
		if connectable != req.From {
			to, ok := connectable.(Component)
			if !ok {
				return nil, errors.New("Cannot convert the connetable to " +
					"Component")
			}
			req.To = to
			break
		}
	}

	return req.To, nil
}
