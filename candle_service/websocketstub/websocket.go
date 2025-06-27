package websocket

import (
	"errors"
	"net/http"
)

// Dialer is a stub dialer that returns a connection preloaded with messages.
type Dialer struct {
	// Messages contains the payloads that the returned connection will
	// provide on successive ReadMessage calls.
	Messages [][]byte
}
// DefaultDialer is a stub dialer instance.
var DefaultDialer = &Dialer{}

// Conn is a stub connection that yields predefined messages.
type Conn struct {
	messages [][]byte
	idx      int
}

// Dial returns a new stub connection that will replay the dialer's messages.
func (d *Dialer) Dial(urlStr string, reqHeader http.Header) (*Conn, *http.Response, error) {
	return &Conn{messages: d.Messages}, nil, nil
}

// ReadMessage returns the next predefined message or an error when exhausted.
func (c *Conn) ReadMessage() (int, []byte, error) {
	if c.idx >= len(c.messages) {
		return 0, nil, errors.New("no more messages")
	}
	msg := c.messages[c.idx]
	c.idx++
	return 1, msg, nil
}

func (*Conn) Close() error {
	return nil
}