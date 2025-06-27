package websocket

import "net/http"

// Dialer is a stub dialer.
type Dialer struct{}

// DefaultDialer is a stub dialer instance.
var DefaultDialer = &Dialer{}

// Conn is a stub connection.
type Conn struct{}

func (*Dialer) Dial(urlStr string, reqHeader http.Header) (*Conn, *http.Response, error) {
	return &Conn{}, nil, nil
}

func (*Conn) ReadMessage() (int, []byte, error) {
	return 0, nil, nil
}

func (*Conn) Close() error {
	return nil
}