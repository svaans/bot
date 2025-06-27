package main

import (
	"bytes"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

func TestHandleConnInvalidJSON(t *testing.T) {
	srv, client := net.Pipe()
	defer client.Close()

	var buf bytes.Buffer
	oldOut := log.Writer()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer log.SetOutput(oldOut)

	done := make(chan struct{})
	go func() {
		handleConn(srv)
		close(done)
	}()

	client.Write([]byte("{bad"))

	client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	b := make([]byte, 1)
	_, err := client.Read(b)
	if err == nil {
		t.Fatalf("expected connection closed")
	}

	<-done

	if !strings.Contains(buf.String(), "decode error") {
		t.Fatalf("expected log to contain decode error, got %q", buf.String())
	}
}