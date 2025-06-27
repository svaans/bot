package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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
}

func TestNumericParsing(t *testing.T) {
	srv, client := net.Pipe()
	defer client.Close()

	websocket.DefaultDialer.Messages = [][]byte{
		[]byte(`{"k":{"t":123,"o":"1.5","h":"2.5","l":"1.0","c":"2.0","v":"100","x":true}}`),
	}

	done := make(chan struct{})
	go func() {
		handleConn(srv)
		close(done)
	}()

	json.NewEncoder(client).Encode(CandleRequest{Symbol: "BTCUSDT", Interval: "1m"})

	var c Candle
	if err := json.NewDecoder(client).Decode(&c); err != nil {
		t.Fatalf("decode candle: %v", err)
	}

	if c.Open != 1.5 || c.High != 2.5 || c.Low != 1.0 || c.Close != 2.0 || c.Volume != 100 {
		t.Fatalf("unexpected candle: %+v", c)
	}

	client.Close()
	<-done
}