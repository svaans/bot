package main

import (
    "bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

    "github.com/gorilla/websocket"
)

var validIntervals = map[string]struct{}{
	"1m": {}, "3m": {}, "5m": {}, "15m": {}, "30m": {},
	"1h": {}, "2h": {}, "4h": {}, "6h": {}, "8h": {},
	"12h": {}, "1d": {}, "3d": {}, "1w": {}, "1M": {},
}

type CandleRequest struct {
    Symbol   string `json:"symbol"`
    Interval string `json:"interval"`
}

type Candle struct {
    Symbol    string  `json:"symbol"`
    Timestamp int64   `json:"timestamp"`
    Open      float64 `json:"open"`
    High      float64 `json:"high"`
    Low       float64 `json:"low"`
    Close     float64 `json:"close"`
    Volume    float64 `json:"volume"`
}

func normalizeSymbol(s string) string {
    return strings.ToLower(strings.ReplaceAll(s, "/", ""))
}

func handleConn(conn net.Conn) {
    defer conn.Close()
	dec := json.NewDecoder(bufio.NewReader(conn))
	var req CandleRequest
	if err := dec.Decode(&req); err != nil {
		return
	}
	if _, ok := validIntervals[req.Interval]; !ok {
		log.Printf("invalid interval: %s", req.Interval)
		json.NewEncoder(conn).Encode(map[string]string{"error": "invalid interval"})
		return
	}
	enc := json.NewEncoder(conn)
	url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@kline_%s", normalizeSymbol(req.Symbol), req.Interval)

    for {
		ws, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("connect error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("connected %s %s", req.Symbol, req.Interval)
		for {
			_, msg, err := ws.ReadMessage()
			if err != nil {
				log.Printf("read error: %v", err)
				ws.Close()
				break
			}
			var data struct {
				K struct {
					T int64  `json:"t"`
					O string `json:"o"`
					H string `json:"h"`
					L string `json:"l"`
					C string `json:"c"`
					V string `json:"v"`
					X bool   `json:"x"`
				} `json:"k"`
			}
			if err := json.Unmarshal(msg, &data); err != nil {
				log.Printf("json unmarshal error: %v raw: %s", err, string(msg))
				continue
			}
			if data.K.X {
				c := Candle{
					Symbol:    req.Symbol,
					Timestamp: data.K.T,
				}
				fmt.Sscan(data.K.O, &c.Open)
				fmt.Sscan(data.K.H, &c.High)
				fmt.Sscan(data.K.L, &c.Low)
				fmt.Sscan(data.K.C, &c.Close)
				fmt.Sscan(data.K.V, &c.Volume)
				if err := enc.Encode(&c); err != nil {
					ws.Close()
					return
				}
			}
		}
	}
}

func main() {
    ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("candle service listening on :9000")
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConn(conn)
	}
}