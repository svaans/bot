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
	"google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

var validIntervals = map[string]struct{}{
	"1m": {}, "3m": {}, "5m": {}, "15m": {}, "30m": {},
    "1h": {}, "2h": {}, "4h": {}, "6h": {}, "8h": {},
    "12h": {}, "1d": {}, "3d": {}, "1w": {}, "1M": {},
}

type server struct{
    pb.UnimplementedCandleServiceServer
}

func normalizeSymbol(s string) string {
    return strings.ToLower(strings.ReplaceAll(s, "/", ""))
}

func (s *server) Subscribe(req *pb.CandleRequest, stream pb.CandleService_SubscribeServer) error {
    if _, ok := validIntervals[req.Interval]; !ok {
        return status.Errorf(codes.InvalidArgument, "invalid interval: %s", req.Interval)
    }
    url := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@kline_%s", normalizeSymbol(req.Symbol), req.Interval)
    // failures counts consecutive connection errors to apply exponential backoff
	failures := 0
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
                log.Printf("connection closed, reconnecting %s %s", req.Symbol, req.Interval)
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
                c := pb.Candle{Symbol: req.Symbol, Timestamp: data.K.T}
                if c.Open, err = strconv.ParseFloat(data.K.O, 64); err != nil { continue }
                if c.High, err = strconv.ParseFloat(data.K.H, 64); err != nil { continue }
                if c.Low, err = strconv.ParseFloat(data.K.L, 64); err != nil { continue }
                if c.Close, err = strconv.ParseFloat(data.K.C, 64); err != nil { continue }
                if c.Volume, err = strconv.ParseFloat(data.K.V, 64); err != nil { continue }
                if err := stream.Send(&c); err != nil {
                    ws.Close()
                    return err
                }
            }
        }
    }
}

func main() {
    lis, err := net.Listen("tcp", ":9000")
    if err != nil {
        log.Fatal(err)
    }
    grpcServer := grpc.NewServer()
    pb.RegisterCandleServiceServer(grpcServer, &server{})
    log.Println("candle service listening on :9000")
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatal(err)
    }
}