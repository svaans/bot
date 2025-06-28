package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	pb "candle_service/proto"
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
    backoff := time.Second

    for {
        if err := stream.Context().Err(); err != nil {
            return err
        }

        ws, _, err := websocket.DefaultDialer.Dial(url, nil)
        if err != nil {
            log.Printf("connect error: %v", err)
            time.Sleep(backoff)
            if backoff < 30*time.Second {
                backoff *= 2
            }
            continue
        }
        log.Printf("connected %s %s", req.Symbol, req.Interval)
		backoff = time.Second
        for {
			if err := stream.Context().Err(); err != nil {
                ws.Close()
				return err
			}
			_, msg, err := ws.ReadMessage()
			if err != nil {
				log.Printf("read error: %v", err)
				ws.Close()
				break
			}
			var data struct {
				K struct {
					T int64   `json:"t"`
					O float64 `json:"o"`
					H float64 `json:"h"`
					L float64 `json:"l"`
					C float64 `json:"c"`
					V float64 `json:"v"`
					X bool    `json:"x"`
				} `json:"k"`
			}
			if err := json.Unmarshal(msg, &data); err != nil {
				log.Printf("json error: %v", err)
				continue
			}
			if !data.K.X {
				continue
			}
			candle := &pb.Candle{
				Symbol:    req.Symbol,
				Timestamp: data.K.T,
				Open:      data.K.O,
				High:      data.K.H,
				Low:       data.K.L,
				Close:     data.K.C,
				Volume:    data.K.V,
			}
			if err := stream.Send(candle); err != nil {
				ws.Close()
				return err
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