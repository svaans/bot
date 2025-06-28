PROTO=proto/candles.proto

all: candle-service

proto:
protoc --go_out=. --go-grpc_out=. $(PROTO)

candle-service: proto
go build -o candle-service

clean:
rm -f candle-service proto/*.pb.go