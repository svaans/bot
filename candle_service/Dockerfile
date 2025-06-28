FROM golang:1.22 as build
WORKDIR /app
COPY go.mod .
COPY proto/ proto/
RUN go mod download
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
RUN protoc --go_out=. --go-grpc_out=. proto/candles.proto
COPY . .
RUN go build -o candle-service

FROM gcr.io/distroless/base-debian11
COPY --from=build /app/candle-service /candle-service
EXPOSE 9000
ENTRYPOINT ["/candle-service"]