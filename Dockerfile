# Build stage
FROM golang:1.23 AS build
WORKDIR /app
# Copy go module and proto definitions
COPY candle_service/go.mod ./
COPY candle_service/proto ./proto
RUN go mod download

# Install protoc and plugins
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

# Generate gRPC code
RUN protoc --go_out=. --go-grpc_out=. proto/candles.proto

# Copy source and build
COPY candle_service .
RUN go build -o candle-service

# Final image
FROM gcr.io/distroless/base-debian11
COPY --from=build /app/candle-service /candle-service
EXPOSE 9000
ENTRYPOINT ["/candle-service"]