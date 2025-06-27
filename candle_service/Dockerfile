FROM golang:1.21 AS build
WORKDIR /src
COPY . .
RUN go mod download
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0
RUN protoc --go_out=. --go-grpc_out=. proto/candle.proto
RUN go build -o candle-service

FROM gcr.io/distroless/base-debian11
COPY --from=build /src/candle-service /candle-service
EXPOSE 9000
ENTRYPOINT ["/candle-service"]