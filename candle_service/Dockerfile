FROM golang:1.21 AS build
WORKDIR /src
COPY . .
RUN go mod download
RUN go build -o candle-service

FROM gcr.io/distroless/base-debian11
COPY --from=build /src/candle-service /candle-service
EXPOSE 9000
ENTRYPOINT ["/candle-service"]