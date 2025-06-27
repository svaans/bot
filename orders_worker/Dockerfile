FROM golang:1.21 AS build
WORKDIR /src
COPY . .
RUN go mod download
RUN go build -o orders-worker

FROM gcr.io/distroless/base-debian11
COPY --from=build /src/orders-worker /orders-worker
EXPOSE 9100
ENTRYPOINT ["/orders-worker"]