module candle_service

go 1.21

require (
    github.com/gorilla/websocket v0.0.0
    google.golang.org/grpc v1.66.0
)

replace github.com/gorilla/websocket => ./websocketstub