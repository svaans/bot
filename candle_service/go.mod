module candle_service

go 1.23

require (
    github.com/gorilla/websocket v1.5.0
    google.golang.org/grpc v1.73.0
    google.golang.org/genproto v0.0.0-20250324211829-b45e905df463
    google.golang.org/protobuf v1.36.6
)

replace google.golang.org/genproto => google.golang.org/genproto v0.0.0-20250324211829-b45e905df463

