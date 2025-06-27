package proto

import (
    context "context"
    codes "google.golang.org/grpc/codes"
    grpc "google.golang.org/grpc"
    status "google.golang.org/grpc/status"
)

type CandleRequest struct {
    Symbol   string `protobuf:"bytes,1,opt,name=symbol,proto3" json:"symbol,omitempty"`
    Interval string `protobuf:"bytes,2,opt,name=interval,proto3" json:"interval,omitempty"`
}

func (x *CandleRequest) Reset() { *x = CandleRequest{} }
func (*CandleRequest) ProtoMessage() {}

func (x *CandleRequest) GetSymbol() string { if x != nil { return x.Symbol } return "" }
func (x *CandleRequest) GetInterval() string { if x != nil { return x.Interval } return "" }

type Candle struct {
    Symbol    string  `protobuf:"bytes,1,opt,name=symbol,proto3" json:"symbol,omitempty"`
    Timestamp int64   `protobuf:"varint,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
    Open      float64 `protobuf:"fixed64,3,opt,name=open,proto3" json:"open,omitempty"`
    High      float64 `protobuf:"fixed64,4,opt,name=high,proto3" json:"high,omitempty"`
    Low       float64 `protobuf:"fixed64,5,opt,name=low,proto3" json:"low,omitempty"`
    Close     float64 `protobuf:"fixed64,6,opt,name=close,proto3" json:"close,omitempty"`
    Volume    float64 `protobuf:"fixed64,7,opt,name=volume,proto3" json:"volume,omitempty"`
}

func (x *Candle) Reset() { *x = Candle{} }
func (*Candle) ProtoMessage() {}

func (x *Candle) GetSymbol() string { if x != nil { return x.Symbol } return "" }
func (x *Candle) GetTimestamp() int64 { if x != nil { return x.Timestamp } return 0 }
func (x *Candle) GetOpen() float64 { if x != nil { return x.Open } return 0 }
func (x *Candle) GetHigh() float64 { if x != nil { return x.High } return 0 }
func (x *Candle) GetLow() float64 { if x != nil { return x.Low } return 0 }
func (x *Candle) GetClose() float64 { if x != nil { return x.Close } return 0 }
func (x *Candle) GetVolume() float64 { if x != nil { return x.Volume } return 0 }

type CandleServiceClient interface {
    Subscribe(ctx context.Context, in *CandleRequest, opts ...grpc.CallOption) (CandleService_SubscribeClient, error)
}

type candleServiceClient struct { cc grpc.ClientConnInterface }

func NewCandleServiceClient(cc grpc.ClientConnInterface) CandleServiceClient {
    return &candleServiceClient{cc}
}

func (c *candleServiceClient) Subscribe(ctx context.Context, in *CandleRequest, opts ...grpc.CallOption) (CandleService_SubscribeClient, error) {
    stream, err := c.cc.NewStream(ctx, &_CandleService_serviceDesc.Streams[0], "/candle.CandleService/Subscribe", opts...)
    if err != nil { return nil, err }
    x := &candleServiceSubscribeClient{stream}
    if err := x.ClientStream.SendMsg(in); err != nil { return nil, err }
    if err := x.ClientStream.CloseSend(); err != nil { return nil, err }
    return x, nil
}

type CandleService_SubscribeClient interface {
    Recv() (*Candle, error)
    grpc.ClientStream
}

type candleServiceSubscribeClient struct { grpc.ClientStream }

func (x *candleServiceSubscribeClient) Recv() (*Candle, error) {
    m := new(Candle)
    if err := x.ClientStream.RecvMsg(m); err != nil { return nil, err }
    return m, nil
}

type CandleServiceServer interface {
    Subscribe(*CandleRequest, CandleService_SubscribeServer) error
}

type UnimplementedCandleServiceServer struct{}

func (UnimplementedCandleServiceServer) Subscribe(*CandleRequest, CandleService_SubscribeServer) error {
    return status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}

func RegisterCandleServiceServer(s grpc.ServiceRegistrar, srv CandleServiceServer) {
    s.RegisterService(&_CandleService_serviceDesc, srv)
}

type CandleService_SubscribeServer interface {
    Send(*Candle) error
    grpc.ServerStream
}

type candleServiceSubscribeServer struct { grpc.ServerStream }

func (x *candleServiceSubscribeServer) Send(m *Candle) error { return x.ServerStream.SendMsg(m) }

func _CandleService_Subscribe_Handler(srv interface{}, stream grpc.ServerStream) error {
    m := new(CandleRequest)
    if err := stream.RecvMsg(m); err != nil {
        return err
    }
    return srv.(CandleServiceServer).Subscribe(m, &candleServiceSubscribeServer{stream})
}

var _CandleService_serviceDesc = grpc.ServiceDesc{
    ServiceName: "candle.CandleService",
    HandlerType: (*CandleServiceServer)(nil),
    Methods: []grpc.MethodDesc{},
    Streams: []grpc.StreamDesc{{
        StreamName:    "Subscribe",
        Handler:       _CandleService_Subscribe_Handler,
        ServerStreams: true,
    }},
    Metadata: "candle.proto",
}