package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "log"
    "net"
    "os"

    pb "orders_worker/proto"

    _ "github.com/mattn/go-sqlite3"
    "github.com/xitongsys/parquet-go/parquet"
    "github.com/xitongsys/parquet-go/writer"
    "google.golang.org/grpc"
)

type server struct {
    pb.UnimplementedOrderWriterServer
}

func ensureDB(db *sql.DB) error {
    schema := `CREATE TABLE IF NOT EXISTS operaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, precio_entrada REAL, cantidad REAL, stop_loss REAL,
        take_profit REAL, timestamp TEXT, estrategias_activas TEXT,
        tendencia TEXT, max_price REAL, direccion TEXT,
        precio_cierre REAL, fecha_cierre TEXT, motivo_cierre TEXT,
        retorno_total REAL)`
    _, err := db.Exec(schema)
    return err
}

func writeParquet(symbol string, data map[string]any) error {
    path := "ordenes_reales/" + symbol + ".parquet"
    os.MkdirAll("ordenes_reales", 0o755)
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
    if err != nil {
        return err
    }
    defer f.Close()

    pw, err := writer.NewParquetWriter(f, data, 1)
    if err != nil {
        return err
    }
    pw.RowGroupSize = 128 * 1024
    pw.CompressionType = parquet.CompressionCodec_SNAPPY
    if err = pw.Write(data); err != nil {
        return err
    }
    return pw.WriteStop()
}

func (s *server) WriteOrders(ctx context.Context, req *pb.OrdersBatch) (*pb.WriteResponse, error) {
    db, err := sql.Open("sqlite3", "ordenes_reales/ordenes.db")
    if err != nil {
        return nil, err
    }
    defer db.Close()
    if err = ensureDB(db); err != nil {
        return nil, err
    }
    tx, err := db.Begin()
    if err != nil {
        return nil, err
    }
    stmt, _ := tx.Prepare(`INSERT INTO operaciones (
        symbol, precio_entrada, cantidad, stop_loss, take_profit, timestamp,
        estrategias_activas, tendencia, max_price, direccion, precio_cierre,
        fecha_cierre, motivo_cierre, retorno_total) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
    for _, o := range req.Orders {
        if _, err = stmt.Exec(
            o.Symbol, o.PrecioEntrada, o.Cantidad, o.StopLoss, o.TakeProfit,
            o.Timestamp, o.EstrategiasActivas, o.Tendencia, o.MaxPrice,
            o.Direccion, o.PrecioCierre, o.FechaCierre, o.MotivoCierre,
            o.RetornoTotal); err != nil {
            log.Printf("sqlite error: %v", err)
        }
        b, _ := json.Marshal(o)
        var m map[string]any
        json.Unmarshal(b, &m)
        if err = writeParquet(o.Symbol, m); err != nil {
            log.Printf("parquet error: %v", err)
        }
    }
    tx.Commit()
    return &pb.WriteResponse{Ok: true}, nil
}

func main() {
    lis, err := net.Listen("tcp", ":9100")
    if err != nil {
        log.Fatalf("listen: %v", err)
    }
    grpcServer := grpc.NewServer()
    pb.RegisterOrderWriterServer(grpcServer, &server{})
    log.Println("order worker listening on :9100")
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatal(err)
    }
}