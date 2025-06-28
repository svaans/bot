.PHONY: all rust cpp proto go_services frontend

all: rust cpp proto go_services frontend

rust:
		maturin develop --release -m fast_tp_sl/Cargo.toml
        maturin develop --release -m fast_indicators_rust/Cargo.toml

cpp:
	python -m pip install .

proto:
	python -m grpc_tools.protoc -I orders_worker/proto --python_out=core --grpc_python_out=core orders_worker/proto/orders.proto

go_services:
	go build -o candle_service/candle-service ./candle_service
	go build -o orders_worker/orders-worker ./orders_worker

frontend:
	npm run build --prefix frontend