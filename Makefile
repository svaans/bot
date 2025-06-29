.PHONY: all rust cpp proto go_services frontend clippy audit cpp_lint

all: clippy audit cpp_lint rust cpp proto go_services frontend

rust:
	maturin develop --release -m fast_tp_sl/Cargo.toml
	maturin develop --release -m fast_indicators_rust/Cargo.toml
	maturin develop --release -m score_rust/Cargo.toml
	maturin develop --release -m trailing_rust/Cargo.toml
	maturin develop --release -m umbral_rust/Cargo.toml
	maturin develop --release -m rust_backtesting/Cargo.toml

clippy:
	cargo clippy --manifest-path fast_tp_sl/Cargo.toml --all-targets -- -D warnings
	cargo clippy --manifest-path fast_indicators_rust/Cargo.toml --all-targets -- -D warnings
	cargo clippy --manifest-path rust_backtesting/Cargo.toml --all-targets -- -D warnings

audit:
	cargo audit --manifest-path fast_tp_sl/Cargo.toml
	cargo audit --manifest-path fast_indicators_rust/Cargo.toml
	cargo audit --manifest-path rust_backtesting/Cargo.toml

cpp:
	clang-tidy fast_indicators/fast_indicators.cpp -- -std=c++17
	CXXFLAGS="-fsanitize=address -O1 -fno-omit-frame-pointer" \
	LDFLAGS="-fsanitize=address" \
	python -m pip install .

proto:
	python -m grpc_tools.protoc -I orders_worker/proto --python_out=core --grpc_python_out=core orders_worker/proto/orders.proto

go_services:
	go build -o candle_service/candle-service ./candle_service
	go build -o orders_worker/orders-worker ./orders_worker

frontend:
	npm run build --prefix frontend