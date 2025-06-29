FROM python:3.11-slim

# Instala compiladores y toolchain de Rust
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential rustc cargo && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /bot

# Dependencias de Python
COPY requirements.txt backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r backend/requirements.txt \
    && pip install maturin

# Copia el proyecto
COPY . .

# Compila todas las extensiones de Rust
RUN maturin develop --release -m fast_tp_sl/Cargo.toml && \
    maturin develop --release -m fast_indicators_rust/Cargo.toml && \
    maturin develop --release -m rust_backtesting/Cargo.toml && \
    cargo build --release --manifest-path rust_backtesting/Cargo.toml --bin backtest_server && \
    maturin develop --release -m score_rust/Cargo.toml && \
    maturin develop --release -m umbral_rust/Cargo.toml && \
    maturin develop --release -m trailing_rust/Cargo.toml && \
    maturin develop --release -m orders_persist_rust/Cargo.toml

CMD ["python", "scripts/supervisor.py"]