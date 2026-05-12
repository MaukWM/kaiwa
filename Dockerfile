FROM rust:latest AS builder

RUN apt-get update && apt-get install -y \
    cmake pkg-config libopus-dev libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies: copy manifests first, build with dummy source.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

# Build the real binary.
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus0 libsodium23 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/kaiwa /usr/local/bin/kaiwa

ENTRYPOINT ["kaiwa"]
