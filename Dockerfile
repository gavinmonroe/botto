FROM rust:1.85-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /bin/false botto
COPY --from=builder /build/target/release/botto /usr/local/bin/botto
USER botto
EXPOSE 7700
VOLUME /data
ENV BOTTO_DATA_DIR=/data
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/botto", "--version"]
CMD ["botto", "--data-dir", "/data"]
