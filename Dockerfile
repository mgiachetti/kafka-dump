# Build Stage
FROM rust:1.45 AS builder
WORKDIR /usr/src/

RUN USER=root cargo new kafka-dump
WORKDIR /usr/src/kafka-dump
COPY Cargo.lock Cargo.toml ./
RUN cargo build --release
COPY ./ ./
RUN touch src/main.rs
RUN cargo build --release

# Bundle Stage
FROM debian:buster-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates
COPY --from=builder /usr/src/kafka-dump/target/release/kafka-dump .
COPY ./config ./config
CMD ["./kafka-dump"]
