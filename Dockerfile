# Build Stage
FROM rust:1.45 AS builder
WORKDIR /usr/src/

WORKDIR /usr/src/kafka-dump
COPY . .
RUN cargo build --release

# Bundle Stage
FROM debian:buster-slim
WORKDIR /app
COPY --from=builder /usr/src/kafka-dump/target/release/kafka-dump .
COPY ./config ./config
CMD ["./kafka-dump"]
