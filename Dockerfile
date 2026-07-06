# syntax=docker/dockerfile:1.7
FROM rust:1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bins && \
    cp target/release/server /server && \
    cp target/release/worker /worker

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /server /usr/local/bin/server
COPY --from=builder /worker /usr/local/bin/worker
CMD ["/usr/local/bin/server"]
