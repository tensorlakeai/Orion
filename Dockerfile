FROM rust:1-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends libclang-dev make pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV LIBCLANG_PATH=/usr/lib/llvm-14/lib
ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
ENV PATH=/usr/local/cargo/bin:$PATH

WORKDIR /workspace
COPY . .
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/orion /usr/local/bin/orion
COPY docker/cluster /etc/orion

ENTRYPOINT ["/usr/local/bin/orion"]
