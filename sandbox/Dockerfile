FROM rust:1.97-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM node:22-slim
RUN useradd --create-home --shell /usr/sbin/nologin sandbox
COPY --from=builder /build/target/release/healix-sandbox /usr/local/bin/healix-sandbox
USER sandbox
WORKDIR /workspace
EXPOSE 4000
CMD ["healix-sandbox"]
