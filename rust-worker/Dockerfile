FROM rust:1.86-bookworm AS builder

RUN apt-get update && apt-get install -y cmake build-essential pkg-config libssl-dev

WORKDIR /usr/src/rust-worker

COPY . .

RUN for i in 1 2 3; do \
			cargo build --release && exit 0; \
			if [ "$i" -eq 3 ]; then exit 1; fi; \
			echo "cargo build failed, retrying ($i/3)..."; \
			sleep 5; \
		done

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/rust-worker/target/release/rust-worker /usr/local/bin/rust-worker

CMD ["rust-worker"]