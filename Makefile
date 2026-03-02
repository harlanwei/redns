amd64-musl:
    cargo build --release --target x86_64-unknown-linux-musl

amd64:
    cargo build --release --target x86_64-unknown-linux-gnu

.PHONY: amd64-musl amd64
