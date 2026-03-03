amd64-musl:
	cargo build --release --target x86_64-unknown-linux-musl

amd64:
	cargo build --release --target x86_64-unknown-linux-gnu

amd64-static:
	RUSTFLAGS='-C target-feature=+crt-static -C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-gnu

.PHONY: amd64-musl amd64 amd64-static
