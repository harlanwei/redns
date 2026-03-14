dashboard:
	make -C dashboard

amd64-musl: dashboard
	RUSTFLAGS='-C target-cpu=x86-64-v3' cargo zigbuild --release --target x86_64-unknown-linux-musl

amd64: dashboard
	RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-gnu

amd64-static: dashboard
	RUSTFLAGS='-C target-feature=+crt-static -C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-gnu


.PHONY: dashboard amd64-musl amd64 amd64-static
