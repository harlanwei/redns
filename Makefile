dashboard:
	make -C dashboard

amd64-v3-linux-musl-static: dashboard
	RUSTFLAGS='-C target-cpu=x86-64-v3' cargo zigbuild --release --target x86_64-unknown-linux-musl

amd64-v3-linux-glibc: dashboard
	RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-gnu

amd64-v3-linux-glibc-static: dashboard
	RUSTFLAGS='-C target-feature=+crt-static -C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-gnu

.PHONY: dashboard amd64-v3-linux-musl-static amd64-v3-linux-glibc amd64-v3-linux-glibc-static
