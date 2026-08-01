default:
	$(MAKE) amd64-v3-linux-musl

dashboard:
	make -C dashboard

amd64-v3-linux-musl:
	RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-musl

amd64-v3-linux-glibc:
	RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-gnu

.PHONY: default dashboard amd64-v3-linux-musl amd64-v3-linux-glibc
