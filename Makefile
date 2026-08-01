default:
	$(MAKE) amd64-v3-linux-musl

dashboard:
	make -C dashboard

amd64-v3-linux-musl:
	@echo "- TARGET CPU: amd64 v3"
	@echo "- TARGET LIBC: musl"
	@RUSTFLAGS='-C target-cpu=x86-64-v3 -C link-arg=-fuse-ld=wild' cargo build --release --target x86_64-unknown-linux-musl

amd64-v3-linux-gnu:
	@echo "- TARGET CPU: amd64 v3"
	@echo "- TARGET LIBC: glibc"
	@RUSTFLAGS='-C target-cpu=x86-64-v3 -C link-arg=-fuse-ld=wild' cargo build --release --target x86_64-unknown-linux-gnu

aarch64-linux-musl:
	@echo "- TARGET CPU: aarch64"
	@echo "- TARGET LIBC: musl"
	@cargo build --release --target aarch64-unknown-linux-musl

.PHONY: default dashboard amd64-v3-linux-musl amd64-v3-linux-gnu aarch64-linux-musl
