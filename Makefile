dashboard:
	make -C dashboard

amd64-v3-linux-musl-static: dashboard
	RUSTFLAGS='-C target-cpu=x86-64-v3' cargo zigbuild --release --target x86_64-unknown-linux-musl

amd64-v3-linux-glibc: dashboard
	RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-gnu

amd64-v3-linux-glibc-static: dashboard
	RUSTFLAGS='-C target-feature=+crt-static -C target-cpu=x86-64-v3' cargo build --release --target x86_64-unknown-linux-gnu

PGO_DIR     := target/pgo-data
PGO_MERGED  := $(PGO_DIR)/merged.profdata
LLVM_PROFDATA := $(shell rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata

pgo-instrument:
	rm -rf $(PGO_DIR)
	mkdir -p $(PGO_DIR)
	RUSTFLAGS='-C target-feature=+crt-static -C target-cpu=x86-64-v3 -C profile-generate=$(CURDIR)/$(PGO_DIR)' \
		cargo build --release --target x86_64-unknown-linux-gnu
	@echo ""
	@echo "Instrumented binary at: target/x86_64-unknown-linux-gnu/release/redns"
	@echo "Copy it to the profiling machine and run with representative workload:"
	@echo "  LLVM_PROFILE_FILE=/path/to/default_%p.profraw ./redns <args...>"
	@echo "Then copy the .profraw files back into $(PGO_DIR)/ and run: make pgo-optimize"

pgo-merge:
	@test -d $(PGO_DIR) || (echo "Run 'make pgo-instrument' first" && false)
	@find $(PGO_DIR) -name '*.profraw' | grep -q . || (echo "No .profraw files in $(PGO_DIR). Copy them from the target device first." && false)
	$(LLVM_PROFDATA) merge -o $(PGO_MERGED) $(PGO_DIR)/*.profraw

pgo-optimize: pgo-merge
	RUSTFLAGS='-C target-cpu=x86-64-v3 -C profile-use=$(CURDIR)/$(PGO_MERGED)' \
		cargo zigbuild --release --target x86_64-unknown-linux-musl

.PHONY: dashboard amd64-v3-linux-musl-static amd64-v3-linux-glibc amd64-v3-linux-glibc-static pgo-instrument pgo-merge pgo-optimize
