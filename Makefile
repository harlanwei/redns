default:
	$(MAKE) amd64-v3-linux-musl

dashboard:
	make -C dashboard

# Linker selection: prefer `wild`, fall back to `mold`, else the driver's
# default linker. clang only accepts linkers by path (--ld-path), so the
# binary is resolved at make time.
WILD_LINKER := $(shell which wild 2>/dev/null)
MOLD_LINKER := $(shell which mold 2>/dev/null)
ifneq ($(WILD_LINKER),)
LINKER_ARG := -C link-arg=--ld-path=$(WILD_LINKER)
else ifneq ($(MOLD_LINKER),)
LINKER_ARG := -C link-arg=--ld-path=$(MOLD_LINKER)
else
LINKER_ARG :=
endif
LINKER_LABEL := $(if $(WILD_LINKER),$(WILD_LINKER),$(if $(MOLD_LINKER),$(MOLD_LINKER),default))

amd64-v3-linux-musl:
	@echo "- TARGET CPU : amd64 v3"
	@echo "- TARGET OS  : Linux"
	@echo "- TARGET LIBC: musl"
	@echo "- LINKER     : $(LINKER_LABEL)"
	@RUSTFLAGS='-C target-cpu=x86-64-v3 $(LINKER_ARG)' cargo build --release --target x86_64-unknown-linux-musl

amd64-v3-linux-gnu:
	@echo "- TARGET CPU : amd64 v3"
	@echo "- TARGET OS  : Linux"
	@echo "- TARGET LIBC: glibc"
	@echo "- LINKER     : $(LINKER_LABEL)"
	@RUSTFLAGS='-C target-cpu=x86-64-v3 $(LINKER_ARG)' cargo build --release --target x86_64-unknown-linux-gnu

aarch64-linux-musl:
	@echo "- TARGET CPU : aarch64"
	@echo "- TARGET OS  : Linux"
	@echo "- TARGET LIBC: musl"
	@cargo build --release --target aarch64-unknown-linux-musl

aarch64-darwin:
	@echo "- TARGET CPU : Apple Silicon"
	@echo "- TARGET OS  : macOS"
	@echo "- TARGET LIBC: libSystem"
	@cargo build --release --target aarch64-apple-darwin

.PHONY: default dashboard amd64-v3-linux-musl amd64-v3-linux-gnu aarch64-linux-musl aarch64-darwin
