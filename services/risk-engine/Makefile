.PHONY: build test lint fmt check

build:
	cargo build --release

test:
	cargo test --all-features

lint:
	cargo clippy --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all

check: fmt lint test
