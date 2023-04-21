ifeq (, $(shell which cargo))
$(warning No `cargo` in path, consider installing Cargo with:)
$(warning - `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
$(warning - Or visit https://www.rust-lang.org/tools/install for more on installing Rust.)
$(error Unable to invoke cargo)
endif

RELEASE_DIR = target/release
CLIENT_EXE = client
SERVER_EXE = server

.PHONY: release
release: clean-exe
	cargo build --release
	mv $(RELEASE_DIR)/tx-$(CLIENT_EXE) $(CLIENT_EXE)
	mv $(RELEASE_DIR)/tx-$(SERVER_EXE) $(SERVER_EXE)

clean-exe:
	rm -f $(CLIENT_EXE) $(SERVER_EXE)

clean: clean-exe
	cargo clean