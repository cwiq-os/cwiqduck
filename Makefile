PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
WASM_RULES := wasm_mvp wasm_eh wasm_other
is_wasm_rule = $(filter $(1),$(WASM_TARGETS))

ifeq ($(call is_wasm_rule,$(MAKECMDGOALS)),)
  GEN ?= ninja
else
  GEN ?= make
endif

EXT_NAME=cwiq
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
