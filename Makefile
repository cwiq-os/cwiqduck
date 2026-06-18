PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=cwiqduck
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Mock-backed tests: exercise the redirect/read/write paths with getxattr + S3 mocked
# (LD_PRELOAD shim + file:// fixtures). Requires a prior `make release`.
.PHONY: test_mock
test_mock:
	BUILD_DIR=build/release ./test/run_with_mock.sh

# Make `make test` run the mock suite too, after the SQLLogicTests (test_release).
# GNU Make merges these prerequisites with the recipe-less `test: test_release`
# defined in extension-ci-tools/makefiles/duckdb_extension.Makefile.
test: test_mock
