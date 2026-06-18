#!/usr/bin/env bash
# Run the cwiqduck C++ unit tests with getxattr + S3 mocked.
#
#   - getxattr is intercepted by an LD_PRELOAD shim (test/mock/cwiqmock.c) so a plain
#     local file can masquerade as a CWIQ-FS file carrying a system.cwiqfs.s3_url xattr.
#   - the "remote" S3 object is a real local file addressed with a file:// URL.
#
# It builds the shim, generates fixtures, then builds and runs the C++ unit tests
# (test/cpp/test_cwiqduck.cpp) under the preload. Requires a prior `make release`.
#
# Usage:  source setenv.sh && ./test/run_with_mock.sh
# Env:    BUILD_DIR (default build/release), CC / CXX (default cc / c++).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BUILD_DIR="${BUILD_DIR:-build/release}"
CC_BIN="${CC:-cc}"
CXX_BIN="${CXX:-c++}"

STATIC_LIB="$BUILD_DIR/src/libduckdb_static.a"
if [[ ! -e "$STATIC_LIB" ]]; then
	echo "error: $STATIC_LIB not found — run 'make release' first (BUILD_DIR=$BUILD_DIR)." >&2
	exit 1
fi

FIX="$(mktemp -d "${TMPDIR:-/tmp}/cwiqmock.XXXXXX")"
cleanup() { rm -rf "$FIX"; }
trap cleanup EXIT

echo "==> fixtures dir: $FIX"
mkdir -p "$FIX/mount" "$FIX/target" "$FIX/err" "$FIX/dirops"

# --- build the LD_PRELOAD shim ---------------------------------------------
SHIM="$FIX/libcwiqmock.so"
echo "==> building getxattr shim"
"$CC_BIN" -shared -fPIC -O2 -o "$SHIM" test/mock/cwiqmock.c -ldl

# --- generate fixtures ------------------------------------------------------
# CSV: target holds the canonical rows; the mount file is byte-length-identical but
# garbled, so a correct read can only come from the redirect target.
printf 'id,name\n1,alice\n2,bob\n3,carol\n' > "$FIX/target/data.csv"
tr 'a-z' 'z' < "$FIX/target/data.csv" > "$FIX/mount/data.csv"
if [[ "$(wc -c < "$FIX/target/data.csv")" != "$(wc -c < "$FIX/mount/data.csv")" ]]; then
	echo "error: mount/target CSV lengths differ; GetFileSize would be wrong." >&2
	exit 1
fi

# --- mock config: path<TAB>{file://url | ERRNO=NAME | EMPTY} ----------------
# tmpfs (/tmp) returns ENOTSUP for system.* xattrs, so error cases are injected by the
# shim rather than relying on the host filesystem's behaviour.
CFG="$FIX/mock.cfg"
{
	printf '%s\tfile://%s\n' "$FIX/mount/data.csv"     "$FIX/target/data.csv"
	printf '%s\tfile://%s\n' "$FIX/mount/writable.csv" "$FIX/target/unused.csv"
	printf '%s\tERRNO=ENODATA\n' "$FIX/err/enodata"
	printf '%s\tERRNO=ENOENT\n'  "$FIX/err/enoent"
	printf '%s\tERRNO=EACCES\n'  "$FIX/err/eacces"
	printf '%s\tERRNO=ENOTSUP\n' "$FIX/err/enotsup"
	printf '%s\tEMPTY\n'         "$FIX/err/empty"
} > "$CFG"

# --- build the C++ unit tests (link every duckdb archive in one group) ------
CPP_BIN="$FIX/test_cwiqduck"
echo "==> building C++ unit tests"
ARCHIVES=$(find "$BUILD_DIR" -name '*.a' | grep -vE 'dummy_static|sqlite3_api_wrapper')
"$CXX_BIN" -std=c++11 -O0 test/cpp/test_cwiqduck.cpp \
	-I duckdb/src/include -I duckdb/third_party/catch -I duckdb/test/include -I src/include \
	-Wl,--start-group $ARCHIVES -Wl,--end-group \
	-lssl -lcrypto -lpthread -ldl -lm \
	-o "$CPP_BIN"

# --- run under the preload --------------------------------------------------
export LD_PRELOAD="$SHIM"
export CWIQ_MOCK_CONFIG="$CFG"
export CWIQ_MOCK_DIR="$FIX"

echo
echo "==> C++ unit tests"
if "$CPP_BIN"; then
	echo
	echo "ALL MOCK TESTS PASSED"
else
	rc=$?
	echo
	echo "MOCK TESTS FAILED (rc=$rc)" >&2
	exit $rc
fi
