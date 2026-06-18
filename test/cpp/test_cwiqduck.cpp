// C++ unit tests for cwiqduck internals.
//
// These call the extension's classes/functions directly (ConvertLocalPathToS3,
// S3RedirectProtocolFileSystem) so they can assert on the parts that are awkward to
// reach from SQL: the xattr error-path messages, directory-op delegation, and the
// write/redirect dispatch decisions.
//
// getxattr is mocked via the LD_PRELOAD shim (test/mock/cwiqmock.c); the fake S3 target
// is a real local file addressed with a file:// URL. Both are set up by
// test/run_with_mock.sh, which also passes the fixtures directory in CWIQ_MOCK_DIR.

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "cwiqduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"

#include <cstdlib>
#include <string>
#include <sys/stat.h>

using namespace duckdb;

// Fixtures dir, provided by the wrapper. All mock.cfg entries are anchored here.
static std::string MockDir() {
	const char *d = std::getenv("CWIQ_MOCK_DIR");
	REQUIRE(d != nullptr); // must be run via test/run_with_mock.sh
	return std::string(d);
}

static std::string P(const std::string &rel) {
	return MockDir() + "/" + rel;
}

// ---------------------------------------------------------------------------
// ConvertLocalPathToS3
// ---------------------------------------------------------------------------

TEST_CASE("ConvertLocalPathToS3 happy path returns mocked url + local inode metadata") {
	auto info = ConvertLocalPathToS3(P("mount/data.csv"));

	REQUIRE(info.s3_url == "file://" + P("target/data.csv"));

	// content_length / mtime come from the local inode, not the remote object.
	struct stat st;
	REQUIRE(::stat(P("mount/data.csv").c_str(), &st) == 0);
	REQUIRE(info.content_length == (idx_t)st.st_size);
	REQUIRE(info.content_length > 0);
}

TEST_CASE("ConvertLocalPathToS3 error paths produce specific messages") {
	auto expect_msg = [](const std::string &path, const std::string &needle) {
		bool threw = false;
		try {
			ConvertLocalPathToS3(path);
		} catch (const std::exception &e) {
			threw = true;
			INFO("path=" << path << " what=" << e.what());
			REQUIRE(std::string(e.what()).find(needle) != std::string::npos);
		}
		REQUIRE(threw);
	};

	SECTION("ENODATA — attribute absent") {
		expect_msg(P("err/enodata"), "attribute does not exist");
	}
	SECTION("ENOENT — file absent") {
		expect_msg(P("err/enoent"), "file does not exist");
	}
	SECTION("EACCES — injected by shim") {
		expect_msg(P("err/eacces"), "permission denied");
	}
	SECTION("ENOTSUP — injected by shim") {
		expect_msg(P("err/enotsup"), "xattrs not supported");
	}
	SECTION("empty xattr value — injected by shim") {
		expect_msg(P("err/empty"), "Empty xattr value");
	}
}

// ---------------------------------------------------------------------------
// S3RedirectProtocolFileSystem dispatch gating
// ---------------------------------------------------------------------------

TEST_CASE("CanHandleFile / FileExists gate on the cwiqfs xattr") {
	DuckDB db(nullptr);
	S3RedirectProtocolFileSystem fs(*db.instance);

	// xattr present -> this FS handles the path.
	REQUIRE(fs.CanHandleFile(P("mount/data.csv")) == true);
	REQUIRE(fs.FileExists(P("mount/data.csv")) == true);

	// Already-a-URL is rejected outright (avoids recursion / double-redirect).
	REQUIRE(fs.CanHandleFile("http://example.com/object") == false);
	REQUIRE(fs.CanHandleFile("https://example.com/object") == false);

	// Real file with no xattr -> not ours.
	REQUIRE(fs.CanHandleFile(P("target/data.csv")) == false);
	REQUIRE(fs.FileExists(P("target/data.csv")) == false);
}

// ---------------------------------------------------------------------------
// OpenFile dispatch: read redirects, write falls back to local
// ---------------------------------------------------------------------------

TEST_CASE("OpenFile read returns an S3 redirect handle that serves the target bytes") {
	DuckDB db(nullptr);
	S3RedirectProtocolFileSystem fs(*db.instance);

	auto handle = fs.OpenFile(P("mount/data.csv"), FileFlags::FILE_FLAGS_READ);
	REQUIRE(handle != nullptr);
	auto *redirect = dynamic_cast<S3RedirectFileHandle *>(handle.get());
	REQUIRE(redirect != nullptr);

	// First bytes must match the (fake) remote target, not the local mount file.
	char buf[7] = {0};
	fs.Read(*handle, buf, 7, 0); // positional read -> borrow-pool path
	REQUIRE(std::string(buf, 7) == "id,name");
}

TEST_CASE("OpenFile for writing falls back to a local handle (not a redirect handle)") {
	DuckDB db(nullptr);
	S3RedirectProtocolFileSystem fs(*db.instance);

	auto flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;
	auto handle = fs.OpenFile(P("mount/writable.csv"), flags);
	REQUIRE(handle != nullptr);
	// The fallback returns a local_fs handle; it must NOT be our redirect handle.
	REQUIRE(dynamic_cast<S3RedirectFileHandle *>(handle.get()) == nullptr);
}

// ---------------------------------------------------------------------------
// Handle-based writes are unsupported; path-based namespace ops delegate to local FS
// ---------------------------------------------------------------------------

TEST_CASE("Handle-based Write/Truncate throw NotImplementedException") {
	DuckDB db(nullptr);
	S3RedirectProtocolFileSystem fs(*db.instance);

	auto handle = fs.OpenFile(P("mount/data.csv"), FileFlags::FILE_FLAGS_READ);
	char buf[4] = {0};
	REQUIRE_THROWS_AS(fs.Write(*handle, buf, 4, 0), NotImplementedException);
	REQUIRE_THROWS_AS(fs.Truncate(*handle, 0), NotImplementedException);
}

TEST_CASE("Directory / namespace ops delegate to the local filesystem") {
	DuckDB db(nullptr);
	S3RedirectProtocolFileSystem fs(*db.instance);

	auto base = P("dirops");
	auto dir = base + "/sub";
	auto file_a = dir + "/a.txt";
	auto file_b = dir + "/b.txt";

	if (fs.DirectoryExists(base)) {
		fs.RemoveDirectory(base);
	}

	fs.CreateDirectory(base);
	fs.CreateDirectory(dir);
	REQUIRE(fs.DirectoryExists(dir) == true);

	// create a file via the local fallback open, then exercise Move + ListFiles + Remove
	{
		auto h = fs.OpenFile(file_a, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
		const char *msg = "hi";
		h->Write((void *)msg, 2);
		h->Close();
	}
	fs.MoveFile(file_a, file_b);

	bool saw_b = false;
	fs.ListFiles(dir, [&](const string &name, bool is_dir) {
		if (name == "b.txt") {
			saw_b = true;
		}
	});
	REQUIRE(saw_b == true);

	fs.RemoveFile(file_b);
	fs.RemoveDirectory(dir);
	fs.RemoveDirectory(base);
	REQUIRE(fs.DirectoryExists(base) == false);
}
