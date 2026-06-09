#define DUCKDB_EXTENSION_MAIN

#include "cwiqduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
// v1.5.3+ wraps the db filesystem in OpenerFileSystem, which auto-injects the opener and
// rejects an explicit one.  The header didn't exist before v1.5.3.
#if __has_include("duckdb/common/opener_file_system.hpp")
#define DUCKDB_HAS_OPENER_FILESYSTEM 1
#endif
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/logging/log_manager.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#ifdef __linux__
#include <sys/xattr.h>
#include <sys/stat.h>
#endif

#include <errno.h>
#include <cstring>

namespace duckdb {

// ---------------------------------------------------------------------------
// S3RedirectFileHandle — pool internals
// ---------------------------------------------------------------------------

unique_ptr<FileHandle> S3RedirectFileHandle::OpenHandle() const {
	CWIQ_LOG_DEBUG(db_instance, "OpenHandle: opening S3 connection for %s", s3_url);
	// Plain read flags: correctness comes from the pool's exclusive-ownership model
	// (no two threads share a handle's mutable state), not from any httpfs-internal flag.
	auto flags = FileFlags::FILE_FLAGS_READ;
	auto &main_fs = FileSystem::GetFileSystem(db_instance);
#ifdef DUCKDB_HAS_OPENER_FILESYSTEM
	// v1.5.3+: DatabaseFileSystem (OpenerFileSystem) auto-injects the db-level opener
	// and explicitly rejects a caller-supplied one.
	return main_fs.OpenFile(s3_url, flags, nullptr);
#else
	// v1.4.x: no auto-injection; pass the opener so httpfs can read S3 credentials.
	return main_fs.OpenFile(s3_url, flags, file_opener);
#endif
}

S3RedirectFileHandle::BorrowedHandle S3RedirectFileHandle::BorrowHandle() {
	{
		std::lock_guard<std::mutex> lk(pool_mu);
		if (!idle_pool.empty()) {
			auto h = std::move(idle_pool.back());
			idle_pool.pop_back();
			return BorrowedHandle(*this, std::move(h));
		}
	}
	// Pool empty: open a new handle without holding the lock (avoids blocking
	// other threads returning handles while a HEAD/GET is in flight).
	CWIQ_LOG_DEBUG(db_instance, "BorrowHandle: pool miss, opening new handle for %s", s3_url);
	return BorrowedHandle(*this, OpenHandle());
}

void S3RedirectFileHandle::ReturnHandle(unique_ptr<FileHandle> h) {
	std::lock_guard<std::mutex> lk(pool_mu);
	idle_pool.push_back(std::move(h));
	CWIQ_LOG_DEBUG(db_instance, "ReturnHandle: pool size now %s for %s", std::to_string(idle_pool.size()), s3_url);
}

// ---------------------------------------------------------------------------
// S3RedirectFileHandle — public interface
// ---------------------------------------------------------------------------

S3RedirectFileHandle::S3RedirectFileHandle(FileSystem &fs, DatabaseInstance &db, const string &url,
                                           idx_t content_length, timestamp_t last_modified,
                                           optional_ptr<FileOpener> opener)
    : FileHandle(fs, url, FileFlags::FILE_FLAGS_READ), s3_url(url), known_content_length(content_length),
      last_modified_time(last_modified), db_instance(db), file_opener(opener) {
	// Open the primary handle eagerly (sequential cursor / metadata ops).
	// This also validates the S3 URL and credentials at open time rather than
	// deferring errors to the first read.
	primary_handle = OpenHandle();
}

FileHandle &S3RedirectFileHandle::GetPrimaryHandle() {
	D_ASSERT(primary_handle);
	return *primary_handle;
}

idx_t S3RedirectFileHandle::GetFileSize() {
	return known_content_length;
}

void S3RedirectFileHandle::Close() {
	if (primary_handle) {
		primary_handle->Close();
	}
	std::lock_guard<std::mutex> lk(pool_mu);
	for (auto &h : idle_pool) {
		if (h) {
			h->Close();
		}
	}
	idle_pool.clear();
}

// Positional read: borrow an exclusive handle from the pool so no two threads
// share any mutable handle state regardless of how the underlying httpfs is implemented.
void S3RedirectFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	CWIQ_LOG_TRACE(db_instance, "Read: %s bytes @ offset %s", std::to_string(nr_bytes), std::to_string(location));
	auto borrowed = BorrowHandle();
	borrowed.get().Read(buffer, nr_bytes, location);
}

// Sequential read: uses the primary handle (single-threaded sequential access path).
int64_t S3RedirectFileHandle::Read(void *buffer, idx_t nr_bytes) {
	CWIQ_LOG_TRACE(db_instance, "Read: %s bytes (sequential)", std::to_string(nr_bytes));
	return GetPrimaryHandle().Read(buffer, nr_bytes);
}

bool S3RedirectFileHandle::CanSeek() {
	return true;
}

void S3RedirectFileHandle::Sync() {
	if (primary_handle) {
		primary_handle->Sync();
	}
}

FileType S3RedirectFileHandle::GetType() {
	return GetPrimaryHandle().GetType();
}

timestamp_t S3RedirectFileHandle::GetLastModifiedTime() {
	return last_modified_time;
}

void S3RedirectFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	GetPrimaryHandle().Write(buffer, nr_bytes);
}

int64_t S3RedirectFileHandle::Write(void *buffer, idx_t nr_bytes) {
	return GetPrimaryHandle().Write(buffer, nr_bytes);
}

void S3RedirectFileHandle::Truncate(int64_t new_size) {
	return GetPrimaryHandle().Truncate(new_size);
}

// ---------------------------------------------------------------------------
// S3RedirectProtocolFileSystem
// ---------------------------------------------------------------------------

unique_ptr<FileHandle> S3RedirectProtocolFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                              optional_ptr<FileOpener> opener) {
	// Writes are not redirectable — the S3 target is read-only. Fall back to the real
	// on-disk file on the CWIQ FS mount so the kernel/mount handles persistence. The
	// returned handle references local_fs, so all later ops bypass this FS entirely
	// (including the read side of a READ|WRITE open).
	if (flags.OpenForWriting()) {
		CWIQ_LOG_DEBUG(db_instance, "OpenFile: %s opened for writing, falling back to local FS", path);
		return local_fs.OpenFile(path, flags, opener);
	}
	try {
		auto s3_info = ConvertLocalPathToS3(path);
		CWIQ_LOG_INFO(db_instance, "OpenFile: redirecting %s to S3", path);
		return make_uniq<S3RedirectFileHandle>(*this, db_instance, s3_info.s3_url, s3_info.content_length,
		                                       s3_info.last_modified_time, opener);
	} catch (const std::exception &e) {
		throw IOException("Failed to redirect to S3: " + string(e.what()));
	}
}

bool S3RedirectProtocolFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	try {
		ConvertLocalPathToS3(filename);
		return true;
	} catch (...) {
		return false;
	}
}

bool S3RedirectProtocolFileSystem::CanHandleFile(const string &fpath) {
#ifdef __linux__
	if (StringUtil::StartsWith(fpath, "http")) // Check if file is already a URL
		return false;

	// Check if file is in CWIQFS
	const char *xattr_name = "system.cwiqfs.s3_url";
	ssize_t size = getxattr(fpath.c_str(), xattr_name, nullptr, 0);
	return size > 0;
#else
	return false;
#endif
}

// Positional read: delegates to S3RedirectFileHandle::Read(buf,n,loc) which
// borrows an exclusive pool handle — no shared mutable state between callers.
void S3RedirectProtocolFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto *h = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (h) {
		return h->Read(buffer, nr_bytes, location);
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

// Sequential read: uses the primary handle.
int64_t S3RedirectProtocolFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto *h = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (h) {
		return h->Read(buffer, nr_bytes);
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

void S3RedirectProtocolFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto *h = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (h) {
		return h->GetPrimaryHandle().Seek(location);
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

idx_t S3RedirectProtocolFileSystem::SeekPosition(FileHandle &handle) {
	auto *h = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (h) {
		return h->GetPrimaryHandle().SeekPosition();
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

int64_t S3RedirectProtocolFileSystem::GetFileSize(FileHandle &handle) {
	auto *h = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (h) {
		return h->GetFileSize();
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

FileType S3RedirectProtocolFileSystem::GetFileType(FileHandle &handle) {
	return FileType::FILE_TYPE_REGULAR;
}

void S3RedirectProtocolFileSystem::FileSync(FileHandle &handle) {
	if (auto *h = dynamic_cast<S3RedirectFileHandle *>(&handle)) {
		h->Sync();
	}
}

timestamp_t S3RedirectProtocolFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto *h = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (h) {
		return h->GetLastModifiedTime();
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

vector<OpenFileInfo> S3RedirectProtocolFileSystem::Glob(const string &path, FileOpener *opener) {
	return local_fs.Glob(path, nullptr);
}

// ---------------------------------------------------------------------------
// ConvertLocalPathToS3
// ---------------------------------------------------------------------------

S3RedirectInfo ConvertLocalPathToS3(const string &local_path) {
#ifndef __linux__
	throw IOException("Failed to read xattr value for " + local_path + ": " + strerror(errno));
#else
	S3RedirectInfo info;
	const char *xattr_name = "system.cwiqfs.s3_url";
	ssize_t size = getxattr(local_path.c_str(), xattr_name, nullptr, 0);

	if (size < 0) {
		int err = errno;
		string error_msg = "Failed to get xattr '" + string(xattr_name) + "' for " + local_path + ": ";

		switch (err) {
		case ENODATA:
			error_msg += "attribute does not exist";
			break;
		case ENOENT:
			error_msg += "file does not exist";
			break;
		case EACCES:
			error_msg += "permission denied";
			break;
		case ENOTSUP:
			error_msg += "xattrs not supported on this filesystem";
			break;
		default:
			error_msg += strerror(err);
		}
		throw IOException(error_msg);
	}

	if (size == 0) {
		throw IOException("Empty xattr value for " + local_path);
	}

	vector<char> buffer(size);
	ssize_t actual_size = getxattr(local_path.c_str(), xattr_name, buffer.data(), size);

	if (actual_size < 0) {
		throw IOException("Failed to read xattr value for " + local_path + ": " + strerror(errno));
	}

	// Convert to string (xattr values may not be null-terminated)
	info.s3_url = string(buffer.data(), actual_size);
	struct stat st;
	if (stat(local_path.c_str(), &st) != 0) {
		throw IOException("Failed to stat file " + local_path + ": " + strerror(errno));
	}

	info.content_length = st.st_size;
	info.last_modified_time = st.st_mtime;

	return info;
#endif
}

// ---------------------------------------------------------------------------
// Extension entry points
// ---------------------------------------------------------------------------

std::string CwiqduckExtension::Name() {
	return "cwiqduck";
}

std::string CwiqduckExtension::Version() const {
#ifdef EXT_VERSION_CWIQDUCK
	return EXT_VERSION_CWIQDUCK;
#else
	return "";
#endif
}

static void LoadInternal(DatabaseInstance &db) {
#ifndef __linux__
	std::cout << "Error: cwiqduck extension not implemented for non-Linux platforms.";
	return;
#endif

	// Force load httpfs first
	Connection con(db);
	auto result = con.Query("LOAD httpfs");
	if (result->HasError()) {
		std::cout << "Warning: Could not load httpfs: " << result->GetError() << std::endl;
		// Try to install and load
		con.Query("INSTALL httpfs");
		con.Query("LOAD httpfs");
	}

	std::cout << "cwiqduck extension enabled" << std::endl;

	// Register our native log type so users can scope logging via `CALL enable_logging('cwiqduck')`
	// and filter duckdb_logs by log_type. RegisterLogType throws on a duplicate (e.g. reload), so
	// swallow that and keep going.
	try {
		db.GetLogManager().RegisterLogType(make_uniq<CwiqduckLogType>());
	} catch (const std::exception &) {
		// Already registered — nothing to do.
	}

	auto s3_redirect_fs = make_uniq<S3RedirectProtocolFileSystem>(db);

	// Register the filesystem with DuckDB for the s3redirect:// protocol
	db.GetFileSystem().RegisterSubSystem(std::move(s3_redirect_fs));
}

void CwiqduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader.GetDatabaseInstance());
}
} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(cwiqduck, loader) {
	duckdb::LoadInternal(loader.GetDatabaseInstance());
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
