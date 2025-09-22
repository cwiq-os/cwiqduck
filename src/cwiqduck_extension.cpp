#define DUCKDB_EXTENSION_MAIN

#include "cwiqduck_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#ifdef __linux__
#include <sys/xattr.h>
#include <sys/stat.h>
#endif

#include <errno.h>
#include <cstring>

namespace duckdb {

// Lazy-load the actual S3 handle when needed
FileHandle &S3RedirectFileHandle::GetS3Handle() {
	if (!s3_handle) {
		// Let DuckDB's httpfs handle the S3 URL
		auto &main_fs = FileSystem::GetFileSystem(db_instance);
		s3_handle = main_fs.OpenFile(s3_url, FileFlags::FILE_FLAGS_READ, nullptr);
	}
	return *s3_handle;
}

idx_t S3RedirectFileHandle::GetFileSize() {
	return known_content_length;
}

void S3RedirectFileHandle::Close() {
	if (s3_handle) {
		s3_handle->Close();
	}
}

void S3RedirectFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	return GetS3Handle().Read(buffer, nr_bytes, location);
}

int64_t S3RedirectFileHandle::Read(void *buffer, idx_t nr_bytes) {
	return GetS3Handle().Read(buffer, nr_bytes);
}

bool S3RedirectFileHandle::CanSeek() {
	return true;
}

void S3RedirectFileHandle::Sync() {
	if (s3_handle)
		s3_handle->Sync();
}

FileType S3RedirectFileHandle::GetType() {
	return GetS3Handle().GetType();
}

timestamp_t S3RedirectFileHandle::GetLastModifiedTime() {
	return last_modified_time;
}

void S3RedirectFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	GetS3Handle().Write(buffer, nr_bytes);
}

int64_t S3RedirectFileHandle::Write(void *buffer, idx_t nr_bytes) {
	return GetS3Handle().Write(buffer, nr_bytes);
}

void S3RedirectFileHandle::Truncate(int64_t new_size) {
	return GetS3Handle().Truncate(new_size);
}

unique_ptr<FileHandle> S3RedirectProtocolFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                              optional_ptr<FileOpener> opener) {
	try {
		auto s3_info = ConvertLocalPathToS3(path);
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

void S3RedirectProtocolFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (s3_handle) {
		return s3_handle->GetS3Handle().Read(buffer, nr_bytes, location);
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

int64_t S3RedirectProtocolFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (s3_handle) {
		return s3_handle->GetS3Handle().Read(buffer, nr_bytes);
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

void S3RedirectProtocolFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (s3_handle) {
		return s3_handle->GetS3Handle().Seek(location);
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

int64_t S3RedirectProtocolFileSystem::GetFileSize(FileHandle &handle) {
	auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (s3_handle) {
		return s3_handle->GetFileSize();
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

FileType S3RedirectProtocolFileSystem::GetFileType(FileHandle &handle) {
	return FileType::FILE_TYPE_REGULAR;
}

void S3RedirectProtocolFileSystem::FileSync(FileHandle &handle) {
	if (auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle)) {
		s3_handle->Sync();
	}
}

timestamp_t S3RedirectProtocolFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
	if (s3_handle) {
		return s3_handle->GetLastModifiedTime();
	}
	throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
}

vector<OpenFileInfo> S3RedirectProtocolFileSystem::Glob(const string &path, FileOpener *opener = nullptr) {
	return LocalFileSystem().Glob(path, nullptr);
}

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
