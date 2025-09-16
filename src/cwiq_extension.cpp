#define DUCKDB_EXTENSION_MAIN

#include "cwiq_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <sys/xattr.h>
#include <sys/stat.h>
#include <errno.h>
#include <cstring>

namespace duckdb {

struct S3RedirectInfo {
	string s3_url;
	idx_t content_length {0};
	time_t last_modified_time;
};

S3RedirectInfo ConvertLocalPathToS3(const string &local_path);

class S3RedirectFileHandle : public FileHandle {
private:
	string s3_url;
	idx_t known_content_length;
	time_t last_modified_time;
	unique_ptr<FileHandle> s3_handle;
	optional_ptr<FileOpener> file_opener;
	DatabaseInstance &db_instance;

public:
	S3RedirectFileHandle(FileSystem &fs, DatabaseInstance &db, const string &s3_url, idx_t content_length,
	                     time_t last_modified, optional_ptr<FileOpener> opener)
	    : FileHandle(fs, s3_url, FileFlags::FILE_FLAGS_READ), s3_url(s3_url), known_content_length(content_length),
	      last_modified_time(last_modified), file_opener(opener), db_instance(db) {
	}

	virtual ~S3RedirectFileHandle() {
	}

	// Lazy-load the actual S3 handle when needed
	FileHandle &GetS3Handle() {
		if (!s3_handle) {
			// Let DuckDB's httpfs handle the S3 URL
			auto &main_fs = FileSystem::GetFileSystem(db_instance);
			s3_handle = main_fs.OpenFile(s3_url, FileFlags::FILE_FLAGS_READ, nullptr);
		}
		return *s3_handle;
	}

	idx_t GetFileSize() {
		return known_content_length;
	}

	void Close() override {
		if (s3_handle) {
			s3_handle->Close();
		}
	}

	void Read(void *buffer, idx_t nr_bytes, idx_t location) {
		return GetS3Handle().Read(buffer, nr_bytes, location);
	}

	int64_t Read(void *buffer, idx_t nr_bytes) {
		return GetS3Handle().Read(buffer, nr_bytes);
	}

	bool CanSeek() {
		return true;
	}

	void Sync() {
		if (s3_handle) {
			s3_handle->Sync();
		}
	}

	FileType GetType() {
		return GetS3Handle().GetType();
	}

	time_t GetLastModifiedTime() {
		return last_modified_time;
	}

	// Write operations (probably not needed for your use case)
	void Write(void *buffer, idx_t nr_bytes, idx_t location) {
		return GetS3Handle().Write(buffer, nr_bytes, location);
	}

	int64_t Write(void *buffer, idx_t nr_bytes) {
		return GetS3Handle().Write(buffer, nr_bytes);
	}

	void Truncate(int64_t new_size) {
		return GetS3Handle().Truncate(new_size);
	}
};

// Custom FileSystem that only handles our special protocol
class S3RedirectProtocolFileSystem : public FileSystem {
private:
	DatabaseInstance &db_instance;

public:
	S3RedirectProtocolFileSystem(DatabaseInstance &db) : db_instance(db) {
	}

	DUCKDB_API unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                           optional_ptr<FileOpener> opener = nullptr) override {
		try {
			// Convert local path to S3 info using your service
			auto s3_info = ConvertLocalPathToS3(path);

			// Return S3 redirect handle with metadata
			return make_uniq<S3RedirectFileHandle>(*this, db_instance, s3_info.s3_url, s3_info.content_length,
			                                       s3_info.last_modified_time, opener);

		} catch (const std::exception &e) {
			throw IOException("Failed to redirect to S3: " + string(e.what()));
		}
	}

	std::string GetName() const override {
		return "s3redirect";
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		try {
			ConvertLocalPathToS3(filename);
			return true;
		} catch (...) {
			return false;
		}
	}

	bool CanHandleFile(const string &fpath) override {
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

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
		if (s3_handle) {
			return s3_handle->GetS3Handle().Read(buffer, nr_bytes, location);
		}
		throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
		if (s3_handle) {
			return s3_handle->GetS3Handle().Read(buffer, nr_bytes);
		}
		throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
	}

	// Other methods can throw "not supported" errors since they're not relevant
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		throw NotImplementedException("Write not supported for s3redirect:// protocol");
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		throw NotImplementedException("Write not supported for s3redirect:// protocol");
	}

	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplementedException("CreateDirectory not supported for s3redirect:// protocol");
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return false;
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplementedException("RemoveDirectory not supported for s3redirect:// protocol");
	}

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		throw NotImplementedException("ListFiles not supported for s3redirect:// protocol");
	}

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplementedException("MoveFile not supported for s3redirect:// protocol");
	}

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplementedException("RemoveFile not supported for s3redirect:// protocol");
	}

	bool CanSeek() override {
		return true;
	}

	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

	void Seek(FileHandle &handle, idx_t location) override {
		auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
		if (s3_handle) {
			return s3_handle->GetS3Handle().Seek(location);
		}
		throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
	}

	int64_t GetFileSize(FileHandle &handle) override {
		auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
		if (s3_handle) {
			return s3_handle->GetFileSize();
		}
		throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
	}

	FileType GetFileType(FileHandle &handle) override {
		return FileType::FILE_TYPE_REGULAR;
	}

	void FileSync(FileHandle &handle) override {
		if (auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle)) {
			s3_handle->Sync();
		}
	}

	void Truncate(FileHandle &handle, int64_t new_size) override {
		throw NotImplementedException("Truncate not supported for s3redirect:// protocol");
	}

	time_t GetLastModifiedTime(FileHandle &handle) override {
		auto s3_handle = dynamic_cast<S3RedirectFileHandle *>(&handle);
		if (s3_handle) {
			return s3_handle->GetLastModifiedTime();
		}
		throw InternalException("Invalid handle type in S3RedirectProtocolFileSystem");
	}
};

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

void CwiqExtension::Load(DuckDB &db) {
#ifndef __linux__
	std::cout << "Error: CWIQ extension not implemented for non-Linux platforms.";
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

	std::cout << "CWIQ extension enabled" << std::endl;

	auto s3_redirect_fs = make_uniq<S3RedirectProtocolFileSystem>(*db.instance);

	// Register the filesystem with DuckDB for the s3redirect:// protocol
	db.GetFileSystem().RegisterSubSystem(std::move(s3_redirect_fs));
}
std::string CwiqExtension::Name() {
	return "cwiq";
}

std::string CwiqExtension::Version() const {
#ifdef EXT_VERSION_CWIQ
	return EXT_VERSION_CWIQ;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void cwiq_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::CwiqExtension>();
}

DUCKDB_EXTENSION_API const char *cwiq_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
