#pragma once

#include "duckdb.hpp"

#include <mutex>
#include <vector>

#undef MoveFile
#undef RemoveDirectory
#undef CreateDirectory

namespace duckdb {

class CwiqduckExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

struct S3RedirectInfo {
	string s3_url;
	idx_t content_length {0};
	timestamp_t last_modified_time;
};

S3RedirectInfo ConvertLocalPathToS3(const string &local_path);

class S3RedirectFileHandle : public FileHandle {
private:
	string s3_url;
	idx_t known_content_length;
	timestamp_t last_modified_time;
	DatabaseInstance &db_instance;
	// Stored for v1.4.x where pool handles must be opened with an explicit opener.
	// In v1.5.3+ (OpenerFileSystem) this is unused; the db-level opener is injected automatically.
	optional_ptr<FileOpener> file_opener;

	// Primary handle: sequential cursor (Seek / SeekPosition / Read(buf,n)).
	// Never shared between threads.
	unique_ptr<FileHandle> primary_handle;

	// Pool of idle handles for concurrent positional reads.
	// Each borrow gives one thread exclusive ownership of a handle; the lock is
	// held only for the O(1) push/pop, never across any network I/O.
	std::mutex pool_mu;
	std::vector<unique_ptr<FileHandle>> idle_pool;

	// Opens a fresh underlying httpfs handle with FILE_FLAGS_DIRECT_IO so every
	// positional read becomes an independent range GET with no shared rolling buffer.
	unique_ptr<FileHandle> OpenHandle() const;

	// RAII borrow: returns the handle to idle_pool on destruction.
	struct BorrowedHandle {
		S3RedirectFileHandle *owner; // pointer (not ref) so the struct is movable
		unique_ptr<FileHandle> handle;

		BorrowedHandle() : owner(nullptr) {
		}
		BorrowedHandle(S3RedirectFileHandle &o, unique_ptr<FileHandle> h) : owner(&o), handle(std::move(h)) {
		}
		BorrowedHandle(BorrowedHandle &&o) noexcept : owner(o.owner), handle(std::move(o.handle)) {
			o.owner = nullptr;
		}
		BorrowedHandle(const BorrowedHandle &) = delete;
		BorrowedHandle &operator=(const BorrowedHandle &) = delete;
		~BorrowedHandle() {
			if (owner && handle) {
				owner->ReturnHandle(std::move(handle));
			}
		}
		FileHandle &get() {
			return *handle;
		}
	};

	BorrowedHandle BorrowHandle();
	void ReturnHandle(unique_ptr<FileHandle> h);

public:
	S3RedirectFileHandle(FileSystem &fs, DatabaseInstance &db, const string &s3_url, idx_t content_length,
	                     timestamp_t last_modified, optional_ptr<FileOpener> opener);
	virtual ~S3RedirectFileHandle() {};

	void Close() override;

	// Positional read: borrows an exclusive handle from the pool.
	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	// Sequential read: uses the primary handle.
	int64_t Read(void *buffer, idx_t nr_bytes);

	FileHandle &GetPrimaryHandle();
	FileType GetType();
	timestamp_t GetLastModifiedTime();
	idx_t GetFileSize();
	bool CanSeek();
	void Sync();
	void Write(void *buffer, idx_t nr_bytes, idx_t location);
	int64_t Write(void *buffer, idx_t nr_bytes);
	void Truncate(int64_t new_size);
};

class S3RedirectProtocolFileSystem : public FileSystem {
private:
	DatabaseInstance &db_instance;

public:
	S3RedirectProtocolFileSystem(DatabaseInstance &db) : db_instance(db) {};

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	std::string GetName() const override {
		return "s3redirect";
	};
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool CanHandleFile(const string &fpath) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool CanSeek() override {
		return true;
	};
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	};
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;
	int64_t GetFileSize(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;

	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	NotImplementedException NotImplemented(const std::string where) const {
		return NotImplementedException(where + "not supported for s3redirect:// protocol");
	};

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		throw NotImplemented(__func__);
	};
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplemented(__func__);
	};
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplemented(__func__);
	};
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplemented(__func__);
	};
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplemented(__func__);
	};
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplemented(__func__);
	};
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		throw NotImplemented(__func__);
	};
	void Truncate(FileHandle &handle, int64_t new_size) override {
		throw NotImplemented(__func__);
	};
};

S3RedirectInfo ConvertLocalPathToS3(const string &local_path);

} // namespace duckdb
