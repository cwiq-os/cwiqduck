#pragma once

#include "duckdb.hpp"
namespace duckdb {

class CwiqExtension : public Extension {
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
	unique_ptr<FileHandle> s3_handle;
	optional_ptr<FileOpener> file_opener;
	DatabaseInstance &db_instance;

public:
	S3RedirectFileHandle(FileSystem &fs, DatabaseInstance &db, const string &s3_url, idx_t content_length,
	                     timestamp_t last_modified, optional_ptr<FileOpener> opener)
	    : FileHandle(fs, s3_url, FileFlags::FILE_FLAGS_READ), s3_url(s3_url), known_content_length(content_length),
	      last_modified_time(last_modified), file_opener(opener), db_instance(db) {
	}
	virtual ~S3RedirectFileHandle() {};

	void Close() override;

	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	int64_t Read(void *buffer, idx_t nr_bytes);
	FileHandle &GetS3Handle();
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
	int64_t GetFileSize(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;

	timestamp_t GetLastModifiedTime(FileHandle &handle) override;

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
