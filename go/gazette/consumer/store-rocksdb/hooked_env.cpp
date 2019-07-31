#include <memory>
#include <iostream>
#include <rocksdb/env.h>
#include <rocksdb/options.h>

extern "C" {
#include "_cgo_export.h"
}

using rocksdb::Env;
using rocksdb::EnvOptions;
using rocksdb::EnvWrapper;
using rocksdb::MemoryMappedFileBuffer;
using rocksdb::Options;
using rocksdb::RandomRWFile;
using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::WritableFile;
using rocksdb::WritableFileWrapper;
using std::unique_ptr;


// Implementation of rocksdb::WritableFile which notifies via cgo of calls to hooked methods.
class HookedWritableFile : public WritableFileWrapper {
 public:

  HookedWritableFile(int handle, std::unique_ptr<WritableFile> delegate)
  : WritableFileWrapper(delegate.get()),
    handle_(handle),
    delegate_(std::move(delegate)) {}

  virtual ~HookedWritableFile() override { observe_wf_dtor(handle_); }

  virtual Status Append(const Slice& data) override {
    Status status = WritableFileWrapper::Append(data);
    if (status.ok()) {
      observe_wf_append(handle_, (char*)data.data(), data.size());
    }
    return status;
  }
  virtual Status PositionedAppend(const Slice& /* data */, uint64_t /* offset */) override {
    return Status::NotSupported("PositionedAppend is not supported by Gazette's RocksDB integration (but could be?)");
  }
  virtual Status Truncate(uint64_t /*size*/) override {
    return Status::NotSupported("Truncate is not supported by Gazette's RocksDB integration");
  }
  virtual Status Close() override {
    Status status = WritableFileWrapper::Close();
    if (status.ok()) {
      observe_wf_close(handle_);
    }
    return status;
  }
  virtual Status Sync() override {
    Status status = WritableFileWrapper::Sync();
    if (status.ok()) {
      observe_wf_sync(handle_);
    }
    return status;
  }
  virtual Status Fsync() override {
    Status status = WritableFileWrapper::Fsync();
    if (status.ok()) {
      observe_wf_fsync(handle_);
    }
    return status;
  }
  virtual Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    Status status = WritableFileWrapper::RangeSync(offset, nbytes);
    if (status.ok()) {
      observe_wf_range_sync(handle_, offset, nbytes);
    }
    return status;
  }

 private:

  int handle_;

  // Delegate is exclusively owned by HookedWritableFile.
  std::unique_ptr<WritableFile> delegate_;
};


// Implementation of rocksdb::Env which notifies via cgo of calls to hooked methods.
class HookedEnv : public EnvWrapper {
 public:

  HookedEnv(int handle) : EnvWrapper(Env::Default()), handle_(handle) {}

  virtual ~HookedEnv() override { observe_env_dtor(handle_); }

  virtual Status NewWritableFile(
    const std::string& fname,
    std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) override {

    Status status = EnvWrapper::NewWritableFile(fname, result, options); // Delegate.
    if (status.ok()) {
      // Wrap |result| *WritableFile with a hooked implementation.
      int wf_handle = observe_new_writable_file(handle_, (char*)(fname.c_str()), fname.length());
      result->reset(new HookedWritableFile(wf_handle, std::move(*result)));
    }
    return status;
  }
  virtual Status ReopenWritableFile(
    const std::string& fname,
    std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) override {
      return Status::NotSupported("ReopenWritableFile is not supported by Gazette's RocksDB integration");
  }
  virtual Status ReuseWritableFile(
    const std::string& fname,
    const std::string& old_fname,
    std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) override {
      return Status::NotSupported("RandomRWFile is not supported by Gazette's RocksDB integration (but could be?)");
  }
  virtual Status NewRandomRWFile(
    const std::string& fname,
    std::unique_ptr<RandomRWFile>* result,
    const EnvOptions& options) override {
      return Status::NotSupported("RandomRWFile is not supported by Gazette's RocksDB integration (but could be?)");
  }
  virtual Status NewMemoryMappedFileBuffer(
    const std::string& fname,
    std::unique_ptr<MemoryMappedFileBuffer>* result) override {
      return Status::NotSupported("MemoryMappedFileBuffer cannot be supported by Gazette's RocksDB integration");
  }
  virtual Status DeleteFile(const std::string& fname) override {
    Status status = EnvWrapper::DeleteFile(fname);
    if (status.ok()) {
      observe_delete_file(handle_, (char*)(fname.c_str()), fname.length());
    }
    return status;
  }
  virtual Status DeleteDir(const std::string& dirname) override {
    return Status::NotSupported("DeleteDir not supported by Gazette's RocksDB integration");
  }
  virtual Status RenameFile(const std::string& src, const std::string& target) override {
    Status result = EnvWrapper::RenameFile(src, target);
    if (result.ok()) {
      observe_rename_file(handle_,
        (char*)(src.c_str()),
        src.length(),
        (char*)(target.c_str()),
        target.length());
    }
    return result;
  }
  virtual Status LinkFile(const std::string& src, const std::string& target) override {
    Status result = EnvWrapper::LinkFile(src, target);
    if (result.ok()) {
      observe_link_file(handle_,
        (char*)(src.c_str()),
        src.length(),
        (char*)(target.c_str()),
        target.length());
    }
    return result;
  }

 private:

  int handle_;
};


extern "C" {

// Note: this definition is copied from github.com/facebook/rocksdb/db/c.cc
// We must copy, as this struct is defined in a .cc we don't have access too.
struct rocksdb_env_t {
  rocksdb::Env* rep;
  bool is_default;
};

rocksdb_env_t* new_hooked_env(int handle) {
  rocksdb_env_t* result = new rocksdb_env_t;
  result->rep = new HookedEnv(handle);
  result->is_default = false;
  return result;
}

} // extern "C"
