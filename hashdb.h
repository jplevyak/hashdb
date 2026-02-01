/*
  Copyright 2003-2019 John Plevyak, All Rights Reserved
*/
#pragma once

#include <string.h>

#include <cstdint>
#include <vector>

#include "threadpool.h"

#include <functional>
#include <span>
#include <memory>

#define HASHDB_SLICE_SIZE_MAX ((uint64_t)-1)

class HashDB {
 public:
  enum class SyncMode { Async, Flush, Sync };

  struct Extent {
    void *data;
    uint32_t len;
    operator bool() { return !data && !len; }
  };

  using WriteCallback = std::function<void(int)>;
  using ReadCallback = std::function<void(int, std::vector<Extent>)>;

  using SerializeFn = std::function<uint64_t(std::span<uint8_t>)>;

  // Specify a contiguous part of the database, may be a raw disk, directory or file.
  // The "size" is the size that will be used or allocated, and -1
  //   for file/dir or raw means use the existing or max size respectively.
  // If "init" is true, the slice will be initialized.
  int slice(const char *pathname, uint64_t size = HASHDB_SLICE_SIZE_MAX, bool init = false);

  int reinitialize();           // Reinitialize all slices (clear data).
  int open(int read_only = 0);  // Open all slices.

  // Read the set of chunks associated with the given key.
  // Returned chunks are free'd with free_chunk().
  // Returns 0 on success, 1 on a miss, -1 on an error.
  int read(uint64_t key, std::vector<Extent> &hit);
  int read(uint64_t key, ReadCallback callback, bool immediate_miss = false);

  // Write a continguous block of data and associate with the given key(s).
  // When a callback is provided, "data" and "keys" must be valid
  //   till the callback is called.
  // Returns 0 on success, -1 on an error.
  int write(uint64_t *key, int nkeys, uint64_t value_len, SerializeFn serializer, WriteCallback callback = nullptr,
            SyncMode mode = SyncMode::Async);
  int write(uint64_t *key, int nkeys, const void *data, int len, WriteCallback callback = nullptr,
            SyncMode mode = SyncMode::Async);
  int write(uint64_t key, const void *data, int len, WriteCallback callback = nullptr, SyncMode mode = SyncMode::Async);

  // Convenience for just SyncMode without callback
  int write(uint64_t *key, int nkeys, uint64_t value_len, SerializeFn serializer, SyncMode mode);
  int write(uint64_t *key, int nkeys, const void *data, int len, SyncMode mode);
  int write(uint64_t key, const void *data, int len, SyncMode mode);

  // Remove mapping for all keys of old_data, a result of HashDB::read
  int remove(void *old_data, WriteCallback callback = nullptr, SyncMode mode = SyncMode::Async);
  int remove(void *old_data, SyncMode mode);

  // Get chained collision match
  int next(uint64_t key, void *old_data, std::vector<Extent> &hit);
  // int next(uint64_t key, void *old_data, Callback *callback, bool immediate_miss = false); // Deprecated/Unused?

  int get_keys(void *old_data, std::vector<uint64_t> &keys);
  int close();             // Close all slices.
  int free_chunk(void *);  // Free a chunk returned by read().

  // Config variables           defaults
  // Config variables           defaults
  int init_data_per_index_ = 16384;
  bool reinit_on_open_error_ = false;
  uint64_t write_buffer_size_ = 1024 * 1024;  // 1MB
  int concurrency_ = 100;                     // * of slices
  int sync_wait_msec_ = 50;
  bool chain_collisions_ = false;  // for write-only only
  std::unique_ptr<ThreadPool> thread_pool_;

  int verify();      // Verify integrity of the database metadata (very expensive).
  int dump_debug();  // dump debug info (DEVELOPER)
  virtual ~HashDB() = default;
  static std::unique_ptr<HashDB> create();
 protected:
  HashDB();
};

/*
  individual writes must be < 35,184,372,088,832 or 32TB
*/

/*
  Usage Example

  HashDB *db = new_HashDB();
  db->slice(".", 1000000000);  // "hashdb.data" in directory "." of 100MB
  db->slice("tmp/mem.db");     // "mem.db" in "tmp" of current size of mem.db
  db->open();
  char buf[128] = "test data";
  db->write(0x1234123412341234ul, buf, 128);
  std::vector<void*> hits;
  db->read(0x1234123412341234ul, hits);
  for (size_t i = 0; i < hits.size(); i++)
    free_chunk(hits[i]);
  db->close();
  delete db;
*/
