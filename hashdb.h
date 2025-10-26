/*
  Copyright 2003-2019 John Plevyak, All Rights Reserved
*/
#pragma once

#include <string.h>

#include <cstdint>
#include <vector>

#include "threadpool.h"

#define HASHDB_SLICE_SIZE_MAX ((uint64_t)-1)
#define HASHDB_SYNC ((HashDB::Callback *)(uintptr_t)2)   // wait at most sync_wait_msec
#define HASHDB_FLUSH ((HashDB::Callback *)(uintptr_t)1)  // flush immediately
#define HASHDB_ASYNC ((HashDB::Callback *)(uintptr_t)0)  // do not wait for flush

class HashDB {
 public:
  class Callback;
  static HashDB::Callback *SYNC;
  static HashDB::Callback *FLUSH;
  static HashDB::Callback *ASYNC;

  struct Extent {
    void *data;
    uint32_t len;
    operator bool() { return !data && !len; }
  };

  // callback after write equivalent to HASHDB_SYNC
  class Callback {
   public:
    typedef void (*cb_fn_t)(int);
    cb_fn_t callbackfn;
    bool flush;
    std::vector<Extent> hit;
    virtual void done(int result) { callbackfn(result); }
    Callback(cb_fn_t fn = 0, bool aflush = false) : callbackfn(fn), flush(aflush) {}
  };

  class Marshal {
   public:
    virtual uint64_t marshal_size() = 0;      // Must always be greater than actual
    virtual uint64_t marshal(char *buf) = 0;  // Returns actual size.
  };

  class Copy : public Marshal {
   public:
    void *p;
    uint64_t len;
    uint64_t marshal_size() { return len; }
    uint64_t marshal(char *buf) {
      memcpy(buf, p, len);
      return len;
    }
    Copy(void *ap, int alen) : p(ap), len(alen) {}
  };

  // Specify a contiguous part of the database, may be a raw disk, directory or file.
  // The "size" is the size that will be used or allocated, and -1
  //   for file/dir or raw means use the existing or max size respectively.
  // If "init" is true, the slice will be initialized.
  int slice(char *pathname, uint64_t size = HASHDB_SLICE_SIZE_MAX, bool init = false);

  int reinitialize();           // Reinitialize all slices (clear data).
  int open(int read_only = 0);  // Open all slices.

  // Read the set of chunks associated with the given key.
  // Returned chunks are free'd with free_chunk().
  // Returns 0 on success, 1 on a miss, -1 on an error.
  int read(uint64_t key, std::vector<Extent> &hit);
  int read(uint64_t key, Callback *callback, bool immediate_miss = false);

  // Write a continguous block of data and associate with the given key(s).
  // When a callback is provided, "data" and "keys" must be valid
  //   till the callback is called.
  // Returns 0 on success, -1 on an error.
  int write(uint64_t *key, int nkeys, Marshal *marshal, Callback *callback = HashDB::ASYNC);
  int write(uint64_t *key, int nkeys, void *data, int len, Callback *callback = HashDB::ASYNC) {
    Copy cp(data, len);
    return write(key, nkeys, &cp, callback);
  }
  int write(uint64_t key, void *data, int len, Callback *callback = HashDB::ASYNC) {
    return write(&key, 1, data, len, callback);
  }

  // Remove mapping for all keys of old_data, a result of HashDB::read
  int remove(void *old_data, Callback *callback = HashDB::ASYNC);

  // Get chained collision match
  int next(uint64_t key, void *old_data, std::vector<Extent> &hit);
  int next(uint64_t key, void *old_data, Callback *callback, bool immediate_miss = false);

  int get_keys(void *old_data, std::vector<uint64_t> &keys);
  int close();             // Close all slices.
  int free_chunk(void *);  // Free a chunk returned by read().

  // Config variables           defaults
  int init_data_per_index;     // 16384
  bool reinit_on_open_error;   // false
  uint64_t write_buffer_size;  // 1MB
  int concurrency;             // 100 (* of slices)
  int sync_wait_msec;          // 50
  bool chain_collisions;       // 0 (for write-only only)
  ThreadPool *thread_pool;     // 0 (create one)

  int verify();      // Verify integrity of the database metadata (very expensive).
  int dump_debug();  // dump debug info (DEVELOPER)
 protected:
  HashDB();
};

HashDB *new_HashDB();

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
