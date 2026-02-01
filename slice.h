#pragma once

#include "hashdb_internal.h"
#include "hdb.h"
#include <memory>

class Slice {
 public:
  HDB *hdb_;
  int islice_;
  int fd_;
  char *pathname_;
  char *layout_pathname_;
  uint64_t layout_size_;
  uint64_t size_;
  uint32_t block_size_;
  uint32_t is_raw_ : 1;
  uint32_t is_file_ : 1;
  uint32_t is_dir_ : 1;

  std::vector<std::unique_ptr<Gen>> gen_;

  int init();
  int open();
  int might_exist(uint64_t key);
  int read(uint64_t key, std::vector<HashDB::Extent> &hit);
  int write(uint64_t *key, int nkeys, uint64_t value_len, HashDB::SerializeFn serializer, HashDB::SyncMode mode);
  int verify();
  int close();

  Slice(HDB *hdb, int islice, const char *pathname, uint64_t layout_size = -1);
};
