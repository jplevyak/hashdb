#pragma once

#include "hashdb_internal.h"
#include "hdb.h"

class Slice {
 public:
  HDB *hdb;
  int islice;
  int fd;
  char *pathname;
  char *layout_pathname;
  uint64_t layout_size;
  uint64_t size;
  uint32_t block_size;
  uint32_t is_raw : 1;
  uint32_t is_file : 1;
  uint32_t is_dir : 1;

  Vec<Gen *> gen;

  int init();
  int open();
  int might_exist(uint64_t key);
  int read(uint64_t key, Vec<HashDB::Extent> &hit);
  int write(uint64_t *key, int nkeys, HashDB::Marshal *marshal, HashDB::Callback *callback);
  int verify();
  int close();

  Slice(HDB *hdb, int islice, const char *pathname, uint64_t layout_size = -1);
};
#define forv_Slice(_x, _v) forv_Vec(Slice, _x, _v)
