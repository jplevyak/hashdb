#pragma once

#include "hashdb.h"
#include "hashdb_internal.h"
#include <pthread.h>

class HDB : public HashDB {
 public:
  Vec<Slice *> slice;
  pthread_mutex_t mutex;
  pthread_t sync_thread;
  pthread_cond_t sync_condition;
  int exiting;
  int read_only;

  int foreach_slice(void *(*pfn)(Doer *));
  void free_slices();

  uint32_t thread_pool_allocated : 1;
  int thread_pool_max_threads;
  int current_write_slice;

  // FUTURE Config variables
  int init_generations;        // 1
  bool separate_db_per_slice;  // true
  int replication_factor;      // 0

  int warn(cchar *format, ...);
  int err(cchar *format, ...);
  HDB();
};

void fail(const char *s, ...);
