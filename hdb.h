#pragma once

#include "hashdb.h"
#include "hashdb_internal.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include <barrier>
#include <vector>

class HDB : public HashDB {
 public:
  std::vector<Slice *> slice;
  std::mutex mutex;
  std::thread sync_thread;
  std::condition_variable sync_condition;
  int exiting{0};
  int read_only{0};

  int foreach_slice(void *(*pfn)(Doer *));
  void free_slices();

  uint32_t thread_pool_allocated : 1 = 0;
  int thread_pool_max_threads{std::numeric_limits<int>::max()};
  int current_write_slice{0};

  // FUTURE Config variables
  int init_generations{1};
  bool separate_db_per_slice{true};
  int replication_factor{0};

  int warn(cchar *format, ...);
  int err(cchar *format, ...);
  void crash();
  HDB();
};

void fail(const char *s, ...);
