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
  std::vector<std::unique_ptr<Slice>> slice_;
  std::mutex mutex_;
  std::thread sync_thread_;
  std::condition_variable sync_condition_;
  int exiting_{0};
  int read_only_{0};

  int foreach_slice(void *(*pfn)(Doer *));
  void free_slices();

  uint32_t thread_pool_allocated_ : 1 = 0;
  int thread_pool_max_threads_{std::numeric_limits<int>::max()};
  int current_write_slice_{0};

  // FUTURE Config variables
  int init_generations_{1};
  bool separate_db_per_slice_{true};
  int replication_factor_{0};

  int warn(cchar *format, ...);
  int err(cchar *format, ...);
  void crash();
  HDB();
};

void fail(const char *s, ...);
