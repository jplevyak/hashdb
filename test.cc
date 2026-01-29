#include "hashdb.h"
#include "hdb.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <cassert>
#include <random>
#include <unistd.h>  // for unlink
#include <atomic>

// Helper for assertions
void check(bool condition, const std::string &message) {
  if (!condition) {
    std::cerr << "FAIL: " << message << std::endl;
    exit(1);
  } else {
    std::cout << "PASS: " << message << std::endl;
  }
}

// Generate random string
std::string random_string(size_t length) {
  static const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string str;  // use std::string for RAII
  str.reserve(length);
  static std::mt19937 rng(std::random_device{}());
  static std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);

  for (size_t i = 0; i < length; ++i) {
    str += charset[dist(rng)];
  }
  return str;
}

void test_basic_ops() {
  std::cout << "\n--- TEST: Basic Operations ---" << std::endl;
  HashDB *db = new_HashDB();
  db->slice(".", 100000000);  // 100MB
  db->open();

  uint64_t key = 0x1111111111111111ul;
  std::string data = "Hello, HashDB!";

  // Write
  int res = db->write(key, (void *)data.c_str(), data.length() + 1, HashDB::SyncMode::Sync);
  check(res == 0, "Basic Write");

  // Read
  std::vector<HashDB::Extent> hits;
  res = db->read(key, hits);
  check(res == 0 && hits.size() > 0, "Basic Read Found");

  // Verify content
  bool found = false;
  for (const auto &hit : hits) {
    if (hit.data && strcmp((char *)hit.data, data.c_str()) == 0) {
      found = true;
      break;
    }
  }
  check(found, "Basic Read Content Match");

  // Cleanup read hits
  for (auto &hit : hits) db->free_chunk(hit.data);

  db->close();
  delete db;
}

void test_large_value() {
  std::cout << "\n--- TEST: Large Value ---" << std::endl;
  HashDB *db = new_HashDB();
  db->slice(".", 100000000);
  db->open();

  uint64_t key = 0x2222222222222222ul;
  size_t size = 100 * 1024;  // 100KB
  std::string data = random_string(size);

  int res = db->write(key, (void *)data.c_str(), size, HashDB::SyncMode::Sync);
  check(res == 0, "Large Value Write");

  std::vector<HashDB::Extent> hits;
  res = db->read(key, hits);
  check(res == 0, "Large Value Read");

  bool verified = false;
  for (const auto &hit : hits) {
    if (hit.len == size && memcmp(hit.data, data.c_str(), size) == 0) {
      verified = true;
      break;
    }
  }
  check(verified, "Large Value Verification");

  for (auto &hit : hits) db->free_chunk(hit.data);
  db->close();
  delete db;
}

void test_many_keys() {
  std::cout << "\n--- TEST: Many Keys ---" << std::endl;
  HashDB *db = new_HashDB();
  db->slice(".", 100000000);
  db->open();

  const int N = 1000;
  std::vector<uint64_t> keys;
  std::vector<std::string> values;

  std::cout << "Writing " << N << " keys..." << std::endl;
  for (int i = 0; i < N; ++i) {
    keys.push_back(0x3000000000000000ul + i);
    values.push_back("Value_" + std::to_string(i));
    db->write(keys[i], (void *)values[i].c_str(), values[i].length() + 1, HashDB::SyncMode::Async);
  }

  // Sync at end to flush all async writes
  db->write(0xFFFFFFFFFFFFFFFFul, (void *)"sync", 5, HashDB::SyncMode::Sync);

  std::cout << "Verifying " << N << " keys..." << std::endl;
  int found_count = 0;
  for (int i = 0; i < N; ++i) {
    std::vector<HashDB::Extent> hits;
    if (db->read(keys[i], hits) == 0) {
      for (const auto &hit : hits) {
        if (hit.data && strcmp((char *)hit.data, values[i].c_str()) == 0) {
          found_count++;
          break;
        }
      }
    }
    for (auto &hit : hits) db->free_chunk(hit.data);
  }
  check(found_count == N, "All " + std::to_string(N) + " keys found");

  db->close();
  delete db;
}

void test_persistence() {
  std::cout << "\n--- TEST: Persistence ---" << std::endl;
  uint64_t key = 0x4444444444444444ul;
  std::string val = "Persistent Data";

  // 1. Write and Close
  {
    HashDB *db = new_HashDB();
    db->slice(".", 100000000);
    db->open();
    db->write(key, (void *)val.c_str(), val.length() + 1, HashDB::SyncMode::Sync);
    db->close();
    delete db;
  }

  // 2. Reopen and Read
  {
    HashDB *db = new_HashDB();
    db->slice(".", 100000000);
    db->open();
    std::vector<HashDB::Extent> hits;
    db->read(key, hits);

    bool found = false;
    for (const auto &hit : hits) {
      if (hit.data && strcmp((char *)hit.data, val.c_str()) == 0) {
        found = true;
        break;
      }
    }
    check(found, "Persistence: Data survived close/open");
    for (auto &hit : hits) db->free_chunk(hit.data);
    db->close();
    delete db;
  }
}

void test_async_callbacks() {
  std::cout << "\n--- TEST: Async Callbacks ---" << std::endl;
  HashDB *db = new_HashDB();
  db->slice(".", 100000000);
  db->open();

  uint64_t key = 0x5555555555555555ul;
  std::string val = "Async Data";

  // Test Async Write Callback
  std::atomic<bool> write_done(false);
  int write_res = -999;

  db->write(
      key, (void *)val.c_str(), val.length() + 1,
      [&](int res) {
        write_res = res;
        write_done = true;
      },
      HashDB::SyncMode::Async);

  // Wait for callback
  int tries = 0;
  while (!write_done && tries++ < 100) {
    usleep(1000);  // 1ms
  }

  // Force a sync if it hasn't happened
  db->write(0xFFFFFFFFFFFFFFFFul, (void *)"sync", 5, HashDB::SyncMode::Sync);

  // Re-check after sync
  while (!write_done && tries++ < 200) {
    usleep(1000);
  }

  check(write_done, "Async Write Callback Executed");
  check(write_res == 0, "Async Write Success");

  // Test Read Callback
  std::atomic<bool> read_done(false);
  std::vector<HashDB::Extent> result_hits;
  int read_res = -999;

  db->read(key, [&](int res, std::vector<HashDB::Extent> hits) {
    read_res = res;
    result_hits = hits;
    read_done = true;
  });

  tries = 0;
  while (!read_done && tries++ < 100) {
    usleep(1000);
  }

  check(read_done, "Async Read Callback Executed");
  check(read_res == 0, "Async Read Success");

  bool content_match = false;
  for (auto &h : result_hits) {
    if (h.data && strcmp((char *)h.data, val.c_str()) == 0) content_match = true;
    db->free_chunk(h.data);
  }
  check(content_match, "Async Read Content Match");

  db->close();
  delete db;
}

void test_remove() {
  std::cout << "\n--- TEST: Remove ---" << std::endl;
  HashDB *db = new_HashDB();
  db->slice(".", 100000000);
  db->open();

  // 1. Sync Remove
  uint64_t key1 = 0x6666666666666666ul;
  std::string val1 = "Remove Sync";
  db->write(key1, (void *)val1.c_str(), val1.length() + 1, HashDB::SyncMode::Sync);
  std::vector<HashDB::Extent> hits1;
  check(db->read(key1, hits1) == 0 && !hits1.empty(), "Pre-remove read success");

  // Get pointer to old data
  void *ptr1 = hits1[0].data;
  // NOTE: In a real usage we might want to ensure we hold the pointer or data is pinned?
  // remove takes (void *old_data) which implies we have the data pointer from a read.
  
  int res = db->remove(ptr1, HashDB::SyncMode::Sync);
  check(res == 0, "Sync Remove Success");
  
  hits1.clear();
  check(db->read(key1, hits1) != 0 || hits1.empty(), "Post-remove read fails");

  // 2. Async Remove
  uint64_t key2 = 0x7777777777777777ul;
  std::string val2 = "Remove Async";
  db->write(key2, (void *)val2.c_str(), val2.length() + 1, HashDB::SyncMode::Sync);
  
  std::vector<HashDB::Extent> hits2;
  check(db->read(key2, hits2) == 0 && !hits2.empty(), "Pre-remove async read success");
  void *ptr2 = hits2[0].data;

  std::atomic<bool> remove_done(false);
  int remove_res = -999;
  
  db->remove(ptr2, 
    [&](int res) {
        remove_res = res;
        remove_done = true;
    },
    HashDB::SyncMode::Async
  );

  int tries = 0;
  while (!remove_done && tries++ < 100) usleep(1000);
  db->write(0xFFFFFFFFFFFFFFFFul, (void *)"sync", 5, HashDB::SyncMode::Sync);
  while (!remove_done && tries++ < 200) usleep(1000);
  
  check(remove_done, "Async Remove Callback Executed");
  check(remove_res == 0, "Async Remove Success");
  
  hits2.clear();
  check(db->read(key2, hits2) != 0 || hits2.empty(), "Post-remove async read fails");

  db->close();
  delete db;
}

void test_recovery() {
  std::cout << "\n--- TEST: Recovery ---" << std::endl;
  uint64_t key1 = 0x8888888888888888ul;
  std::string val1 = "Recovered Sync Data";
  uint64_t key2 = 0x9999999999999999ul;
  std::string val2 = "Lost Async Data";

  // 1. Write and Crash
  {
    HashDB *db = new_HashDB();
    db->slice(".", 100000000);
    db->open();
    
    // Write key1 (Sync) - should persist
    db->write(key1, (void *)val1.c_str(), val1.length() + 1, HashDB::SyncMode::Sync);
    
    // Write key2 (Async) - might be lost if we crash immediately
    // Note: Async writes can happen very fast, so we might need to flood or just hope.
    // However, the test requirement is "recovery from non-clean shutdown", 
    // effectively asserting that the DB is not corrupted and synced data is safe.
    db->write(key2, (void *)val2.c_str(), val2.length() + 1, HashDB::SyncMode::Async);

    // Simulate CRASH
    // Cast to HDB* to access crash()
    ((HDB *)db)->crash();
    
    // Leak db memory to simulate process death (or just because crash() left it in bad state for delete)
    // Actually, we can just delete it if we accept that it won't be clean.
    // But HDB::crash() closed FDs, so ~HashDB might struggle if it tries to close/write.
    // Current codebase has no virtual dtor, so `delete db` is just memory free of HashDB part (not HDB).
    // We'll leave it leaked or just manually free what we can? 
    // For test cleanliness, we might want to fix the virtual dtor issue later, but for now just leak to be safe/crash-like.
  }

  // 2. Reopen and Verify
  {
    HashDB *db = new_HashDB();
    db->slice(".", 100000000);
    int res = db->open();
    check(res == 0, "Reopen after crash success");

    // Verify key1 (Sync) is present
    std::vector<HashDB::Extent> hits1;
    db->read(key1, hits1);
    bool found1 = false;
    for (auto &h : hits1) {
       if (h.data && strcmp((char*)h.data, val1.c_str()) == 0) found1 = true;
       db->free_chunk(h.data);
    }
    check(found1, "Sync data recovered");

    // key2 (Async) outcome is undefined but DB should be consistent.
    // We mainly check that we can read key1 and DB is open.

    db->close();
    delete db;
  }
}

int main() {
  // Clean start
  unlink("hashdb.data");

  test_basic_ops();
  test_large_value();
  test_many_keys();
  test_persistence();
  test_persistence();
  test_async_callbacks();
  test_remove();
  test_recovery();

  std::cout << "\nSIU: ALL TESTS PASSED" << std::endl;
  return 0;
}
