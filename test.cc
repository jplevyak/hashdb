#include "hashdb.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <cassert>
#include <random>
#include <unistd.h>  // for unlink

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

int main() {
  // Clean start
  unlink("hashdb.data");

  test_basic_ops();
  test_large_value();
  test_many_keys();
  test_persistence();

  std::cout << "\nSIU: ALL TESTS PASSED" << std::endl;
  return 0;
}
