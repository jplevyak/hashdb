#include <cassert>
#include <cstdio>
#include <vector>

#include "hashdb.h"

void test_hashdb_api() {
  // Test case 1: Basic initialization and shutdown
  printf("Running test case 1: Basic initialization and shutdown...\\n");
  auto *db = new_HashDB();
  assert(db != nullptr && "Failed to create HashDB instance");
  db->close();
  printf("Test case 1 passed.\\n");

  // Test case 2: Slicing and opening the database
  printf("Running test case 2: Slicing and opening the database...\\n");
  db = new_HashDB();
  assert(db->slice(".", 10000000, true) == 0 && "Failed to slice database");
  assert(db->open() == 0 && "Failed to open database");
  db->close();
  printf("Test case 2 passed.\\n");

  // Test case 3: Writing and reading a simple value
  printf("Running test case 3: Writing and reading a simple value...\\n");
  db = new_HashDB();
  db->slice(".", 10000000, true);
  db->open();
  const uint64_t key = 0x123456789ABCDEF0;
  char write_buf[] = "test_data";
  assert(db->write(key, write_buf, sizeof(write_buf)) == 0 &&
         "Failed to write data");
  std::vector<HashDB::Extent> hits;
  assert(db->read(key, hits) == 0 && "Failed to read data");
  assert(!hits.empty() && "No data found for the key");
  db->close();
  printf("Test case 3 passed.\\n");

  // Test case 4: Testing persistence
  printf("Running test case 4: Testing persistence...\\n");
  db = new_HashDB();
  db->slice(".", 10000000, false);
  db->open();
  hits.clear();
  assert(db->read(key, hits) == 0 && "Failed to read data after reopening");
  assert(!hits.empty() && "Data was not persisted");
  db->close();
  printf("Test case 4 passed.\\n");
}

int main() {
  test_hashdb_api();
  return 0;
}
