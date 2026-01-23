#include "hashdb.h"

int main() {
  HashDB *db = new_HashDB();
  db->slice(".", 1000000000);  // "hashdb.data" in directory "." of 100MB
  db->open();
  char buf[128] = "test data";
  db->write(0x1234123412341234ul, buf, 128);
  std::vector<HashDB::Extent> hits;
  db->read(0x1234123412341234ul, hits);
  for (int i = 0; i < hits.size(); i++)
    db->free_chunk(hits[i].data);
  db->close();
  delete db;
  return 0;
}
