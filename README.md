# hashdb

A high-performance, persistent key-value store written in C++.

## Features

- **Slicing:** The database can be split across multiple files or raw devices.
- **Asynchronous I/O:** Writes are performed asynchronously for high throughput.
- **Collision Chaining:** Hash collisions are handled using chaining.
- **Data Verification:** The database includes a `verify()` method to check for data integrity.

## Design

`hashdb` is a key-value store designed for high performance and reliability. It uses a combination of in-memory and on-disk data structures to provide fast access to data.

### Slices and Generations

The database is divided into one or more **slices**, which can be a file, a directory, or a raw device. Each slice can have multiple **generations**, allowing for a form of versioning and making it possible to roll back to a previous state.

### Indexing

The keys are hashed and stored in an index. The index uses a combination of buckets and overflow lists to handle collisions. The size of the index is determined by a prime number to ensure a good distribution of keys. The `prime.cc` file contains the implementation of the Miller-Rabin primality test, which is used to find a suitable prime number for the index size.

## Building

To build the project, you will need a C++ compiler and the `make` utility. The project also has a dependency on a garbage collector library (`libgc`).

```bash
make
```

This will create a static library `libhashdb.a`.

## Usage

Here is an example of how to use the `hashdb` library:

```cpp
#include "hashdb.h"
#include <vector>

int main() {
  HashDB *db = new_HashDB();
  db->slice(".", 1000000000);  // "hashdb.data" in directory "." of 100MB
  db->slice("tmp/mem.db");     // "mem.db" in "tmp" of current size of mem.db
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
```
