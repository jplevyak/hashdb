#pragma once

#include <cstdint>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <limits>
#include <chrono>

#include "hashdb.h"

// Macros
#define HDB_MAGIC 0x46789FED71329ACDull
#define DATA_MAGIC 0x46789DEF78329ADCull
#define LOG_MAGIC 0xA987FE543210FEEFull
#define BAD_DATA ((void *)(uintptr_t)-1)
#define HDB_MAJOR_VERSION 0
#define HDB_MINOR_VERSION 0
#define SIZE_4K 4096
#define SIZE_8K 8192
#define SAFE_SECTOR_SIZE SIZE_4K  // prepare for 4k native physical sector size
#define SECTOR_SIZE 512
#define ELEMENTS_PER_SECTOR 64  // 512 / 8 bytes = 64
#define BUCKETS_PER_SECTOR 6
#define BUCKET_SIZE 8
#define FREELIST_SIZE 16
#define ELEMENTS_PER_BUCKET 8
#define WRITE_BUFFERS 2      // do not change without modifying write_buffer()
#define DATA_BLOCK_SIZE 512  // change to 4k for 4k native physical sector size
#define FOOTER_SIZE DATA_BLOCK_SIZE
#define STALE_INDEX_RESULT 0           // ok to not delete or evacuate data
#define SLICE_INDEX_MISMATCH_RESULT 0  // ok not match slice index
#define SYNC_PERIOD 5                  // seconds
#define INDEX_BYTES_PER_PART (512 * 1024)
#define LOG_BUFFERS 2
#define LOG_FOOTER_SIZE (sizeof(LogHeaderFooter))

#define ROUND_TO(_x, _n) (((_x) + ((_n) - 1)) & ~((_n) - 1))
#define ROUND_DOWN(_x, _n) ((_x) & ~((_n) - 1))
#define ROUND_DIV(_x, _n) (((_x) + ((_n) - 1)) / (_n))
#define ROUND_DOWN_SAFE_SECTOR_SIZE(_x) ROUND_DOWN(_x, SAFE_SECTOR_SIZE)
#define KEY2TAG(_x) ((_x) >> 48)
#define MODULAR_DIFFERENCE(_x, _y, _m) (((_x) + (_m) - (_y)) % (_m))

#define DELETE(p) \
  do {            \
    delete (p);   \
    (p) = NULL;   \
  } while (0)

#define DEBUG_LOG_SIZE (1 << 27)  // 128 MB
#define DEBUG_LOG_FILENAME "hashdb.debuglog."
#define DEBUG_LOG_PTR_MASK (0x0FFFFFFFFFFFFFFFull)
#define DEBUG_LOG_TAG_SHIFT 60

typedef const char cchar;

enum DEBUG_LOG_TYPES {
  DEBUG_LOG_EMPTY,
  DEBUG_LOG_SET,
  DEBUG_LOG_DELETE,
  DEBUG_LOG_INSERT_LA,
  DEBUG_LOG_DELETE_LA,
  DEBUG_LOG_INS_KEY,
  DEBUG_LOG_DEL_KEY,
  DEBUG_LOG_WRITE
};

// Forward Declarations
class HDB;
class Gen;
class Slice;
struct Doer;
struct Data;
struct LogHeaderFooter;
struct LogEntry;

// Templates and Helper Functions
template <class K, class HF>
class NBlockHash {
 public:
  std::vector<K> v;
  uint32_t n;
  void resize(uint32_t N) {
    n = N;
    v.resize(n * 4);
  }
  void clear() { v.clear(); }
  int count() { return 0; }
  void put(K l) {
    uint32_t h = HF::hash(l);
    K *p = &v[(h % n) * 4];
    for (int i = 0; i < 4; i++) {
      if (!p[i]) {
        p[i] = l;
        return;
      }
    }
  }
  int del(K l) {
    uint32_t h = HF::hash(l);
    K *p = &v[(h % n) * 4];
    for (int i = 0; i < 4; i++) {
      if (HF::equal(p[i], l)) {
        p[i] = K(0);
        return 1;
      }
    }
    return 0;
  }
};

static inline void *new_aligned(size_t s) {
  void *p = nullptr;
  size_t alignment = SAFE_SECTOR_SIZE;
  size_t rounded_s = (s + alignment - 1) & ~(alignment - 1);
  int res = posix_memalign(&p, alignment, rounded_s);
  if (res != 0) return nullptr;
  return p;
}

static inline void delete_aligned(void *p) { free(p); }

static inline uint32_t length_to_size(uint64_t l) {
  uint64_t b = DATA_BLOCK_SIZE;
  uint32_t m = 0;
  while (1) {
    if (l < 256 * b) return (m << 8) + ROUND_DIV(l, b);
    b <<= 4;
    m++;
  }
}

static inline uint64_t size_to_length(uint64_t s) { return (s & 255) * ((1 << ((s >> 8) * 4)) * DATA_BLOCK_SIZE); }

// Structures

struct Header {
  uint64_t magic;
  uint32_t major_version;
  uint32_t minor_version;
  uint64_t write_position;
  uint64_t write_serial;
  uint64_t index_serial;
  uint32_t phase;  // 0 or 1
  // configuration
  uint32_t data_per_index;
  uint64_t size;
  uint32_t generations;
};

struct Index {
  uint32_t offset : 32;  // DATA_BLOCK_SIZE * 2^32
  uint32_t tag : 16;     // 64k/8 entries/bucket =~ .0122% collision
  uint32_t size : 11;    // DATA_BLOCK_SIZE * 4k
  uint32_t next : 4;     // 16 possible next entries, 1 == remove in log/lookaside
  uint32_t phase : 1;
  Index(uint32_t aoffset, uint16_t atag, uint32_t asize, uint32_t anext, uint32_t aphase)
      : offset(aoffset), tag(atag), size(asize), next(anext), phase(aphase) {}
  Index(Index *i) { *(uint64_t *)this = *(uint64_t *)i; }
  Index() {}
  operator bool() { return !!(*(uint64_t *)this); }
};

struct Lookaside {
  uint64_t key;
  Index index;
  // normally index.next is 0, however, for deleted elements, next == 1
  Lookaside(int zero = 0) { memset((void *)this, 0, sizeof(*this)); }
  operator bool() { return index.size != 0; }
};

class LookasideHashFns {
 public:
  static uint32_t hash(Lookaside a) { return ((uint32_t)(a.key >> 32) ^ ((uint32_t)a.key)); }
  static int equal(Lookaside a, Lookaside b) {
    return a.key == b.key && a.index.offset == b.index.offset &&  // size will mismatch on removes
           a.index.next == b.index.next && a.index.phase == b.index.phase;
  }
};

typedef NBlockHash<Lookaside, LookasideHashFns> LookasideCache;

struct LogHeaderFooter {
  uint64_t magic;
  uint64_t last_write_position;
  uint64_t write_position;
  uint64_t last_write_serial;
  uint64_t write_serial;
  uint32_t length;
  uint32_t size;
  uint32_t initial_phase;
  uint32_t final_phase;
};

struct LogEntry {
  uint32_t reserved;
  uint32_t nkeys;
  int size() { return sizeof(LogEntry) + sizeof(Index) + sizeof(uint64_t) * nkeys; }
  Index *index() { return (Index *)(((char *)this) + sizeof(LogEntry)); }
  uint64_t *keys() { return (uint64_t *)(((char *)this) + sizeof(LogEntry) + sizeof(Index)); }
};

struct KeyChain {
  uint64_t key;
  Index next;
};

struct Data {
  uint64_t magic;
  uint64_t length;
  uint64_t write_serial;  // do not move these relative to each other
  uint32_t slice : 20;
  uint32_t gen : 8;
  uint32_t reserved1 : 4;
  uint32_t offset;  // this needs to be above the flags for "clear flags" below to work
  uint32_t size : 11;
  uint32_t remove : 1;
  uint32_t phase : 1;
  uint32_t padding : 1;     // write_serial does not increment
  uint32_t reserved2 : 18;  // do not move these relative to each other
  uint32_t nkeys;
  KeyChain chain[1];
};

struct DataFooter {
  uint32_t nkeys;
};

#define DATA_TO_PTR(_d) (((char *)_d) + (sizeof(Data) + sizeof(KeyChain) * (_d->nkeys - 1) + sizeof(DataFooter)))
#define PTR_TO_DATA(_p)                                                                                            \
  ((Data *)(((char *)_p) -                                                                                         \
            ((sizeof(Data) + sizeof(KeyChain) * (((DataFooter *)(((char *)_p) - sizeof(DataFooter)))->nkeys - 1) + \
              sizeof(DataFooter)))))
#define DATA_TO_FOOTER(_d) ((DataFooter *)(((char *)_d) + (sizeof(Data) + sizeof(KeyChain) * (_d->nkeys - 1))))

struct DebugLogEntry {
  uint64_t key;
  uint64_t ptr;
  void set_tag(uint64_t t) { ptr = (ptr & DEBUG_LOG_PTR_MASK) + (t << DEBUG_LOG_TAG_SHIFT); }
  int get_tag() { return (int)(ptr >> DEBUG_LOG_TAG_SHIFT); }
  void set_i(Index *i) { ptr = (((uintptr_t)i) & DEBUG_LOG_PTR_MASK) + (ptr & ~DEBUG_LOG_PTR_MASK); }
  Index *get_i() { return (Index *)(uintptr_t)(ptr & DEBUG_LOG_PTR_MASK); }
};

// Helper macros for Index
#define element_to_bucket(_e) (((_e) / ELEMENTS_PER_BUCKET))
#define element_to_first_in_bucket(_e) (((_e) & ~(ELEMENTS_PER_BUCKET - 1)))
#define bucket_to_first_element(_b) \
  ((((_b) / BUCKETS_PER_SECTOR) * ELEMENTS_PER_SECTOR) + ((_b) % BUCKETS_PER_SECTOR) * ELEMENTS_PER_BUCKET)
#define sector_to_bucket(_s) ((_s) * BUCKETS_PER_SECTOR)
#define bucket_to_sector(_b) ((_b) / BUCKETS_PER_SECTOR)
#define element_to_first_in_sector(_e) ((_e) & ~(ELEMENTS_PER_SECTOR - 1))
#define sector_to_first_element(_s) ((_s) * ELEMENTS_PER_SECTOR)
#define element_to_sector(_e) ((_e) / ELEMENTS_PER_SECTOR)

#define overflow_element(_s, _i) (sector_to_first_element(_s) + (BUCKETS_PER_SECTOR * ELEMENTS_PER_BUCKET) + (_i))
#define overflow_element_number(_e) ((_e) - (BUCKETS_PER_SECTOR * ELEMENTS_PER_BUCKET))
#define overflow_present(_b) index(bucket_to_first_element(_b))->next
#define overflow_head(_b) index((bucket_to_first_element(_b) + 1))->next
#define overflow_head_element(_b) overflow_element((bucket_to_first_element(_b) + 1)->next)
#define freelist_present(_s) index(sector_to_first_element(_s) + 2)->next
#define freelist_head(_s) index(sector_to_first_element(_s) + 3)->next
#define freelist_head_element(_s) overflow_element(_s, freelist_head(_s))

#define copy_index(x, y)                 \
  do {                                   \
    uint32_t n = (x)->next;              \
    *(uint64_t *)(x) = *(uint64_t *)(y); \
    (x)->next = n;                       \
  } while (0)

#define foreach_contiguous_element(_gen, _elem, _bucket, _tmp) \
  for (int _elem = bucket_to_first_element(_bucket), _tmp = 0; _tmp < BUCKET_SIZE; _tmp++, _elem++)

#define foreach_overflow_element(_gen, _elem, _bucket, _tmp)                                                 \
  if ((_gen)->overflow_present(_bucket))                                                                     \
    for (int _elem = overflow_element(bucket_to_sector(_bucket), (_gen)->overflow_head(_bucket)), _tmp = -1; \
         _elem != _tmp; _tmp = _elem, _elem = overflow_element(bucket_to_sector(_bucket), (_gen)->index(_elem)->next))
