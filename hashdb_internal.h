#pragma once

#include <cstdint>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <cassert>
#include <limits>
#include <chrono>
#include "blake3.h"
#include <print>

#include "hashdb.h"

// Macros
#define HDB_MAGIC 0x46789FED71329ACDull
#define DATA_MAGIC 0x46789DEF78329ADCull
#define LOG_MAGIC 0xA987FE543210FEEFull
#define BAD_DATA ((void *)(uintptr_t)-1)
#define HDB_MAJOR_VERSION 0
#define HDB_MINOR_VERSION 1
// ATOMIC_WRITE_SIZE is the max atomic write size (conservatively for different hardware types)
#define ATOMIC_WRITE_SIZE 512
// SAFE_BLOCK_SIZE is the min safe block size (e.g. 4096 for M.2 SSDs)
#define SAFE_BLOCK_SIZE 4096
#define SIZE_8K 8192

#define ELEMENTS_PER_SECTOR (ATOMIC_WRITE_SIZE / 8)
#define BUCKETS_PER_SECTOR 6
#define BUCKET_SIZE 8
#define FREELIST_SIZE 16
#define ELEMENTS_PER_BUCKET 8
#define WRITE_BUFFERS 2  // do not change without modifying write_buffer()

#define STALE_INDEX_RESULT 0           // ok to not delete or evacuate data
#define SLICE_INDEX_MISMATCH_RESULT 0  // ok not match slice index
#define SYNC_PERIOD 5                  // seconds
#define INDEX_BYTES_PER_PART (ATOMIC_WRITE_SIZE * 1024)
#define LOG_BUFFERS 2

#define LOG_HEADER_SIZE (sizeof(LogHeader))

#define ROUND_TO(_x, _n) (((_x) + ((_n) - 1)) & ~((_n) - 1))
#define ROUND_DOWN(_x, _n) ((_x) & ~((_n) - 1))
#define ROUND_DIV(_x, _n) (((_x) + ((_n) - 1)) / (_n))
#define ROUND_DOWN_SAFE_BLOCK_SIZE(_x) ROUND_DOWN(_x, SAFE_BLOCK_SIZE)
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
struct LogHeader;
struct LogEntry;

// Templates and Helper Functions
template <class K, class HF>
class NBlockHash {
 public:
  std::vector<K> v_;
  uint32_t n_;
  void resize(uint32_t N) {
    n_ = N;
    v_.resize(n_ * 4);
  }
  void clear() { v_.clear(); }
  int count() { return 0; }
  void put(K l) {
    uint32_t h = HF::hash(l);
    K *p = &v_[(h % n_) * 4];
    for (int i = 0; i < 4; i++) {
      if (!p[i]) {
        p[i] = l;
        return;
      }
    }
  }
  int del(K l) {
    uint32_t h = HF::hash(l);
    K *p = &v_[(h % n_) * 4];
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
  size_t alignment = SAFE_BLOCK_SIZE;
  size_t rounded_s = (s + alignment - 1) & ~(alignment - 1);
  int res = posix_memalign(&p, alignment, rounded_s);
  if (res != 0) return nullptr;
  return p;
}

using std::print;
using std::println;

static inline void delete_aligned(void *p) { free(p); }

static inline uint32_t length_to_size(uint64_t l) {
  uint64_t b = ATOMIC_WRITE_SIZE;
  uint32_t m = 0;
  while (1) {
    if (l < 256 * b) return (m << 8) + ROUND_DIV(l, b);
    b <<= 4;
    m++;
  }
}

static inline uint64_t size_to_length(uint64_t s) { return (s & 255) * ((1 << ((s >> 8) * 4)) * ATOMIC_WRITE_SIZE); }

// Structures

struct Header {
  uint64_t magic_;
  uint32_t major_version_;
  uint32_t minor_version_;
  uint64_t write_position_;
  uint64_t write_serial_;
  uint64_t index_serial_;
  uint32_t phase_;  // 0 or 1
  // configuration
  uint32_t data_per_index_;
  uint64_t size_;
  uint32_t generations_;
};

struct Index {
  uint64_t offset_ : 32;  // ATOMIC_WRITE_SIZE * 2^32
  uint64_t tag_ : 16;     // 64k/8 entries/bucket =~ .0122% collision
  uint64_t size_ : 11;    // ATOMIC_WRITE_SIZE * 4k
  uint64_t next_ : 4;     // 16 possible next entries, 1 == remove in log/lookaside
  uint64_t phase_ : 1;
  Index(uint32_t aoffset, uint16_t atag, uint32_t asize, uint32_t anext, uint32_t aphase)
      : offset_(aoffset), tag_(atag), size_(asize), next_(anext), phase_(aphase) {}
  Index(Index *i) { *(uint64_t *)this = *(uint64_t *)i; }
  Index() {}
  operator bool() { return !!(*(uint64_t *)this); }
};

struct Lookaside {
  uint64_t key_;
  Index index_;
  // normally index.next is 0, however, for deleted elements, next == 1
  Lookaside(int zero = 0) { memset((void *)this, 0, sizeof(*this)); }
  operator bool() { return index_.size_ != 0; }
};

class LookasideHashFns {
 public:
  static uint32_t hash(Lookaside a) { return ((uint32_t)(a.key_ >> 32) ^ ((uint32_t)a.key_)); }
  static int equal(Lookaside a, Lookaside b) {
    return a.key_ == b.key_ && a.index_.offset_ == b.index_.offset_ &&  // size will mismatch on removes
           a.index_.next_ == b.index_.next_ && a.index_.phase_ == b.index_.phase_;
  }
};

typedef NBlockHash<Lookaside, LookasideHashFns> LookasideCache;

struct LogHeader {
  uint64_t magic_;
  uint64_t last_write_position_;
  uint64_t write_position_;
  uint64_t last_write_serial_;
  uint64_t write_serial_;
  uint32_t length_;
  uint32_t size_;
  uint32_t initial_phase_;
  uint32_t final_phase_;
  uint8_t hash_[32];
};

struct LogEntry {
  uint32_t reserved_;
  uint32_t nkeys_;
  int size() { return sizeof(LogEntry) + sizeof(Index) + sizeof(uint64_t) * nkeys_; }
  Index *index() { return (Index *)(((char *)this) + sizeof(LogEntry)); }
  uint64_t *keys() { return (uint64_t *)(((char *)this) + sizeof(LogEntry) + sizeof(Index)); }
};

struct KeyChain {
  uint64_t key_;
  Index next_;
};

struct Data {
  uint64_t magic_;
  uint64_t length_;
  uint64_t write_serial_;  // do not move these relative to each other
  uint32_t slice_ : 20;
  uint32_t gen_ : 8;
  uint32_t reserved1_ : 4;
  uint32_t offset_;  // this needs to be above the flags for "clear flags" below to work
  uint32_t size_ : 11;
  uint32_t remove_ : 1;
  uint32_t phase_ : 1;
  uint32_t padding_ : 1;     // write_serial does not increment
  uint32_t reserved2_ : 18;  // do not move these relative to each other
  uint32_t nkeys_;
  uint8_t hash_[32];
  KeyChain chain_[1];
};

struct DataFooter {
  uint32_t nkeys_;
};

#define DATA_TO_PTR(_d) (((char *)_d) + (sizeof(Data) + sizeof(KeyChain) * (_d->nkeys_ - 1) + sizeof(DataFooter)))
#define PTR_TO_DATA(_p)                                                                                             \
  ((Data *)(((char *)_p) -                                                                                          \
            ((sizeof(Data) + sizeof(KeyChain) * (((DataFooter *)(((char *)_p) - sizeof(DataFooter)))->nkeys_ - 1) + \
              sizeof(DataFooter)))))
#define DATA_TO_FOOTER(_d) ((DataFooter *)(((char *)_d) + (sizeof(Data) + sizeof(KeyChain) * (_d->nkeys_ - 1))))

struct DebugLogEntry {
  uint64_t key_;
  uint64_t ptr_;
  void set_tag(uint64_t t) { ptr_ = (ptr_ & DEBUG_LOG_PTR_MASK) + (t << DEBUG_LOG_TAG_SHIFT); }
  int get_tag() { return (int)(ptr_ >> DEBUG_LOG_TAG_SHIFT); }
  void set_i(Index *i) { ptr_ = (((uintptr_t)i) & DEBUG_LOG_PTR_MASK) + (ptr_ & ~DEBUG_LOG_PTR_MASK); }
  Index *get_i() { return (Index *)(uintptr_t)(ptr_ & DEBUG_LOG_PTR_MASK); }
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
#define overflow_present(_b) index(bucket_to_first_element(_b))->next_
#define overflow_head(_b) index((bucket_to_first_element(_b) + 1))->next_
#define overflow_head_element(_b) overflow_element((bucket_to_first_element(_b) + 1)->next_)
#define freelist_present(_s) index(sector_to_first_element(_s) + 2)->next_
#define freelist_head(_s) index(sector_to_first_element(_s) + 3)->next_
#define freelist_head_element(_s) overflow_element(_s, freelist_head(_s))

#define copy_index(x, y)                 \
  do {                                   \
    uint64_t n = (x)->next_;             \
    *(uint64_t *)(x) = *(uint64_t *)(y); \
    (x)->next_ = n;                      \
  } while (0)

#define foreach_contiguous_element(_gen, _elem, _bucket, _tmp) \
  for (int _elem = bucket_to_first_element(_bucket), _tmp = 0; _tmp < BUCKET_SIZE; _tmp++, _elem++)

#define foreach_overflow_element(_gen, _elem, _bucket, _tmp)                                                 \
  if ((_gen)->overflow_present(_bucket))                                                                     \
    for (int _elem = overflow_element(bucket_to_sector(_bucket), (_gen)->overflow_head(_bucket)), _tmp = -1; \
         _elem != _tmp;                                                                                      \
         _tmp = _elem, _elem = overflow_element(bucket_to_sector(_bucket), (_gen)->index(_elem)->next_))
