/*
  Copyright 2003-2022 John Plevyak, All Rights Reserved
*/

#include "prime.h"
#include "hashdb.h"
#include "threadpool.h"
#ifdef linux
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/hdreg.h>
//#include "hdparm.h"
#endif
#include <cerrno>
#include <cinttypes>
#include <assert.h>
#include <stdlib.h>
#include <new>
#include <unistd.h>
#include <pthread.h>
#include <stdio.h>
#include <cstdarg>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#include <vector>
#include <chrono>
#include <limits>

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

/*
 TODO
 speed recovery by specializing check_data() LOW FEATURE
 add support for transactions with transaction number FEATURE
 demand loading of the index FEATURE
 add support for Data smaller than a block (search for subblocks) FEATURE
 allow different generations and the indexes to be moved to a different slice (e.g. SSD) FEATURE
 generations FEATURE
 evacuation FEATURE
 replication FEATURE
 hash to slice (vs separate DB per slice) FEATURE
 add validate() call to API (for remote caching) FEATURE
 optimize callback path (currently requires both callback&bufferwrite threads)
 test FLUSH/ASYNC/SYNC/callback code for remove and write
 add support for chaining keys on replace/remove (have to restore old one) FEATURE
 test Hash::next()
*/

//#define HELGRIND                        1
//#define DEBUG_LOG                       1 // enable

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

#define ROUND_TO(_x, _n) (((_x) + ((_n)-1)) & ~((_n)-1))
#define ROUND_DOWN(_x, _n) ((_x) & ~((_n)-1))
#define ROUND_DIV(_x, _n) (((_x) + ((_n)-1)) / (_n))
#define ROUND_DOWN_SAFE_SECTOR_SIZE(_x) ROUND_DOWN(_x, SAFE_SECTOR_SIZE)
#define KEY2TAG(_x) ((_x) >> 48)
#define MODULAR_DIFFERENCE(_x, _y, _m) (((_x) + (_m) - (_y)) % (_m))

#define forv_Vec(_type, _x, _v) for (auto _x : _v)
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
#define Vec std::vector

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
const static char *DEBUG_LOG_TYPE_NAME[] = {"empty ", "set   ", "delete ", "ins la",
                                            "del la", "inskey", "delkey",  "write "};

class HDB;
class Gen;
class Slice;
struct Doer;
struct Data;
struct LogHeaderFooter;
struct LogEntry;

typedef HashDB::Extent Extent;
typedef HashDB::Marshal Marshal;
typedef HashDB::Callback Callback;

Callback *HashDB::SYNC = (Callback *)(uintptr_t)1;
Callback *HashDB::FLUSH = (Callback *)(uintptr_t)2;
Callback *HashDB::ASYNC = (Callback *)(uintptr_t)0;
#define SPECIAL_CALLBACK(_c) (((uintptr_t)(_c)) <= 2)

// should be no bigger than 512 bytes (one (small) sector size)
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

// The index layout is 8 buckets of 6 associative indexes
// per small sector (512 bytes).  The last 16 elements (overflow)
// are stored on a freelist and allocated and freed to there.

// element: a single index entry
// bucket: 6 associative elements
// sector: 64 total elements (8x6 + 16)

// 64 bits; 512 / 8 / 8 = 8 buckets per small sector
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
  Lookaside(int zero = 0) { memset(this, 0, sizeof(*this)); }
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

#define element_to_bucket(_e) (((_e) / ELEMENTS_PER_BUCKET))
#define element_to_first_in_bucket(_e) (((_e) & ~(ELEMENTS_PER_BUCKET - 1)))
#define bucket_to_first_element(_b) \
  ((((_b) / BUCKETS_PER_SECTOR) * ELEMENTS_PER_SECTOR) + ((_b) % BUCKETS_PER_SECTOR) * ELEMENTS_PER_BUCKET)
#define sector_to_bucket(_s) ((_s)*BUCKETS_PER_SECTOR)
#define bucket_to_sector(_b) ((_b) / BUCKETS_PER_SECTOR)
#define element_to_first_in_sector(_e) ((_e) & ~(ELEMENTS_PER_SECTOR - 1))
#define sector_to_first_element(_s) ((_s)*ELEMENTS_PER_SECTOR)
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

class WriteBuffer {
 public:
  Gen *gen;
  uint8_t *start;
  uint8_t *cur;
  uint8_t *end;
  uint8_t *last;
  int writing;
  uint32_t phase, committed_phase, next_phase;
  uint64_t offset, next_offset;
  uint64_t pad_position;
  uint64_t committed_write_position;
  uint64_t committed_write_serial;
  ssize_t result;

  void init(Gen *g, int i);
};

//
// Gen (Generation): self contained database
//

class Gen {
 public:
  Slice *slice;
  int igen;
  Header *header;
  Header *sync_header;
  uint64_t size;
  uint32_t buckets;
  uint64_t header_offset;
  uint64_t index_offset;
  uint64_t index_size;
  uint64_t log_offset[2];
  uint64_t log_size;
  uint64_t log_buffer_size;
  uint64_t data_offset;
  uint64_t data_size;
  int index_parts;

  void *raw_index;
  Index *index(int e) { return &((Index *)raw_index)[e]; }
  uint8_t *index_dirty_marks;
  uint8_t *sync_buffer;
  int syncing;
  void dirty_sector(int s) { index_dirty_marks[s / (INDEX_BYTES_PER_PART / SECTOR_SIZE)] = 1; }
  int is_marked_part(int p) { return index_dirty_marks[p]; }
  void unmark_part(int p) { index_dirty_marks[p] = 0; }

  WriteBuffer lbuf[LOG_BUFFERS];
  WriteBuffer wbuf[WRITE_BUFFERS];
  uint32_t cur_log;
  uint32_t cur_write;
  uint64_t log_position;
  LookasideCache lookaside;
  pthread_cond_t write_condition;
  pthread_mutex_t mutex;
  int sync_part;
  uint64_t committed_write_position;
  uint64_t committed_write_serial;
  uint32_t committed_phase;
  char *debug_log, *debug_log_ptr;

  HDB *hdb();
  int sectors() { return (buckets + (BUCKETS_PER_SECTOR - 1)) / BUCKETS_PER_SECTOR; }
  int log_phase() { return header->index_serial & 1; }

  void alloc_header();
  void init_header();
  int load_header();
  void write_header(Header *h);
  void free_header();
  void snap_header();
  void alloc_index();
  void init_index();
  int load_index();
  void write_index();
  void write_index_part(int p);
  void write_upto_index_part(int p, int stop_on_marked = 0);
  void complete_index_sync();
  void free_index();
  void free_bufs();
  void write_log_buffer();
  void write_buffer(int force_wrap = 0);
  void compute_sizes(uint64_t asize = 0, uint32_t data_per_index = 16384);
  void init_debug_log();
  void debug_log_it(uint64_t key, Index *i, int tag);
  void dump_debug_log();

  int init();
  int open();
  int verify();
  int close();

  int recovery();
  int recover_log();
  int recover_data();
  int save();
  void free();
  void periodic_sync();

  int read(uint64_t key, Vec<Extent> &hit);
  int next(uint64_t key, Data *d, Vec<Extent> &hit);
  int write(uint64_t *key, int nkeys, Marshal *marshal);
  int write_remove(uint64_t *key, int nkeys, Index *i);
  int read_element(Index *i, uint64_t key, Vec<Extent> &hit);
  Data *read_data(Index *i);
  WriteBuffer *get_buffer(int nkeys, uint64_t data_len);

  int find_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset);
  void insert_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset);
  void delete_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset);
  void set_element(Index *i, uint64_t key, bool phase, uint32_t size, uint32_t offset);
  void find_indexes(uint64_t key, Vec<Index> &indexes);
  void delete_collision(uint64_t key);
  int delete_bucket_element(int e, int b);
  int delete_overflow_element(int e, int p, int b);
  void reserve_log_space(int nkeys);
  void insert_log(uint64_t *key, int nkeys, Index *i);
  void insert_lookaside(uint64_t key, Index *i);
  int delete_lookaside(uint64_t key, Index *i);
  void clean_sector(int s);
  void clean_index_part(int p);
  void commit_log_entry(LogEntry *d);
  void commit_data(Data *d);
  void commit_buffer(uint8_t *start, uint8_t *end);
  int check_data(Data *d, uint64_t o, uint64_t l, uint32_t offset, int recovery = 0);
  void chain_keys_for_write(Data *d);

  ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
  ssize_t pread(int fd, void *buf, size_t count, off_t offset);

  Gen(Slice *aslice, int aigen = 0);
};
#define forv_Gen(_x, _v) forv_Vec(Gen, _x, _v)

//
// Slices: contiguous storage
//

class Slice {
 public:
  HDB *hdb;
  int islice;
  int fd;
  char *pathname;
  char *layout_pathname;
  uint64_t layout_size;
  uint64_t size;
  uint32_t block_size;
  uint32_t is_raw : 1;
  uint32_t is_file : 1;
  uint32_t is_dir : 1;

  Vec<Gen *> gen;

  int init();
  int open();
  int might_exist(uint64_t key);
  int read(uint64_t key, Vec<Extent> &hit);
  int write(uint64_t *key, int nkeys, Marshal *marshal, Callback *callback);
  int verify();
  int close();

  Slice(HDB *ahdb, int aislice, char *apathname, uint64_t alayout_size = -1);
};
#define forv_Slice(_x, _v) forv_Vec(Slice, _x, _v)

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

void hashdb_print_data_header(void *p) {
  Data *d = PTR_TO_DATA(p);
  printf("offset %u phase %d remove %d\n", d->offset, d->phase, d->remove);
}

struct DebugLogEntry {
  uint64_t key;
  uint64_t ptr;
  void set_tag(uint64_t t) { ptr = (ptr & DEBUG_LOG_PTR_MASK) + (t << DEBUG_LOG_TAG_SHIFT); }
  int get_tag() { return (int)(ptr >> DEBUG_LOG_TAG_SHIFT); }
  void set_i(Index *i) { ptr = (((uintptr_t)i) & DEBUG_LOG_PTR_MASK) + (ptr & ~DEBUG_LOG_PTR_MASK); }
  Index *get_i() { return (Index *)(uintptr_t)(ptr & DEBUG_LOG_PTR_MASK); }
};

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

HashDB::HashDB() {
  init_data_per_index = 16384;
  reinit_on_open_error = false;
  write_buffer_size = (1024 * 1024);  // 1MB
  concurrency = 100;
  sync_wait_msec = 50;
  chain_collisions = 0;
  thread_pool = 0;
}

HDB::HDB() {
  pthread_mutex_init(&mutex, 0);
  pthread_mutex_lock(&mutex);
  pthread_cond_init(&sync_condition, 0);
  sync_thread = 0;
  thread_pool_allocated = 0;
  thread_pool_max_threads = std::numeric_limits<int>::max();
  current_write_slice = 0;
  exiting = 0;
  read_only = 0;
  init_generations = 1;
  separate_db_per_slice = true;
  replication_factor = 0;
  pthread_mutex_unlock(&mutex);
}

inline HDB *Gen::hdb() { return slice->hdb; }

int HDB::err(cchar *format, ...) {
  va_list va;
  std::vector<char> s(strlen(format) + 100);
  strcpy(s.data(), "HDB_ERROR: ");
  strcat(s.data(), format);
  strcat(s.data(), "\n");
  va_start(va, format);
  vprintf(s.data(), va);
  return 1;
}

int HDB::warn(cchar *format, ...) {
  va_list va;
  std::vector<char> s(strlen(format) + 100);
  strcpy(s.data(), "HDB_WARN: ");
  strcat(s.data(), format);
  strcat(s.data(), "\n");
  va_start(va, format);
  vprintf(s.data(), va);
  return 1;
}

void WriteBuffer::init(Gen *g, int i) {
  gen = g;
  start = cur = end = 0;
  writing = 0;
}

void Gen::init_debug_log() {
  char debug_log_filename[256];
  strcpy(debug_log_filename, DEBUG_LOG_FILENAME);
  sprintf(debug_log_filename + strlen(debug_log_filename), "%d.%d", slice->islice, igen);
  ::truncate(debug_log_filename, DEBUG_LOG_SIZE);
  int debug_log_fd = ::open(debug_log_filename, O_RDWR | O_CREAT | O_NOATIME, 00660);
  assert(debug_log_fd > 0);
  debug_log_ptr = debug_log =
      (char *)mmap(0, DEBUG_LOG_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_NORESERVE, debug_log_fd, 0);
  ::close(debug_log_fd);
}

Gen::Gen(Slice *aslice, int aigen) {
  pthread_mutex_init(&mutex, 0);
  pthread_mutex_lock(&mutex);
  memset((void*)this, 0, sizeof(*this));
  slice = aslice;
  igen = aigen;
  for (int i = 0; i < LOG_BUFFERS; i++) lbuf[i].init(this, i);
  for (int i = 0; i < WRITE_BUFFERS; i++) wbuf[i].init(this, i);
  lookaside.clear();
  pthread_cond_init(&write_condition, 0);
  syncing = 0;
#ifdef DEBUG_LOG
  init_debug_log();
#endif
  pthread_mutex_unlock(&mutex);
}

Slice::Slice(HDB *ahdb, int aislice, char *alayout_pathname, uint64_t alayout_size) {
  struct stat stat_buf;

  hdb = ahdb;
  islice = aislice;
  fd = ::open(alayout_pathname, O_RDONLY);
  layout_pathname = strdup(alayout_pathname);
  layout_size = alayout_size;
  size = 0;
  is_raw = is_file = is_dir = 0;

  if (stat(layout_pathname, &stat_buf)) fail("unable to stat '%s'", layout_pathname);
  switch (stat_buf.st_mode & S_IFMT) {
    case S_IFBLK:
    case S_IFCHR:
      is_raw = 1;
      break;
    case S_IFDIR:
      is_dir = 1;
      break;
    case S_IFREG:
      is_file = 1;
      break;
    default:
      fail("unknown stat result '%s'", pathname);
  }
  if (is_file)
    pathname = layout_pathname;
  else if (is_dir) {
    char p[1024];
    strcpy(p, layout_pathname);
    strcat(p, "/hashdb.data");
    pathname = strdup(p);
  } else {
#ifndef linux
    fail("raw devices not supported");
#else
    if (::ioctl(fd, BLKGETSIZE64, &size)) fail("unable to get raw device size");
    pathname = layout_pathname;
#endif
  }
  ::close(fd);
  if ((fd = ::open(pathname, O_CREAT | O_RDWR | O_DIRECT, S_IRUSR | S_IWUSR)) < 0)
    fail("unable to open '%s'", pathname);
  if (::fstat(fd, &stat_buf)) fail("unable to stat '%s'", pathname);
  block_size = SAFE_SECTOR_SIZE;
  if (!is_raw) {
    if ((stat_buf.st_mode & S_IFMT) != S_IFREG) fail("bad file type '%s'", pathname);
    size = stat_buf.st_size;
  } else {
#ifdef linux
    if (::ioctl(fd, BLKSSZGET, &block_size)) fail("unable to get block size '%s'", pathname);
    uint16_t id[256];
    bool write_cache_disabled = 0;
    if (!::ioctl(fd, HDIO_GET_IDENTITY, id))
      if (id[82] & 0x20) write_cache_disabled = id[85] & 0x20 ? false : true;
    if (!write_cache_disabled) {
      if (::ioctl(fd, HDIO_SET_WCACHE, 0)) {
        // uint8_t setcache[4] = {0xef, 0, 0x82, 0};
        // if (do_drive_cmd(fd, setcache))
        //   hdb->warn("unable to disable write cache, data loss possible on power loss '%s': %s", pathname,
        //             strerror(errno));
      }
    }
#endif
  }
  if (layout_size && size != layout_size) {
    if (is_file || is_dir) {
      if (ftruncate(fd, layout_size) < 0) fail("unable to truncate '%s'", pathname);
    }
    size = layout_size;
  }
  size = ROUND_DOWN_SAFE_SECTOR_SIZE(size);
}

#include <cstdlib>

static inline void *new_aligned(size_t s) {
    size_t alignment = SAFE_SECTOR_SIZE;
    // Round s up to the nearest multiple of alignment
    size_t rounded_s = (s + alignment - 1) & ~(alignment - 1);
    return aligned_alloc(alignment, rounded_s);
}

static inline void delete_aligned(void *p) {
    free(p);
}

inline ssize_t Gen::pwrite(int fd, const void *buf, size_t count, off_t offset) {
  if (!hdb()->read_only) {
    // printf("pwrite %lld %lld\n", (long long int)count, (long long int)offset);
    return ::pwrite(fd, buf, count, offset);
  } else
    return count;
}

inline ssize_t Gen::pread(int fd, void *buf, size_t count, off_t offset) {
  // printf("pread %lld %lld\n", (long long int)count, (long long int)offset);
  return ::pread(fd, buf, count, offset);
}

#ifdef DEBUG_LOG
void Gen::debug_log_it(uint64_t key, Index *i, int tag) {
  if (hdb()->read_only) return;
  DebugLogEntry *e = (DebugLogEntry *)debug_log_ptr;
  e->key = key;
  e->set_tag(tag);
  assert(tag);
  assert(e->get_tag() == tag);
  e->set_i(i);
  assert(e->get_i() == i);
  // printf("%s %llu %p\n", DEBUG_LOG_TYPE_NAME[e->get_tag()], e->key, e->get_i());
  debug_log_ptr += sizeof(DebugLogEntry);
  if (debug_log_ptr - debug_log + sizeof(DebugLogEntry) > DEBUG_LOG_SIZE) {
    debug_log_ptr = debug_log;
    printf("rolling debug_log %d %d\n", slice->islice, igen);
  } else
    memset(debug_log_ptr, 0, sizeof(DebugLogEntry));
}
#else
void Gen::debug_log_it(uint64_t key, Index *i, int tag) { assert(0); }
#define debug_log_it(a, b, c)
#endif

void Gen::dump_debug_log() {
  char *p = debug_log;
  while (p - debug_log + sizeof(DebugLogEntry) < DEBUG_LOG_SIZE) {
    DebugLogEntry *e = (DebugLogEntry *)p;
    if (!e->get_tag()) break;
    printf("%s %lu %p\n", DEBUG_LOG_TYPE_NAME[e->get_tag()], e->key, e->get_i());
    p += sizeof(DebugLogEntry);
  }
}

void Gen::alloc_header() {
  if (!header) header = (Header *)new_aligned(SAFE_SECTOR_SIZE);
  memset(header, 0, SAFE_SECTOR_SIZE);
  if (!sync_header) sync_header = (Header *)new_aligned(SAFE_SECTOR_SIZE);
  memset(sync_header, 0, SAFE_SECTOR_SIZE);
}

void Gen::compute_sizes(uint64_t asize, uint32_t data_per_index) {
  if (!asize) {
    uint64_t tmp = slice->size;
    tmp = ROUND_DOWN_SAFE_SECTOR_SIZE(tmp / slice->gen.size());
    if (slice->is_raw)  // don't overwrite MBR
      tmp -= SAFE_SECTOR_SIZE;
    asize = tmp;
  }
  size = asize;
  if (slice->is_raw)  // don't overwrite MBR
    header_offset = SAFE_SECTOR_SIZE;
  else
    header_offset = 0;
  header_offset += size * igen;
  // compute buckets
  uint64_t s = size;
  s -= SAFE_SECTOR_SIZE;  // header + footer
  uint64_t i = s / data_per_index;
  int sectors = i / ELEMENTS_PER_SECTOR;
  buckets = sectors * BUCKETS_PER_SECTOR;
  buckets = next_higher_prime(buckets);
  sectors = (buckets + 5) / 6;
  // compute index length
  i = sectors;
  i *= ELEMENTS_PER_SECTOR;
  i *= sizeof(Index);
  assert(8 == sizeof(Index));
  index_size = ROUND_TO(i, SAFE_SECTOR_SIZE);
  index_parts = ROUND_DIV(index_size, INDEX_BYTES_PER_PART);
  log_size = index_size;
  log_buffer_size = hdb()->write_buffer_size < log_size ? hdb()->write_buffer_size : log_size;
  // compute offsets
  index_offset = header_offset + SAFE_SECTOR_SIZE;
  log_offset[0] = index_offset + index_size;
  log_offset[1] = log_offset[0] + log_size;
  data_offset = log_offset[1] + log_size;
  // compute data length
  data_size = s - header_offset - SAFE_SECTOR_SIZE - index_size - log_size;
  data_size = ROUND_DOWN_SAFE_SECTOR_SIZE(data_size);
  assert(data_size >= hdb()->write_buffer_size);
  // printf("index_size = %llu data_size = %llu data_offset = %llu\n", index_size, data_size, data_offset);
}

void Gen::init_header() {
  compute_sizes();
  // setup header
  alloc_header();
  header->magic = HDB_MAGIC;
  header->major_version = HDB_MAJOR_VERSION;
  header->minor_version = HDB_MINOR_VERSION;
  header->write_position = 0;
  header->index_serial = (int32_t)time(NULL);
  // semi-monotonic, semi-random for fast clearing
  header->write_serial = (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count();
  header->phase = 0;
  header->data_per_index = hdb()->init_data_per_index;
  header->size = size;
  header->generations = slice->gen.size();
  memcpy(sync_header, header, SAFE_SECTOR_SIZE);
}

int Gen::load_header() {
  alloc_header();
  if (pread(slice->fd, header, SAFE_SECTOR_SIZE, header_offset) != SAFE_SECTOR_SIZE) return -1;
  return 0;
}

void Gen::free_header() {
  if (header) {
    delete_aligned(header);
    header = 0;
  }
  if (sync_header) {
    delete_aligned(sync_header);
    sync_header = 0;
  }
}

void Gen::snap_header() {
  memcpy(sync_header, header, SAFE_SECTOR_SIZE);
  sync_header->write_position = committed_write_position;
  sync_header->write_serial = committed_write_serial;
  sync_header->phase = committed_phase;
}

void Gen::free_index() {
  if (raw_index) {
    delete_aligned(raw_index);
    raw_index = 0;
  }
  if (index_dirty_marks) {
    delete[] index_dirty_marks;
    index_dirty_marks = 0;
  }
  if (sync_buffer) {
    delete_aligned(sync_buffer);
    sync_buffer = 0;
  }
}

void Gen::free_bufs() {
  for (int i = 0; i < LOG_BUFFERS; i++) {
    if (lbuf[i].start) {
      delete_aligned(lbuf[i].start);
      lbuf[i].start = 0;
    }
  }
  for (int i = 0; i < WRITE_BUFFERS; i++) {
    if (wbuf[i].start) {
      delete_aligned(wbuf[i].start);
      wbuf[i].start = 0;
    }
  }
}

void Gen::alloc_index() {
  free_index();
  assert(index_size);
  raw_index = new_aligned(index_size);
  index_dirty_marks = new uint8_t[index_parts];
  sync_buffer = (uint8_t *)new_aligned(INDEX_BYTES_PER_PART);
}

void Gen::init_index() {
  alloc_index();
  memset(raw_index, 0, index_size);
  memset(index_dirty_marks, 1, index_parts);
  for (int s = 0; s < sectors(); s++) {
    freelist_present(s) = 1;
    freelist_head(s) = 0;
    for (int i = 0; i < FREELIST_SIZE; i++) {
      int e = overflow_element(s, i);
      if (i != FREELIST_SIZE - 1)
        index(e)->next = i + 1;
      else
        index(e)->next = i;  // terminate with self pointer
    }
  }
}

int Gen::init() {
  pthread_mutex_lock(&mutex);
  init_header();
  init_index();
  int r = save();
  pthread_mutex_unlock(&mutex);
  return r;
}

int Slice::init() {
  int res = 0;
  forv_Gen(g, gen) if (!res) res = g->init();
  else g->init();
  return res;
}

int Gen::recovery() {
  int r = 0;
  r = recover_log() | r;
  r = recover_data() | r;
  return r;
}

int Gen::recover_log() {
  int ret = 0;
  int l = (int)log_buffer_size, keep = 0;
  uint8_t *buf = (uint8_t *)new_aligned(l);
  uint64_t wpos = 0;
  while (1) {
    if (pread(slice->fd, buf + keep, l - keep, log_offset[log_phase()] + wpos + keep) < 0) {
      ret = -1;
      goto Lreturn;
    }
    keep = 0;
    LogHeaderFooter *h = (LogHeaderFooter *)buf;
    if (h->magic != LOG_MAGIC || h->initial_phase != header->phase ||
        h->last_write_position != header->write_position || h->last_write_serial != header->write_serial)
      break;
    LogHeaderFooter *f = (LogHeaderFooter *)(buf + h->length - sizeof(LogHeaderFooter));
    if (f->magic != LOG_MAGIC || f->initial_phase != header->phase ||
        f->last_write_position != header->write_position || f->last_write_serial != header->write_serial)
      break;
    header->write_serial = h->write_serial;
    header->write_position = h->write_position;
    header->phase = h->final_phase;
    committed_write_position = header->write_position;
    committed_write_serial = header->write_serial;
    committed_phase = header->phase;
    log_position = wpos;
    uint8_t *b = buf + sizeof(LogHeaderFooter);
    while (1) {
      if ((size_t)(h->length - (b - buf) - sizeof(LogHeaderFooter)) < sizeof(LogEntry)) {
        keep = l - (b - buf);
        if (keep >= l) goto Lreturn;
        break;
      }
      LogEntry *e = (LogEntry *)b;
      // printf("recovering log entry %s %d keys\n", e->index()->next ? "remove" : "add", e->nkeys);
      commit_log_entry(e);
      b += e->size();
    }
    if (keep) memmove(buf, buf + l - keep, keep);
    wpos += l - keep;
  }
Lreturn:
  // printf("recovered log absolute write position = %lld, wserial = %lld, phase = %d\n", header->write_position +
  // data_offset, header->write_serial, header->phase);
  delete_aligned(buf);
  return ret;
}

int Gen::recover_data() {
  int ret = 0;
  uint64_t wserial = header->write_serial;
  int l = hdb()->write_buffer_size, keep = 0;
  uint8_t *buf = (uint8_t *)new_aligned(l);
  uint64_t wpos = header->write_position;
  while (1) {
  Lwrap:;
    int bytes = l - keep;
    if (wpos + bytes > data_size) {
      bytes = data_size - wpos;
      if (!bytes) {
        header->write_position = wpos = 0;
        header->phase = !header->phase;
        committed_write_position = header->write_position;
        committed_write_serial = header->write_serial;
        committed_phase = header->phase;
        goto Lwrap;
      }
    }
    if (pread(slice->fd, buf + keep, bytes, data_offset + wpos + keep) < 0) {
      ret = -1;
      goto Lreturn;
    }
    keep = 0;
    uint8_t *b = buf;
    while (1) {
      keep = l - (b - buf);
      if ((size_t)keep < sizeof(Data)) {
        if (keep >= l) goto Lreturn;
        break;
      }
      Data *d = (Data *)b;
      int len = size_to_length(d->size);
      if (keep < (int)(len + FOOTER_SIZE)) {
        if (keep >= l) goto Lreturn;
        break;
      }
      if (check_data(d, wpos + (b - buf), len, d->offset, 1)) goto Lreturn;
      if (d->write_serial != wserial) {
        if (!(d->padding && d->write_serial == wserial - 1)) goto Lreturn;
      }
      if (!d->padding) {
        Data *f = (Data *)(b + len);
        if (check_data(f, wpos + (b - buf) + len, size_to_length(f->size), f->offset, 1)) goto Lreturn;
        // printf("recovering data %s %d keys at %lld\n",  d->remove ? "remove" : "add", d->nkeys, data_offset + wpos +
        // (b - buf));
        commit_data(d);
        wserial++;
      }
      b += len;
      header->write_position = wpos + (b - buf);
      header->write_serial = wserial;
      committed_write_position = header->write_position;
      committed_write_serial = header->write_serial;
      committed_phase = header->phase;
    }
    if (keep) memmove(buf, buf + l - keep, keep);
    wpos += l - keep;
  }
Lreturn:
  // printf("recovered data absolute write position = %lld, wserial = %lld, phase = %d\n", header->write_position +
  // data_offset, header->write_serial, header->phase);
  delete_aligned(buf);
  return ret;
}

int Gen::load_index() {
  alloc_index();
  memset(index_dirty_marks, 0, index_parts);
  if ((uint64_t)pread(slice->fd, raw_index, index_size, index_offset) != index_size) return -1;
  if (recovery() < 0) return -1;
  return 0;
}

int Gen::open() {
  pthread_mutex_lock(&mutex);
  compute_sizes();
  if (load_header() < 0) goto Lerror;
  if (header->magic != HDB_MAGIC || header->major_version != HDB_MAJOR_VERSION) {
    if (slice->hdb->reinit_on_open_error)
      init();
    else
      goto Lerror;
  } else {  // normal load path
    compute_sizes(header->size, header->data_per_index);
    load_index();
  }
  for (int i = 0; i < LOG_BUFFERS; i++) {
    lbuf[i].offset = std::numeric_limits<unsigned long long>::max();
    lbuf[i].start = lbuf[i].cur = (uint8_t *)new_aligned(log_buffer_size);
    lbuf[i].end = lbuf[i].start + log_buffer_size;
    lbuf[i].cur += sizeof(LogHeaderFooter);
  }
  cur_log = 0;
  log_position = 0;
  for (int i = 0; i < WRITE_BUFFERS; i++) {
    wbuf[i].offset = std::numeric_limits<unsigned long long>::max();  // short circuit test in Gen::read_data
    wbuf[i].start = wbuf[i].cur = (uint8_t *)new_aligned(hdb()->write_buffer_size);
    wbuf[i].end = wbuf[i].start + hdb()->write_buffer_size;
    wbuf[i].next_offset = std::numeric_limits<unsigned long long>::max();
    wbuf[i].next_phase = 0;
  }
  cur_write = 0;
  wbuf[cur_write].offset = data_offset + header->write_position;
  wbuf[cur_write].phase = header->phase;
  committed_write_position = header->write_position;
  committed_write_serial = header->write_serial;
  committed_phase = header->phase;
  for (int s = 0; s < sectors(); s++) clean_sector(s);
  snap_header();
  pthread_mutex_unlock(&mutex);
  return 0;
Lerror:
  pthread_mutex_unlock(&mutex);
  return -1;
}

static inline void wait_for_write_to_complete(WriteBuffer *b) {
  while (b->writing) pthread_cond_wait(&b->gen->write_condition, &b->gen->mutex);
}

static inline int cmp_wpos(uint64_t wpos1, int phase1, uint64_t wpos2, int phase2) {
  if (phase1 == phase2) return wpos1 < wpos2 ? -1 : ((wpos1 == wpos2) ? 0 : 1);
  return wpos1 > wpos2 ? -1 : ((wpos1 == wpos2) ? 0 : 1);
}

static inline void wait_for_write_commit(Gen *g, uint64_t wpos, int phase) {
  while (cmp_wpos(wpos, phase, g->committed_write_position, g->committed_phase) < 0) {
    WriteBuffer &w = g->wbuf[g->cur_write];
    if (cmp_wpos(wpos, phase, w.offset - g->data_offset + (w.cur - w.start), w.phase) < 0) g->write_buffer();
    pthread_cond_wait(&g->write_condition, &g->mutex);
  }
}

static inline void wait_for_flush(WriteBuffer *b, int wait_msec) {
  uint64_t o = b->next_offset;
  Gen *g = b->gen;
  auto startt = std::chrono::high_resolution_clock::now();
  auto donet = startt + std::chrono::milliseconds(wait_msec);
  while (std::chrono::high_resolution_clock::now() < donet && b->next_offset == o && b->start != b->cur) {
    struct timespec ts;
    ts.tv_sec = wait_msec / 1000;
    ts.tv_nsec = (wait_msec % 1000) * 1000000;
    pthread_cond_timedwait(&g->write_condition, &g->mutex, &ts);
  }
  if (b->next_offset == o && b->start != b->cur) {
    if (!b->writing) g->write_buffer();
    wait_for_write_to_complete(b);
  }
}

WriteBuffer *Gen::get_buffer(int nkeys, uint64_t l) {
  l += FOOTER_SIZE;
Lagain:
  reserve_log_space(nkeys);
  WriteBuffer *b = &wbuf[cur_write];
  if (b->writing) {
    wait_for_write_to_complete(b);
    goto Lagain;
  }
  if (l > hdb()->write_buffer_size) return 0;
  int force_wrap = b->offset + (b->cur - b->start) + l > data_offset + data_size;
  if (b->cur + l >= b->end || force_wrap) {
    write_buffer(force_wrap);
    goto Lagain;
  }
  return b;
}

void Gen::write_index_part(int p) {
  clean_index_part(p);
  uint64_t o = INDEX_BYTES_PER_PART * p;
  uint8_t *b = ((uint8_t *)raw_index) + o;
  int l = INDEX_BYTES_PER_PART;
  if (p >= index_parts - 1) l = index_size - o;
  memcpy(sync_buffer, b, l);
  pthread_mutex_unlock(&mutex);  // release lock around write
  if (pwrite(slice->fd, sync_buffer, l, index_offset + o) != (ssize_t)l)
    hdb()->err("unable to save index part %d for gen %d '%s'", p, igen, slice->pathname);
  pthread_mutex_lock(&mutex);
  // printf("write index part %d at %lld length %d\n", p, index_offset + o, l);
}

void Gen::write_upto_index_part(int p, int stop_on_marked) {
  while (syncing)  // prevent function from being reentered
    pthread_cond_wait(&write_condition, &mutex);
  syncing = 1;
  int broadcast = 0;
  if (p > index_parts) p = index_parts;
  for (; sync_part < p;) {
    if (is_marked_part(sync_part)) {
      write_index_part(sync_part);
      unmark_part(sync_part);
      sync_part++;
      broadcast = 1;
      if (stop_on_marked) break;
    } else
      sync_part++;
  }
  syncing = 0;
  if (broadcast) pthread_cond_broadcast(&write_condition);
}

void Gen::write_index() {
  if (pwrite(slice->fd, raw_index, index_size, index_offset) != (ssize_t)index_size)
    hdb()->err("unable to save index for gen %d '%s'", igen, slice->pathname);
}

void Gen::write_header(Header *aheader) {
  if (pwrite(slice->fd, aheader, SAFE_SECTOR_SIZE, header_offset) != (ssize_t)SAFE_SECTOR_SIZE)
    hdb()->err("unable to save header for gen %d '%s'", igen, slice->pathname);
}

int Gen::save() {
  WriteBuffer &w = wbuf[cur_write];
  if (w.start != w.cur) write_buffer();
  for (int i = 0; i < WRITE_BUFFERS; i++) wait_for_write_to_complete(&wbuf[i]);
  write_upto_index_part(index_parts);
  write_header(header);
  return 0;
}

static int verify_offset(Gen *g, Index *i) {
  if (i->size) {
    if (g->committed_phase == i->phase) {
      uint64_t o = ((uint64_t)i->offset) * DATA_BLOCK_SIZE + size_to_length(i->size);
      if (g->committed_write_position < o) return -1;
    } else {
      uint64_t o = ((uint64_t)i->offset) * DATA_BLOCK_SIZE;
      if (g->committed_write_position > o) return -1;
    }
  }
  return 0;
}

static int verify_element(Gen *g, int e) {
  Index *i = g->index(e);
  if (!i->size) return 0;  // empty
  if (verify_offset(g, i)) return -1;
  Data *d = g->read_data(i);
  if (!d) return -1;
  printf("key %lu size %d off %d\n", d->chain[0].key, d->size, i->offset);
  delete_aligned(d);
  return 0;
}

int Gen::verify() {
  pthread_mutex_lock(&mutex);
  int ret = 0;
  for (uint32_t b = 0; b < buckets; b++) {
    foreach_contiguous_element(this, e, b, tmp) ret = verify_element(this, e) | ret;
    foreach_overflow_element(this, e, b, tmp) ret = verify_element(this, e) | ret;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

void Gen::free() {
  free_header();
  free_index();
  free_bufs();
  lookaside.clear();
}

int Gen::close() {
  pthread_mutex_lock(&mutex);
  int res = save();
  free();
  pthread_mutex_unlock(&mutex);
  return res;
}

int Slice::open() {
  int res = 0;
  forv_Gen(g, gen) if (!res) res = g->open();
  else g->open();
  return res;
}

int Slice::verify() {
  int res = 0;
  forv_Gen(g, gen) if (!res) res = g->verify();
  else g->verify();
  return res;
}

int Slice::close() {
  forv_Gen(g, gen) {
    g->close();
    DELETE(g);
  }
  return 0;
}

void HDB::free_slices() {
  forv_Vec(Slice, x, slice) { DELETE(x); }
  slice.clear();
}

struct Doer {
  Slice *s;
  pthread_barrier_t *barrier;
  int res;

  void init(Slice *as, pthread_barrier_t *abarrier) {
    s = as;
    barrier = abarrier;
    res = 0;
  }
};

int HDB::foreach_slice(void *(*pfn)(Doer *)) {
  pthread_barrier_t barrier;
  pthread_barrier_init(&barrier, NULL, slice.size());
  std::vector<Doer> doer(slice.size());
  int res = 0;
  for (size_t i = 0; i < slice.size(); i++) {
    doer[i].init(slice[i], &barrier);
    thread_pool->add_job((void *(*)(void *))pfn, (void *)&doer[i]);
  }
  pthread_barrier_wait(&barrier);
  for (size_t i = 0; i < slice.size(); i++) res |= doer[i].res;
  return res;
}

#define DO_SLICE(_op)              \
  static void *do_##_op(Doer *d) { \
    d->res = d->s->_op();          \
    pthread_barrier_wait(d->barrier);    \
    return NULL;                   \
  }                                \
  static int _op##_slices(HDB *hdb) { return hdb->foreach_slice(do_##_op); }

DO_SLICE(init);
DO_SLICE(open);
DO_SLICE(verify);
DO_SLICE(close);

HashDB *new_HashDB() { return new HDB(); }

int HashDB::slice(char *pathname, uint64_t size, bool init) {
  HDB *hdb = ((HDB *)this);
  pthread_mutex_lock(&hdb->mutex);
  Slice *s = new Slice(hdb, hdb->slice.size(), pathname, size);
  for (int i = 0; i < hdb->init_generations; i++) s->gen.push_back(new Gen(s, i));
  if (init) s->init();
  hdb->slice.push_back(s);
  pthread_mutex_unlock(&hdb->mutex);
  return 0;
}

void Gen::complete_index_sync() {
  sync_part = 0;
  if (log_position) {
    // printf("complete_index_sync\n");
    header->index_serial++;
    log_position = 0;
    write_header(sync_header);
    snap_header();
  }
}

void Gen::periodic_sync() {
  pthread_mutex_lock(&mutex);
  WriteBuffer &w = wbuf[cur_write];
  if (w.start != w.cur && !w.writing) write_buffer();
  write_upto_index_part(sync_part + 1, 1);  // write one more
  if (sync_part >= index_parts) complete_index_sync();
  pthread_mutex_unlock(&mutex);
}

static void *sync_main(void *data) {
  HDB *hdb = (HDB *)data;
  struct timespec ts;
  ts.tv_sec = time(NULL);
  ts.tv_nsec = 0;
  pthread_mutex_lock(&hdb->mutex);
  while (1) {
    ts.tv_sec += SYNC_PERIOD;
    pthread_cond_timedwait(&hdb->sync_condition, &hdb->mutex, &ts);
    if (hdb->exiting) break;
    forv_Slice(s, hdb->slice) forv_Gen(g, s->gen) g->periodic_sync();
  }
  pthread_mutex_unlock(&hdb->mutex);
  return 0;
}

int HashDB::open(int aread_only) {
  HDB *hdb = ((HDB *)this);
  hdb->read_only = aread_only;
  assert(reinit_on_open_error == false);
  assert(hdb->separate_db_per_slice == true);
  assert(hdb->replication_factor == 0);
  if (!thread_pool) {
    hdb->thread_pool_max_threads = concurrency * hdb->slice.size();
    thread_pool = new ThreadPool(hdb->thread_pool_max_threads);
    hdb->thread_pool_allocated = 1;
  } else
    hdb->thread_pool_allocated = 0;
  int r = open_slices(hdb);
  if (!r) {
    assert(!hdb->sync_thread);
    hdb->sync_thread = ThreadPool::thread_create(sync_main, this);
  }
  return r;
}

int HashDB::reinitialize() {
  HDB *hdb = ((HDB *)this);
  return init_slices(hdb);
}

int Slice::read(uint64_t key, Vec<Extent> &hit) {
  int r = 0;
  forv_Gen(g, gen) r = g->read(key, hit) | r;
  return r;
}

int HashDB::read(uint64_t key, Vec<Extent> &hit) {
  HDB *hdb = ((HDB *)this);
  int r = 0;
  forv_Slice(s, hdb->slice) r = s->read(key, hit) | r;
  return 0;
}

int HashDB::next(uint64_t key, void *old_data, Vec<Extent> &hit) {
  HDB *hdb = ((HDB *)this);
  Data *d = PTR_TO_DATA(old_data);
  if (d->magic != DATA_MAGIC) return -1;
  if ((int)d->slice >= hdb->slice.size()) return -1;
  Slice *s = hdb->slice[d->slice];
  if ((int)d->gen >= s->gen.size()) return -1;
  Gen *g = s->gen[d->gen];
  int r = 0;
  pthread_mutex_lock(&g->mutex);
  r = g->next(key, d, hit);
  pthread_mutex_unlock(&g->mutex);
  return r;
}

int HashDB::next(uint64_t key, void *old_data, Callback *callback, bool immediate_miss) {
  assert(0);
  return 0;
}

struct Reader {
  Slice *s;
  uint64_t key;
  pthread_barrier_t *barrier;
  Vec<Extent> hit;
  int found;  // only for master (0)
  ssize_t result;
  Callback *callback;

  void init(Slice *as, uint64_t akey) {
    s = as;
    key = akey;
    hit.clear();
    result = 0;
  }
};

static void *do_read(Reader *r) {
  r->result = r->s->read(r->key, r->hit) | r->result;
  pthread_barrier_wait(r->barrier);
  return 0;
}

static void *do_read_callback(Reader *r) {
  pthread_barrier_t barrier;
  pthread_barrier_init(&barrier, NULL, r->found);
  for (int i = 0; i < r->found; i++) {
    r[i].barrier = &barrier;
    r->s->hdb->thread_pool->add_job((void *(*)(void *))do_read, (void *)&r[i]);
  }
  pthread_barrier_wait(&barrier);
  for (int i = 0; i < r->found; i++) r->callback->hit.insert(r->callback->hit.end(), r[i].hit.begin(), r[i].hit.end());
  r->callback->done(r->result);
  delete_aligned(r);
  return 0;
}

int HashDB::read(uint64_t key, Callback *callback, bool immediate_miss) {
  HDB *hdb = ((HDB *)this);
  int n = hdb->slice.size(), found = 0;
  std::vector<Reader> reader(n);
  for (int i = 0; i < n; i++) {
    if (hdb->slice[i]->might_exist(key)) reader[found++].init(hdb->slice[i], key);
  }
  if (immediate_miss && !found) return 1;
  int size = sizeof(Reader) * (found ? found : 1);
  Reader *areader = (Reader *)new_aligned(size);
  for (int i = 0; i < found; i++) areader[i] = reader[i];
  areader[0].s = hdb->slice[0];
  areader[0].found = found;
  areader[0].callback = callback;
  areader[0].result = !found;
  hdb->thread_pool->add_job((void *(*)(void *))do_read_callback, (void *)&areader[0]);
  return 0;
}

int Slice::might_exist(uint64_t key) {
  int buckets = gen[0]->buckets;
  int b = key % buckets;
  uint16_t tag = KEY2TAG(key);
  forv_Gen(g, gen) {
    pthread_mutex_lock(&g->mutex);
    foreach_contiguous_element(g, e, b, tmp) {
      Index *i = g->index(e);
      if (i->tag == tag && i->size) return 1;
    }
    foreach_overflow_element(g, e, b, tmp) {
      Index *i = g->index(e);
      if (i->tag == tag && i->size) return 1;
    }
    pthread_mutex_unlock(&g->mutex);
  }
  return 0;
}

struct Writer {
  Slice *s;
  uint64_t *key;
  int nkeys;
  Marshal *marshal;
  void *old_data;
  pthread_barrier_t *barrier;
  ssize_t result;
  HashDB::Callback *callback;

  void init(Slice *as, uint64_t *akey, int ankeys, Marshal *amarshal, pthread_barrier_t *abarrier) {
    s = as;
    key = akey;
    nkeys = ankeys;
    marshal = amarshal;
    barrier = abarrier;
    result = 0;
  }
};

static void append_footer(WriteBuffer *b) {
  assert(b->cur + FOOTER_SIZE <= b->end);
  Data *dlast = (Data *)b->last;
  Data *d = (Data *)b->cur;
  memset(d, 0, FOOTER_SIZE);
  d->magic = DATA_MAGIC;
  d->write_serial = dlast->write_serial;
  uint32_t o = (b->offset + (b->cur - b->start) - b->gen->data_offset) / DATA_BLOCK_SIZE;
  d->slice = b->gen->slice->islice;
  d->gen = b->gen->igen;
  d->reserved1 = 0;
  d->offset = o;
  ((&d->offset)[1]) = 0;  // clear flags
  d->padding = 1;
  d->nkeys = 0;
  d->length = FOOTER_SIZE - sizeof(Data) - sizeof(DataFooter);
  d->size = 1;
  b->cur += FOOTER_SIZE;
}

static void write_padding(WriteBuffer *b) {
  uint64_t left = b->gen->data_size - b->pad_position;
  Data *dlast = (Data *)b->last;
  Data *d = (Data *)new_aligned(left);
  memset(d, 0, left);
  d->magic = DATA_MAGIC;
  d->write_serial = dlast->write_serial;
  d->slice = b->gen->slice->islice;
  d->gen = b->gen->igen;
  d->reserved1 = 0;
  ((&d->offset)[1]) = 0;  // clear flags
  d->padding = 1;
  d->nkeys = 0;
  uint32_t s = length_to_size(left);
  uint32_t o = b->pad_position / DATA_BLOCK_SIZE;
  // multiple size pads to hit the end exactly
  for (uint32_t i = 0; i <= (s >> 8); i++) {
    if (!left) break;
    uint64_t still_left = 0;
    if (size_to_length(s) != left) {
      uint32_t new_s = ((s >> 8) << 8) + ((s & 255) - 1);
      still_left = left;
      left = size_to_length(new_s);
      assert(left < still_left);
      still_left -= left;
    }
    d->offset = o;
    d->length = left - sizeof(Data) - sizeof(DataFooter);
    d->size = length_to_size(left);
    if (b->gen->pwrite(b->gen->slice->fd, b->start, left, b->pad_position + b->gen->data_offset) < 0)
      b->gen->hdb()->err("write error, errno %d slice %d generation %d offset %lld length %d", errno,
                         b->gen->slice->islice, b->gen->igen, b->pad_position + b->gen->data_offset, left);
    o += left;
    left = still_left;
    s = length_to_size(left);
  }
  delete_aligned(d);
}

static void *do_write_buffer(WriteBuffer *b) {
  Gen *g = b->gen;
#ifdef HELGRIND
  pthread_mutex_lock(&g->mutex);  // for halgrind
#endif
  // producer-consumer, no races
  ssize_t r = b->gen->pwrite(g->slice->fd, b->start, b->cur - b->start, b->offset);
  int padding = b->pad_position;
#ifdef HELGRIND
  pthread_mutex_unlock(&g->mutex);  // for halgrind
#endif
  if (padding) write_padding(b);
  pthread_mutex_lock(&g->mutex);
  if (r < 0)
    g->hdb()->err("write error, errno %d slice %d generation %d offset %lld length %d", errno, g->slice->islice,
                  g->igen, b->offset, b->cur - b->start);
  b->result = r;
  // printf("write buffer absolute write position = %lld, wserial = %lld, phase = %d, offset = %d\n",
  // b->committed_write_position + g->data_offset, b->committed_write_serial, b->phase, (int)((b->offset -
  // b->gen->data_offset)/512));
  g->committed_write_position = b->committed_write_position;
  g->committed_write_serial = b->committed_write_serial;
  g->committed_phase = b->committed_phase;
  g->commit_buffer(b->start, b->cur);
  b->cur = b->start;
  b->offset = b->next_offset;
  b->phase = b->next_phase;
  b->next_offset = std::numeric_limits<unsigned long long>::max();  // short circuit test in Gen::read_data
  b->writing = 0;
  pthread_cond_broadcast(&g->write_condition);
  pthread_mutex_unlock(&g->mutex);
  return 0;
}

void Gen::write_buffer(int force_wrap) {
  WriteBuffer &w = wbuf[cur_write];
  assert(!w.writing);
  w.writing = 1;
  w.pad_position = 0;
  append_footer(&w);
  uint64_t new_write_position = header->write_position + (w.cur - w.start);
  assert(new_write_position <= data_size);
  if (new_write_position > data_size || force_wrap) {
    if (header->write_position < data_size) w.pad_position = header->write_position;
    header->phase = (header->phase + 1) & 1;
    header->write_position = 0;
    for (int s = 0; s < sectors(); s++) clean_sector(s);
  } else
    header->write_position = new_write_position;
  uint64_t done = MODULAR_DIFFERENCE(header->write_position, sync_header->write_position, data_size);
  int parts_done = done / (data_size / index_parts);
  // printf("done %lld part_size %lld parts_done %d sync_part %d\n", done, data_size / index_parts, parts_done,
  // sync_part);
  w.committed_write_position = header->write_position;
  w.committed_write_serial = header->write_serial;
  w.committed_phase = header->phase;
  int prev_write = (cur_write + (WRITE_BUFFERS - 1)) % WRITE_BUFFERS;
  cur_write = (cur_write + 1) % WRITE_BUFFERS;
  WriteBuffer &p = wbuf[prev_write], &n = wbuf[cur_write];
  if (n.writing) {
    n.next_offset = data_offset + header->write_position;
    n.next_phase = header->phase;
  } else {
    n.offset = data_offset + header->write_position;
    n.phase = header->phase;
  }
  while (p.writing && cmp_wpos(w.offset, w.phase, p.offset, p.phase) >= 0)
    pthread_cond_wait(&p.gen->write_condition, &p.gen->mutex);
  if (parts_done > sync_part) write_upto_index_part(parts_done);
  slice->hdb->thread_pool->add_job((void *(*)(void *))do_write_buffer, (void *)&w);
  return;
}

static void add_log_header_footer(WriteBuffer *b) {
  LogHeaderFooter *h = (LogHeaderFooter *)b->start;
  LogHeaderFooter *f = (LogHeaderFooter *)b->cur;
  b->cur += LOG_FOOTER_SIZE;
  assert(b->cur <= b->end);
  uint32_t l = b->cur - b->start;
  memset(h, 0, sizeof(LogHeaderFooter));
  h->magic = LOG_MAGIC;
  Gen *g = b->gen;
  h->write_position = g->committed_write_position;
  h->write_serial = g->committed_write_serial;
  h->initial_phase = g->sync_header->phase;
  h->last_write_position = g->sync_header->write_position;
  h->last_write_serial = g->sync_header->write_serial;
  h->final_phase = g->header->phase;
  h->length = l;
  // footer
  memcpy(f, h, sizeof(LogHeaderFooter));
  // padding
  uint32_t s = ROUND_TO(l, DATA_BLOCK_SIZE);
  if (s - l) memset(b->cur, 0, s - l);
  b->cur += s - l;  // pad out
}

static void *do_write_log_buffer(WriteBuffer *b) {
  Gen *g = b->gen;
#ifdef HELGRIND
  pthread_mutex_lock(&g->mutex);  // for halgrind
#endif
  b->offset = g->log_offset[g->log_phase()] + g->log_position;
  b->result = b->gen->pwrite(g->slice->fd, b->start, b->cur - b->start, b->offset);
  if (b->result < 0)
    g->hdb()->err("write error, errno %d slice %d generation %d offset %lld length %d", errno, g->slice->islice,
                  g->igen, b->offset, b->cur - b->start);
    // printf("wrote log %lld %d \n", b->offset, (int)(b->cur - b->start));
#ifndef HELGRIND
  pthread_mutex_lock(&g->mutex);
#endif
  g->log_position += b->cur - b->start;
  b->writing = 0;
  b->cur = b->start + sizeof(LogHeaderFooter);
  pthread_cond_broadcast(&g->write_condition);
  pthread_mutex_unlock(&g->mutex);
  return 0;
}

void Gen::write_log_buffer() {
  WriteBuffer &b = lbuf[cur_log];
  assert(b.start != b.cur && !b.writing);
  b.writing = 1;
  if (log_position + (b.cur - b.start) + LOG_FOOTER_SIZE > log_size) {
    hdb()->warn("out of log space, completing index sync");
    write_upto_index_part(index_parts);
    complete_index_sync();
  }
  add_log_header_footer(&b);
  uint32_t prev_log_write = (cur_log + (LOG_BUFFERS - 1)) % LOG_BUFFERS;
  wait_for_write_to_complete(&lbuf[prev_log_write]);  // log writes initiated in order
  slice->hdb->thread_pool->add_job((void *(*)(void *))do_write_log_buffer, (void *)&b);
  cur_log = (cur_log + 1) % LOG_BUFFERS;
}

int Gen::delete_bucket_element(int e, int b) {
  Index *i = index(e);
  debug_log_it(0, i, DEBUG_LOG_DELETE);
  int s = bucket_to_sector(b);
  dirty_sector(s);
  if (overflow_present(b)) {
    int n = overflow_head(b);
    int ee = overflow_element(s, n);
    copy_index(i, index(ee));
    return delete_overflow_element(ee, -1, b);
  } else
    i->size = 0;  // just blow it away
  return 0;
}

int Gen::delete_overflow_element(int e, int p, int b) {
  int s = bucket_to_sector(b);
  Index *i = index(e);
  debug_log_it(0, i, DEBUG_LOG_DELETE);
  int r = -1;
  dirty_sector(s);
  if (p == -1) {  // first element
    int n = (int)overflow_element(s, i->next);
    if (e == n)  // end of list, only element
      overflow_present(b) = 0;
    else {
      r = n;
      overflow_head(b) = i->next;
    }
  } else {
    int n = (int)overflow_element(s, i->next);
    if (e == n)  // end of list
      index(p)->next = overflow_element_number(p);
    else {
      r = n;
      index(p)->next = i->next;
    }
  }
  if (!freelist_present(s)) {
    freelist_present(s) = 1;
    i->next = overflow_element_number(e);
  } else {
    i->next = freelist_head(s);
  }
  freelist_head(s) = overflow_element_number(e);
  return r;
}

void Gen::set_element(Index *i, uint64_t key, bool phase, uint32_t size, uint32_t offset) {
  i->offset = offset;
  i->phase = phase;
  i->size = size;
  assert(i->size);
  i->tag = KEY2TAG(key);
  debug_log_it(key, i, DEBUG_LOG_SET);
}

void Gen::clean_sector(int s) {
  for (int b = sector_to_bucket(s); b < sector_to_bucket(s) + BUCKETS_PER_SECTOR; b++) {
    foreach_contiguous_element(this, e, b, tmp) if (verify_offset(this, index(e))) delete_bucket_element(e, b);
  Lagain:
    int l = -1;
    foreach_overflow_element(this, e, b, p) {
      if (l == e) {
        hdb()->err("breaking cyclic overflow");
        index(p)->next = overflow_element_number(p);
      }
      if (l == -1) l = e;
      if (verify_offset(this, index(e))) {
        if ((e = delete_overflow_element(e, p, b)) < 0) break;
        goto Lagain;
      }
    }
  }
}

void Gen::clean_index_part(int p) {
  int s = element_to_sector((INDEX_BYTES_PER_PART * p) / sizeof(Index));
  int ss = element_to_sector((INDEX_BYTES_PER_PART * (p + 1)) / sizeof(Index));
  if (ss > sectors()) ss = sectors();
  for (int i = s; i < ss; i++) clean_sector(s);
}

int Gen::check_data(Data *d, uint64_t o, uint64_t l, uint32_t offset, int recovery) {
  if (d->magic != DATA_MAGIC) {
    if (!recovery) hdb()->err("bad data magic, slice %d generation %d offset %lld", slice->islice, igen, o);
    return -1;
  }
  if (((char *)(&d->chain[d->nkeys])) > ((char *)d) + l) {
    if (!recovery) hdb()->err("off the end, slice %d generation %d offset %lld", slice->islice, igen, o);
    return -1;
  }
  if (d->length > l - (((char *)&d->chain[d->nkeys]) - ((char *)d))) {
    if (!recovery) hdb()->err("too small, slice %d generation %d offset %lld", slice->islice, igen, o);
    return -1;
  }
  if (d->offset != offset) {
    if (!recovery)
      hdb()->err("bad data offset, slice %d generation %d offset %lld, data offset %d, offset %d", slice->islice, igen,
                 o, d->offset, offset);
    return -1;
  }
  if ((int)d->gen != igen) {
    if (!recovery)
      hdb()->err("bad data gen, slice %d generation %d offset %lld, data gen %d", slice->islice, igen, o, d->gen);
    return -1;
  }
  if ((int)d->slice != slice->islice) {
    if (SLICE_INDEX_MISMATCH_RESULT) return 1;
    hdb()->warn("unexpected data slice index, slice %d generation %d offset %lld, data slice index %d", slice->islice,
                igen, o, d->slice);
  }
  return 0;
}

void Gen::commit_data(Data *d) {
  for (uint32_t i = 0; i < d->nkeys; i++) {
    if (d->remove) {
      Index *index = (Index *)DATA_TO_PTR(d);
      if (delete_lookaside(d->chain[i].key, index))
        delete_key(d->chain[i].key, index->phase, index->size, index->offset);
    } else {
      Index index(d->offset, 0, d->size, 0, d->phase);
      if (delete_lookaside(d->chain[i].key, &index)) {
        if (d->chain[i].next.size)  // !empty
          if (verify_offset(this, &d->chain[i].next) >= 0)
            delete_key(d->chain[i].key, d->chain[i].next.phase, d->chain[i].next.size, d->chain[i].next.offset);
        if (find_key(d->chain[i].key, d->phase, d->size, d->offset))
          insert_key(d->chain[i].key, d->phase, d->size, d->offset);
      }
    }
  }
}

void Gen::commit_log_entry(LogEntry *e) {
  Index *index = e->index();
  uint64_t *keys = e->keys();
  for (uint32_t i = 0; i < e->nkeys; i++) {
    if (index->next) {  // remove
      delete_key(keys[i], index->phase, index->size, index->offset);
    } else {
      if (hdb()->chain_collisions) delete_collision(keys[i]);
      if (find_key(keys[i], index->phase, index->size, index->offset) < 0)
        insert_key(keys[i], index->phase, index->size, index->offset);
    }
  }
}

void Gen::commit_buffer(uint8_t *start, uint8_t *end) {
  uint8_t *b = start;
  while (b < end) {
    Data *d = (Data *)b;
    int len = size_to_length(d->size);
    commit_data(d);
    b += len;
  }
}

int Gen::find_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset) {
  int b = ((uint32_t)key) % buckets;
  uint16_t tag = KEY2TAG(key);
  foreach_contiguous_element(this, e, b, tmp) {
    Index *i = index(e);
    if (i->tag == tag && i->phase == phase && i->size == size && i->offset == offset) return e;
  }
  foreach_overflow_element(this, e, b, tmp) {
    Index *i = index(e);
    if (i->tag == tag && i->phase == phase && i->size == size && i->offset == offset) return e;
  }
  return -1;
}

void Gen::insert_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset) {
  debug_log_it(key, 0, DEBUG_LOG_INS_KEY);
  int b = ((uint32_t)key) % buckets;
  int s = bucket_to_sector(b);
  int retry = 0;
  dirty_sector(s);
  {
  Lretry:
    foreach_contiguous_element(this, e, b, tmp) {
      Index *i = index(e);
      if (!i->size) {
        set_element(i, key, phase, size, offset);
        return;
      }
    }
    if (!freelist_present(s)) {
      if (retry) {
        // yes this can leave cruft around, but it should be rare and reads are verified
        hdb()->err("freelist empty, slice %d generation %d sector %d", slice->islice, igen, s);
        return;
      } else {
        clean_sector(s);
        retry = 1;
        goto Lretry;
      }
    }
  }
  int f = freelist_head(s);
  int e = overflow_element(s, f);
  Index *i = index(e);
  int n = i->next;
  if (n == f)
    freelist_present(s) = 0;
  else
    freelist_head(s) = n;
  int nn = 0;
  if (overflow_present(b)) {
    nn = overflow_head(b);
  } else {
    overflow_present(b) = 1;
    nn = f;  // indicates end-of-list
  }
  overflow_head(b) = f;
  set_element(i, key, phase, size, offset);
  i->next = nn;
  return;
}

void Gen::delete_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset) {
  debug_log_it(key, 0, DEBUG_LOG_DEL_KEY);
  int b = ((uint32_t)key) % buckets;
  int s = bucket_to_sector(b);
  uint16_t tag = KEY2TAG(key);
  foreach_contiguous_element(*this, e, b, tmp) {
    Index *i = index(e);
    if (i->offset == offset && i->tag == tag && i->size == size && i->phase == phase) {
      if (overflow_present(b)) {
        int n = overflow_head(b);
        int ee = overflow_element(s, n);
        copy_index(i, index(ee));
        delete_overflow_element(ee, -1, b);
      } else
        i->size = 0;  // just blow it away
      goto Ldeleted;
    }
  }
  foreach_overflow_element(this, e, b, p) {
    Index *i = index(e);
    if (i->offset == offset && i->tag == tag && i->size == size && i->phase == phase) {
      delete_overflow_element(e, p, b);
      goto Ldeleted;
    }
  }
  return;
Ldeleted:
  dirty_sector(s);
  return;
}

void Gen::delete_collision(uint64_t key) {
  int b = ((uint32_t)key) % buckets;
  int s = bucket_to_sector(b);
  uint16_t tag = KEY2TAG(key);
  foreach_contiguous_element(*this, e, b, tmp) {
    Index *i = index(e);
    if (i->tag == tag && i->size) {
      if (overflow_present(b)) {
        int n = overflow_head(b);
        int ee = overflow_element(s, n);
        copy_index(i, index(ee));
        delete_overflow_element(ee, -1, b);
      } else
        i->size = 0;  // just blow it away
      goto Ldeleted;
    }
  }
  foreach_overflow_element(this, e, b, p) {
    Index *i = index(e);
    if (i->tag == tag && i->size) {
      delete_overflow_element(e, p, b);
      goto Ldeleted;
    }
  }
  return;
Ldeleted:
  dirty_sector(s);
  return;
}

void Gen::reserve_log_space(int nkeys) {
  int l = sizeof(LogEntry) + sizeof(Index) + sizeof(uint64_t) * nkeys;
Lagain:
  uint64_t wpos = header->write_position;
  int phase = header->phase;
  WriteBuffer *b = &lbuf[cur_log];
  if (b->writing) {
    wait_for_write_to_complete(b);
    goto Lagain;
  }
  assert(l + LOG_FOOTER_SIZE <= log_buffer_size);
  if (b->cur + l + LOG_FOOTER_SIZE >= b->end) {
    wait_for_write_commit(this, wpos, phase);
    write_log_buffer();
    goto Lagain;
  }
}

void Gen::insert_log(uint64_t *key, int nkeys, Index *i) {
  int l = sizeof(LogEntry) + sizeof(Index) + sizeof(*key) * nkeys;
  WriteBuffer *b = &lbuf[cur_log];
  assert(b->cur + l + LOG_FOOTER_SIZE < b->end);
  LogEntry *le = (LogEntry *)b->cur;
  uint64_t *e = (uint64_t *)b->cur;
  le->reserved = 0;
  le->nkeys = nkeys;
  int x = sizeof(LogEntry) / sizeof(uint64_t);
  memcpy(&e[x], i, sizeof(Index));
  x += sizeof(Index) / sizeof(uint64_t);
  memcpy(&e[x], key, nkeys * sizeof(*key));
  assert(l == ((uint8_t *)&e[x + nkeys]) - b->cur);
  b->cur += l;
}

void Gen::insert_lookaside(uint64_t key, Index *i) {
  Lookaside l;
  l.key = key;
  l.index = *i;
  lookaside.put(l);
  debug_log_it(key, i, DEBUG_LOG_INSERT_LA);
}

int Gen::delete_lookaside(uint64_t key, Index *i) {
  Lookaside l;
  l.key = key;
  l.index = *i;
  int r = lookaside.del(l);
  if (r) debug_log_it(key, i, DEBUG_LOG_DELETE_LA);
  return r;
}

int Gen::write(uint64_t *key, int nkeys, Marshal *marshal) {
  uint64_t len = marshal->marshal_size();
  uint64_t hsize = sizeof(Data) + sizeof(DataFooter) + (nkeys - 1) * sizeof(uint64_t);
  uint32_t size = length_to_size(len + hsize);
  uint64_t l = size_to_length(size);
  WriteBuffer *b = get_buffer(nkeys, l);
  debug_log_it(*key, 0, DEBUG_LOG_WRITE);
  if (!b) return -1;
  Data *d = (Data *)b->cur;
  d->magic = DATA_MAGIC;
  d->write_serial = header->write_serial++;
  uint32_t o = (b->offset + (b->cur - b->start) - data_offset) / DATA_BLOCK_SIZE;
  d->slice = slice->islice;
  d->gen = igen;
  d->reserved1 = 0;
  d->offset = o;
  ((&d->offset)[1]) = 0;  // clear flags
  d->phase = b->phase;
  d->nkeys = nkeys;
  memset(d->chain, 0, nkeys * sizeof(KeyChain));
  for (int i = 0; i < nkeys; i++) d->chain[i].key = key[i];
  DATA_TO_FOOTER(d)->nkeys = nkeys;
  char *target = (char *)(b->cur + hsize);
  uint64_t actual_len = marshal->marshal(target);
  if (l != actual_len + hsize) memset(target + actual_len, 0, l - (actual_len + hsize));
  d->length = actual_len;
  d->size = size;
  if (hdb()->chain_collisions) chain_keys_for_write(d);
  Index index(d->offset, 0, d->size, 0, d->phase);
  insert_log(key, nkeys, &index);
  for (int i = 0; i < nkeys; i++) insert_lookaside(key[i], &index);
  b->last = b->cur;
  b->cur += l;
  return 0;
}

void Gen::chain_keys_for_write(Data *d) {
  for (uint32_t i = 0; i < d->nkeys; i++) {
    Vec<Index> rd;
    find_indexes(d->chain[i].key, rd);
    if (rd.size()) d->chain[i].next = rd[0];
  }
}

int Gen::write_remove(uint64_t *key, int nkeys, Index *i) {
  uint64_t hsize = sizeof(Data) + sizeof(DataFooter) + (nkeys - 1) * sizeof(uint64_t);
  uint32_t size = length_to_size(hsize);
  uint64_t l = size_to_length(size);
  WriteBuffer *b = get_buffer(nkeys, l);
  if (!b) return -1;
  Data *d = (Data *)b->cur;
  d->magic = DATA_MAGIC;
  d->write_serial = header->write_serial++;
  uint32_t o = (b->offset + (b->cur - b->start) - data_offset) / DATA_BLOCK_SIZE;
  d->slice = slice->islice;
  d->gen = igen;
  d->reserved1 = 0;
  d->offset = o;
  ((&d->offset)[1]) = 0;  // clear flags
  d->remove = 1;
  d->phase = b->phase;
  d->nkeys = nkeys;
  assert(nkeys);
  memset(d->chain, 0, nkeys * sizeof(KeyChain));
  for (int j = 0; j < nkeys; j++) d->chain[j].key = key[j];
  DATA_TO_FOOTER(d)->nkeys = nkeys;
  *(Index *)DATA_TO_PTR(d) = *i;
  char *target = (char *)(b->cur + hsize + sizeof(Index));
  if (l != hsize + sizeof(Index)) memset(target, 0, l - hsize - sizeof(Index));
  d->length = sizeof(Index);
  d->size = size;
  b->last = b->cur;
  b->cur += l;
  return 0;
}

int Gen::read_element(Index *i, uint64_t key, Vec<Extent> &hit) {
  Data *d = read_data(i);
  if (d == BAD_DATA) return -1;
  if (!d) return 0;
  for (uint32_t x = 0; x < d->nkeys; x++) {
    if (d->chain[x].key == key) {
      hit.push_back(Extent());
      Extent &e = hit.back();
      e.data = DATA_TO_PTR(d);
      e.len = d->length;
      return 0;
    }
  }
  delete_aligned(d);
  return 0;
}

Data *Gen::read_data(Index *i) {
  uint32_t l = size_to_length(i->size);
  uint64_t o = data_offset + ((uint64_t)i->offset) * DATA_BLOCK_SIZE;
  void *buf = new_aligned(l);
  for (int x = 0; x < WRITE_BUFFERS; x++) {
    if (wbuf[x].offset <= o && o + l <= wbuf[x].offset + (wbuf[x].cur - wbuf[x].start)) {
      memcpy(buf, wbuf[x].start + (o - wbuf[x].offset), l);
      goto Lfound;
    }
  }
  if (verify_offset(this, i)) {
    if (STALE_INDEX_RESULT)
      hdb()->warn("state index entry, slice %d generation %d offset %lld", slice->islice, igen, o);
    goto Lreturn;
  }
  pthread_mutex_unlock(&mutex);
  if (pread(slice->fd, buf, l, (off_t)o) != l) {
    delete_aligned(buf);
    pthread_mutex_lock(&mutex);
    return (Data *)BAD_DATA;
  }
  pthread_mutex_lock(&mutex);
  if (verify_offset(this, i)) goto Lreturn;
Lfound : {
  Data *d = (Data *)buf;
  if (d->remove) goto Lreturn;
  if (check_data(d, o, l, i->offset)) goto Lreturn;
  if (!SLICE_INDEX_MISMATCH_RESULT)  // fixup slice number
    d->slice = slice->islice;
  return d;
}
Lreturn:
  delete_aligned(buf);
  return 0;
}

void Gen::find_indexes(uint64_t key, Vec<Index> &rd) {
  int b = ((uint32_t)key) % buckets;
  uint16_t tag = KEY2TAG(key);
  Vec<Index> del;
  unsigned int h = ((uint32_t)(key >> 32) ^ ((uint32_t)key));
  Lookaside *la = &lookaside.v[(h % lookaside.v.size()) * 4];
  for (int a = 0; a < 4; a++) {
    if (key == la[a].key) {
      if (!la[a].index.next)
        rd.push_back(la[a].index);
      else
        del.push_back(la[a].index);
    }
  }
  if (!rd.size() || !hdb()->chain_collisions) {
    foreach_contiguous_element(this, e, b, tmp) {
      Index *i = index(e);
      if (i->tag != tag || !i->size) continue;
      unsigned int x = 0;
      for (; x < del.size(); x++)
        if (del[x].offset == i->offset && del[x].phase == i->phase) break;
      if (x == del.size()) rd.push_back(*i);
    }
    foreach_overflow_element(this, e, b, tmp) {
      Index *i = index(e);
      if (i->tag != tag || !i->size) continue;
      unsigned int x = 0;
      for (; x < del.size(); x++)
        if (del[x].offset == i->offset && del[x].phase == i->phase) break;
      if (x == del.size()) rd.push_back(*i);
    }
  }
}

int Gen::read(uint64_t key, Vec<Extent> &hit) {
  int r = 0;
  Vec<Index> rd;
  pthread_mutex_lock(&mutex);
  find_indexes(key, rd);
  if (!rd.size()) r = 2;
  for (size_t x = 0; x < rd.size(); x++) r = read_element(&rd[x], key, hit) | r;
  pthread_mutex_unlock(&mutex);
  return r;
}

int Gen::next(uint64_t key, Data *d, Vec<Extent> &hit) {
  int r = 0;
  for (uint32_t i = 0; i < d->nkeys; i++) {
    if (d->chain[i].key == key && d->chain[i].next.size) {
      pthread_mutex_lock(&mutex);
      r = read_element(&d->chain[i].next, key, hit) | r;
      pthread_mutex_unlock(&mutex);
      return r;
    }
  }
  return 0;
}

int Slice::write(uint64_t *key, int nkeys, Marshal *marshal, Callback *callback) {
  Gen *g = gen[0];
  pthread_mutex_lock(&g->mutex);
  int res = g->write(key, nkeys, marshal);
  if (!res) {
    if (callback == HASHDB_SYNC || callback == HASHDB_FLUSH) {
      WriteBuffer *b = &g->wbuf[g->cur_write];
      if (callback == HASHDB_FLUSH) {
        g->write_buffer();
        wait_for_write_to_complete(b);
      } else
        wait_for_flush(b, hdb->sync_wait_msec);
    } else if (callback != HASHDB_ASYNC) {
    }
  }
  pthread_mutex_unlock(&g->mutex);
  return res;
}

#ifdef INCOMPLETE_REPLICATION_CODE
static void *do_write(Writer *w) {
  w->result = w->s->write(w->key, w->nkeys, w->marshal) | w->result;
  pthread_barrier_wait(w->barrier);
  return 0;
}
#endif

static void *do_write_callback(Writer *w) {
#ifdef INCOMPLETE_REPLICATION_CODE
  int r = w->s->hdb->replication_factor;
  if (!r) {
  } else {
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, r);
    for (int i = 0; i < r; i++) {
      w[i].init(w->s->hdb->slice[i], w->key, w->nkeys, w->marshal, &barrier);
      w->s->hdb->thread_pool->add_job((void *(*)(void *))do_write, (void *)&w[i]);
    }
    pthread_barrier_wait(&barrier);
  }
#endif
  w->result = w->s->write(w->key, w->nkeys, w->marshal, w->callback->flush ? HASHDB_FLUSH : HASHDB_SYNC);
  w->callback->done(w->result);
  delete_aligned(w);
  return 0;
}

int HashDB::write(uint64_t *key, int nkeys, Marshal *marshal, Callback *callback) {
  HDB *hdb = ((HDB *)this);
#ifdef HELGRIND
  pthread_mutex_lock(&hdb->mutex);
#endif
  int s = hdb->current_write_slice;
  hdb->current_write_slice = s + 1;  // race is OK, just looking for distribution
#ifdef HELGRIND
  pthread_mutex_unlock(&hdb->mutex);
#endif
  if (SPECIAL_CALLBACK(callback)) return hdb->slice[s % hdb->slice.size()]->write(key, nkeys, marshal, callback);
#ifdef INCOMPLETE_REPLICATION_CODE
  {
    int r = 0;
    assert(!"replication not implemented");
    if (callback == ASYNC) {
      for (int i = 0; i < r; i++)  // incomplete
        r = hdb->slice[(s + i) % hdb->slice.size()]->write(key, nkeys, marshal) | r;
    } else {
      barrier_t barrier;
      std::vector<Writer> writer(r);
      barrier_init(&barrier, n);
      for (int i = 0; i < r; i++) {  // incomplete
        writer[i].init(hdb->slice[(s + i) % hdb->slice.size()], key, nkeys, marshal, &barrier);
        hdb->thread_pool->add_job((void *(*)(void *))do_write, (void *)&writer[i]);
      }
      barrier_wait(&barrier);
      r = writer[0].result;
    }
    return r;
  }
#endif
  int nn = 1;
  int size = sizeof(Writer) * nn;
  Writer *awriter = (Writer *)new_aligned(size);
  awriter[0].s = hdb->slice[s];
  awriter[0].callback = callback;
  awriter[0].key = key;
  awriter[0].nkeys = nkeys;
  awriter[0].marshal = marshal;
  hdb->thread_pool->add_job((void *(*)(void *))do_write_callback, (void *)&awriter[0]);
  return 0;
}

static inline void wait_for_log_flush(Gen *g, int wait_msec) {
  uint64_t wpos = g->header->write_position;
  int phase = g->header->phase;
  auto startt = std::chrono::high_resolution_clock::now();
  auto donet = startt + std::chrono::milliseconds(wait_msec);
  while (std::chrono::high_resolution_clock::now() < donet && cmp_wpos(wpos, phase, g->committed_write_position, g->committed_phase) < 0) {
    struct timespec ts;
    ts.tv_sec = wait_msec / 1000;
    ts.tv_nsec = (wait_msec % 1000) * 1000000;
    pthread_cond_timedwait(&g->write_condition, &g->mutex, &ts);
  }
  if (cmp_wpos(wpos, phase, g->committed_write_position, g->committed_phase) < 0) {
    wait_for_write_commit(g, g->header->write_position, g->header->phase);
    g->write_log_buffer();
  }
}

static void *do_remove_callback(Writer *w) {
  w->result = w->s->hdb->remove(w->old_data, w->callback->flush ? HASHDB_FLUSH : HASHDB_SYNC);
  w->callback->done(w->result);
  delete_aligned(w);
  return 0;
}

int HashDB::remove(void *old_data, Callback *callback) {
  HDB *hdb = ((HDB *)this);
  Data *d = PTR_TO_DATA(old_data);
  if (d->magic != DATA_MAGIC) return -1;
  if ((int)d->slice >= hdb->slice.size()) return -1;
  Slice *s = hdb->slice[d->slice];
  if ((int)d->gen >= s->gen.size()) return -1;
  if (SPECIAL_CALLBACK(callback)) {
    Gen *g = s->gen[d->gen];
    int r = 0;
    pthread_mutex_lock(&g->mutex);
    Index dindex(d->offset, 0, d->size, 1, d->phase);
    Index iindex(d->offset, 0, d->size, 0, d->phase);
    std::vector<uint64_t> keys(d->nkeys);
    for (uint32_t j = 0; j < d->nkeys; j++) keys[j] = d->chain[j].key;
    r = g->write_remove(keys.data(), d->nkeys, &dindex) | r;
    for (int i = 0; i < (int)d->nkeys; i++)
      if (!g->delete_lookaside(keys[i], &iindex)) g->insert_lookaside(keys[i], &dindex);
    g->insert_log(keys.data(), d->nkeys, &dindex);
    if (callback) {
      if (callback == HASHDB_FLUSH) {
        wait_for_write_commit(g, g->header->write_position, g->header->phase);
        g->write_log_buffer();
      } else
        wait_for_log_flush(g, hdb->sync_wait_msec);
    }
    pthread_mutex_unlock(&g->mutex);
    return r;
  }
  int nn = 1;
  int size = sizeof(Writer) * nn;
  Writer *awriter = (Writer *)new_aligned(size);
  awriter[0].s = s;
  awriter[0].callback = callback;
  awriter[0].old_data = old_data;
  hdb->thread_pool->add_job((void *(*)(void *))do_remove_callback, (void *)&awriter[0]);
  return 0;
}

int HashDB::get_keys(void *old_data, Vec<uint64_t> &keys) {
  Data *d = PTR_TO_DATA(old_data);
  if (d->magic == DATA_MAGIC) return -1;
  for (int i = 0; i < (int)d->nkeys; i++) keys.push_back(d->chain[i].key);
  return 0;
}

int HashDB::verify() {
  HDB *hdb = ((HDB *)this);
  return verify_slices(hdb);
}

int HashDB::dump_debug() {
  HDB *hdb = ((HDB *)this);
  forv_Slice(s, hdb->slice) {
    forv_Gen(g, s->gen) {
      printf("Slice %d Gen %d\n", s->islice, g->igen);
      g->dump_debug_log();
    }
  }
  return 0;
}

int HashDB::close() {
  HDB *hdb = ((HDB *)this);
  pthread_mutex_lock(&hdb->mutex);
  if (hdb->sync_thread) {
    hdb->exiting = 1;
    pthread_cond_signal(&hdb->sync_condition);
    pthread_mutex_unlock(&hdb->mutex);
    pthread_join(hdb->sync_thread, 0);
    hdb->sync_thread = 0;
  } else
    pthread_mutex_unlock(&hdb->mutex);
  int res = close_slices(hdb);
  pthread_mutex_lock(&hdb->mutex);
  forv_Slice(s, hdb->slice) {
    s->gen.clear();  // no-race here
    ::close(s->fd);
    if (s->pathname != s->layout_pathname) free(s->pathname);
    s->pathname = 0;
    free(s->layout_pathname);
    s->layout_pathname = 0;
    s->fd = -1;
  }
  hdb->free_slices();
  if (hdb->thread_pool_allocated) DELETE(thread_pool);
  thread_pool = 0;
  pthread_mutex_unlock(&hdb->mutex);
  return res;
}

int HashDB::free_chunk(void *ptr) {
  Data *p = PTR_TO_DATA(ptr);
  delete_aligned(p);
  return 0;
}

/* Test functions accessing internal data
 */
void hashdb_print_info(HashDB *dd) {
  HDB *d = (HDB *)dd;
  forv_Slice(slice, d->slice) {
    forv_Gen(g, slice->gen) {
      printf("Slice %d Gen %d size %lu phase %d\n", slice->islice, g->igen, g->header->size, g->header->phase);
    }
  }
}

uint64_t hashdb_write_position(HashDB *dd) {
  HDB *d = (HDB *)dd;
  return d->slice[0]->gen[0]->header->write_position;
}

void hashdb_index_fullness(HashDB *dd) {
  HDB *hdb = (HDB *)dd;
  int bcount[9] = {0};
  int ocount[17] = {0};
  int fcount[17] = {0};
  int lacount = 0;
  int x = 0;
  forv_Slice(slice, hdb->slice) {
    forv_Gen(g, slice->gen) {
      for (uint32_t b = 0; b < g->buckets; b++) {
        x = 0;
        foreach_contiguous_element(g, e, b, tmp) if (g->index(e)->size) x++;
        assert(x < 9);
        bcount[x]++;
        x = 0;
        foreach_overflow_element(g, e, b, tmp) x++;
        assert(x < 17);
        ocount[x]++;
      }
      for (int s = 0; s < g->sectors(); s++) {
        x = 0;
        if (g->freelist_present(s)) {
          int e = overflow_element(s, g->freelist_head(s));
          int n = e;
          do {
            e = n;
            x++;
            n = overflow_element(s, g->index(e)->next);
          } while (e != n);
        }
        assert(x < 17);
        fcount[x]++;
      }
      lacount += g->lookaside.count();
    }
  }
  printf("bcount: ");
  for (int i = 0; i < 9; i++) printf("%4d ", bcount[i]);
  printf("\nocount: ");
  for (int i = 0; i < 17; i++) printf("%4d ", ocount[i]);
  printf("\nfcount: ");
  for (int i = 0; i < 17; i++) printf("%4d ", fcount[i]);
  printf("\nlacount: %d", lacount);
  printf("\n");
}

void fail(cchar *str, ...) {
  char nstr[256];
  va_list ap;

  fflush(stdout);
  fflush(stderr);

  va_start(ap, str);
  snprintf(nstr, 255, "fail: %s\n", str);
  vfprintf(stderr, nstr, ap);
  va_end(ap);
  exit(1);
}
