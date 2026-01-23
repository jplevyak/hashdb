#pragma once

#include "hashdb_internal.h"
#include "slice.h"

#include <mutex>
#include <condition_variable>

class WriteBuffer {
 public:
  Gen *gen{};
  uint8_t *start{nullptr};
  uint8_t *cur{nullptr};
  uint8_t *end{nullptr};
  uint8_t *last{nullptr};
  int writing{0};
  uint32_t phase{0}, committed_phase{0}, next_phase{0};
  uint64_t offset{0}, next_offset{0};
  uint64_t pad_position{0};
  uint64_t committed_write_position{0};
  uint64_t committed_write_serial{0};
  ssize_t result{0};

  void init(Gen *g, int i);
};

class Gen {
 public:
  Slice *slice;
  int igen;
  Header *header{nullptr};
  Header *sync_header{nullptr};
  uint64_t size{0};
  uint32_t buckets{0};
  uint64_t header_offset{0};
  uint64_t index_offset{0};
  uint64_t index_size{0};
  uint64_t log_offset[2];
  uint64_t log_size{0};
  uint64_t log_buffer_size{0};
  uint64_t data_offset{0};
  uint64_t data_size{0};
  int index_parts{0};

  void *raw_index{nullptr};
  Index *index(int e) { return &((Index *)raw_index)[e]; }
  uint8_t *index_dirty_marks{nullptr};
  uint8_t *sync_buffer{nullptr};
  int syncing{0};
  void dirty_sector(int s) { index_dirty_marks[s / (INDEX_BYTES_PER_PART / SECTOR_SIZE)] = 1; }
  int is_marked_part(int p) { return index_dirty_marks[p]; }
  void unmark_part(int p) { index_dirty_marks[p] = 0; }

  WriteBuffer lbuf[LOG_BUFFERS];
  WriteBuffer wbuf[WRITE_BUFFERS];
  uint32_t cur_log{0};
  uint32_t cur_write{0};
  uint64_t log_position{0};
  LookasideCache lookaside;

  std::condition_variable write_condition;
  std::mutex mutex;
  int sync_part{0};
  uint64_t committed_write_position{0};
  uint64_t committed_write_serial{0};
  uint32_t committed_phase{0};
  char *debug_log{nullptr}, *debug_log_ptr{nullptr};

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

  int read(uint64_t key, std::vector<HashDB::Extent> &hit);
  int next(uint64_t key, Data *d, std::vector<HashDB::Extent> &hit);
  int write(uint64_t *key, int nkeys, HashDB::Marshal *marshal);
  int write_remove(uint64_t *key, int nkeys, Index *i);
  int read_element(Index *i, uint64_t key, std::vector<HashDB::Extent> &hit);
  Data *read_data(Index *i);
  WriteBuffer *get_buffer(int nkeys, uint64_t data_len);

  int find_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset);
  void insert_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset);
  void delete_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset);
  void set_element(Index *i, uint64_t key, bool phase, uint32_t size, uint32_t offset);
  void find_indexes(uint64_t key, std::vector<Index> &indexes);
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
