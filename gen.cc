/*
  Copyright 2003-2022 John Plevyak, All Rights Reserved
*/

#include "gen.h"
#include "hdb.h"
#include "slice.h"
#include "prime.h"

#include <cerrno>
#include <cinttypes>
#include <assert.h>
#include <stdlib.h>
#include <new>
#include <unistd.h>
#include <stdio.h>
#include <cstdarg>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <cstring>
#include <chrono>
#include <limits>

inline HDB *Gen::hdb() { return slice_->hdb_; }

void WriteBuffer::init(Gen *g, int i) { gen_ = g; }

void Gen::init_debug_log() {
  char debug_log_filename[256];
  strcpy(debug_log_filename, DEBUG_LOG_FILENAME);
  snprintf(debug_log_filename + strlen(debug_log_filename), sizeof(debug_log_filename) - strlen(debug_log_filename),
           "%d.%d", slice_->islice_, igen_);
  ::truncate(debug_log_filename, DEBUG_LOG_SIZE);
#ifndef O_NOATIME
#define O_NOATIME 0
#endif
  int debug_log_fd = ::open(debug_log_filename, O_RDWR | O_CREAT | O_NOATIME, 00660);
  assert(debug_log_fd > 0);
  debug_log_ptr_ = debug_log_ =
      (char *)mmap(0, DEBUG_LOG_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_NORESERVE, debug_log_fd, 0);
  ::close(debug_log_fd);
}

Gen::Gen(Slice *aslice, int aigen) {
  mutex_.lock();
  // memset((void*)this, 0, sizeof(*this)); // Unsafe for non-POD types
  slice_ = aslice;
  igen_ = aigen;

  for (int i = 0; i < LOG_BUFFERS; i++) lbuf_[i].init(this, i);
  for (int i = 0; i < WRITE_BUFFERS; i++) wbuf_[i].init(this, i);
  lookaside_.resize(1024);
#ifdef DEBUG_LOG
  init_debug_log();
#endif
  mutex_.unlock();
}

inline ssize_t Gen::pwrite(int fd, const void *buf, size_t count, off_t offset) {
  if (!hdb()->read_only_) {
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
  if (hdb()->read_only_) return;
  DebugLogEntry *e = (DebugLogEntry *)debug_log_ptr_;
  e->key_ = key;
  e->set_tag(tag);
  assert(tag);
  assert(e->get_tag() == tag);
  e->set_i(i);
  assert(e->get_i() == i);
  // printf("%s %llu %p\n", DEBUG_LOG_TYPE_NAME[e->get_tag()], e->key_, e->get_i());
  debug_log_ptr_ += sizeof(DebugLogEntry);
  if (debug_log_ptr_ - debug_log_ + sizeof(DebugLogEntry) > DEBUG_LOG_SIZE) {
    debug_log_ptr_ = debug_log_;
    printf("rolling debug_log %d %d\n", slice_->islice_, igen_);
  } else
    memset(debug_log_ptr_, 0, sizeof(DebugLogEntry));
}
#else
void Gen::debug_log_it(uint64_t key, Index *i, int tag) {}
// #define debug_log_it(a, b, c) // Already defined as empty if needed or just use function
#endif

const static char *DEBUG_LOG_TYPE_NAME[] = {"empty ", "set   ", "delete ", "ins la",
                                            "del la", "inskey", "delkey",  "write "};

void Gen::dump_debug_log() {
  if (!debug_log_) return;
  char *p = debug_log_;
  while (p - debug_log_ + sizeof(DebugLogEntry) < DEBUG_LOG_SIZE) {
    DebugLogEntry *e = (DebugLogEntry *)p;
    if (!e->get_tag()) break;
    printf("%s %llu %p\n", DEBUG_LOG_TYPE_NAME[e->get_tag()], (unsigned long long)e->key_, e->get_i());
    p += sizeof(DebugLogEntry);
  }
}

void Gen::alloc_header() {
  if (!header_) header_ = (Header *)new_aligned(SAFE_BLOCK_SIZE);
  memset(header_, 0, SAFE_BLOCK_SIZE);
  if (!sync_header_) sync_header_ = (Header *)new_aligned(SAFE_BLOCK_SIZE);
  memset(sync_header_, 0, SAFE_BLOCK_SIZE);
}

void Gen::compute_sizes(uint64_t asize, uint32_t data_per_index) {
  if (!asize) {
    uint64_t tmp = slice_->size_;
    tmp = ROUND_DOWN_SAFE_BLOCK_SIZE(tmp / slice_->gen_.size());
    if (slice_->is_raw_)  // don't overwrite MBR
      tmp -= SAFE_BLOCK_SIZE;
    asize = tmp;
  }
  size_ = asize;
  if (slice_->is_raw_)  // don't overwrite MBR
    header_offset_ = SAFE_BLOCK_SIZE;
  else
    header_offset_ = 0;
  header_offset_ += size_ * igen_;
  // compute buckets
  uint64_t s = size_;
  s -= SAFE_BLOCK_SIZE;  // header + footer
  uint64_t i = s / data_per_index;
  int sectors = i / ELEMENTS_PER_SECTOR;
  buckets_ = sectors * BUCKETS_PER_SECTOR;
  buckets_ = next_higher_prime(buckets_);
  sectors = (buckets_ + 5) / 6;
  // compute index length
  i = sectors;
  i *= ELEMENTS_PER_SECTOR;
  i *= sizeof(Index);
  assert(8 == sizeof(Index));
  index_size_ = ROUND_TO(i, SAFE_BLOCK_SIZE);
  index_parts_ = ROUND_DIV(index_size_, INDEX_BYTES_PER_PART);
  log_size_ = index_size_;
  log_buffer_size_ = hdb()->write_buffer_size_ < log_size_ ? hdb()->write_buffer_size_ : log_size_;
  // compute offsets
  index_offset_ = header_offset_ + SAFE_BLOCK_SIZE;
  log_offset_[0] = index_offset_ + index_size_;
  log_offset_[1] = log_offset_[0] + log_size_;
  data_offset_ = log_offset_[1] + log_size_;
  // compute data length
  data_size_ = s - header_offset_ - SAFE_BLOCK_SIZE - index_size_ - log_size_;
  data_size_ = ROUND_DOWN_SAFE_BLOCK_SIZE(data_size_);
  assert(data_size_ >= hdb()->write_buffer_size_);
  // printf("index_size = %llu data_size = %llu data_offset = %llu\n", index_size_, data_size_, data_offset_);
}

void Gen::init_header() {
  compute_sizes();
  // setup header
  alloc_header();
  header_->magic_ = HDB_MAGIC;
  header_->major_version_ = HDB_MAJOR_VERSION;
  header_->minor_version_ = HDB_MINOR_VERSION;
  header_->write_position_ = 0;
  header_->index_serial_ = (int32_t)time(NULL);
  // semi-monotonic, semi-random for fast clearing
  header_->write_serial_ = (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count();
  header_->phase_ = 0;
  header_->data_per_index_ = hdb()->init_data_per_index_;
  header_->size_ = size_;
  header_->generations_ = slice_->gen_.size();
  memcpy(sync_header_, header_, SAFE_BLOCK_SIZE);
}

int Gen::load_header() {
  alloc_header();
  if (pread(slice_->fd_, header_, SAFE_BLOCK_SIZE, header_offset_) != SAFE_BLOCK_SIZE) return -1;
  return 0;
}

void Gen::free_header() {
  if (header_) {
    delete_aligned(header_);
    header_ = 0;
  }
  if (sync_header_) {
    delete_aligned(sync_header_);
    sync_header_ = 0;
  }
}

void Gen::snap_header() {
  memcpy(sync_header_, header_, SAFE_BLOCK_SIZE);
  sync_header_->write_position_ = committed_write_position_;
  sync_header_->write_serial_ = committed_write_serial_;
  sync_header_->phase_ = committed_phase_;
}

void Gen::free_index() {
  if (raw_index_) {
    delete_aligned(raw_index_);
    raw_index_ = 0;
  }
  if (index_dirty_marks_) {
    delete[] index_dirty_marks_;
    index_dirty_marks_ = 0;
  }
  if (sync_buffer_) {
    delete_aligned(sync_buffer_);
    sync_buffer_ = 0;
  }
}

void Gen::free_bufs() {
  for (int i = 0; i < LOG_BUFFERS; i++) {
    if (lbuf_[i].start_) {
      delete_aligned(lbuf_[i].start_);
      lbuf_[i].start_ = 0;
    }
  }
  for (int i = 0; i < WRITE_BUFFERS; i++) {
    if (wbuf_[i].start_) {
      delete_aligned(wbuf_[i].start_);
      wbuf_[i].start_ = 0;
    }
  }
}

void Gen::alloc_index() {
  free_index();
  assert(index_size_);
  raw_index_ = new_aligned(index_size_);
  index_dirty_marks_ = new uint8_t[index_parts_];
  sync_buffer_ = (uint8_t *)new_aligned(INDEX_BYTES_PER_PART);
}

void Gen::init_index() {
  alloc_index();
  memset(raw_index_, 0, index_size_);
  memset(index_dirty_marks_, 1, index_parts_);
  for (int s = 0; s < sectors(); s++) {
    freelist_present(s) = 1;
    freelist_head(s) = 0;
    for (int i = 0; i < FREELIST_SIZE; i++) {
      int e = overflow_element(s, i);
      if (i != FREELIST_SIZE - 1)
        index(e)->next_ = i + 1;
      else
        index(e)->next_ = i;  // terminate with self pointer
    }
  }
}

int Gen::_init() {
  init_header();
  init_index();
  return save();
}

int Gen::init() {
  mutex_.lock();
  int r = _init();
  mutex_.unlock();
  return r;
}

int Gen::recovery() {
  int r = 0;
  r = recover_log() | r;
  r = recover_data() | r;
  return r;
}

int Gen::recover_log() {
  int ret = 0;
  int l = (int)log_buffer_size_, keep = 0;
  uint8_t *buf = (uint8_t *)new_aligned(l);
  uint64_t wpos = 0;
  while (1) {
    if (pread(slice_->fd_, buf + keep, l - keep, log_offset_[log_phase()] + wpos + keep) < 0) {
      ret = -1;
      goto Lreturn;
    }
    keep = 0;
    LogHeader *h = (LogHeader *)buf;
    if (h->magic_ != LOG_MAGIC || h->initial_phase_ != header_->phase_ ||
        h->last_write_position_ != header_->write_position_ || h->last_write_serial_ != header_->write_serial_)
      break;

    // Verify hash
    blake3_hasher hasher;
    uint8_t hash[32];
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, buf + sizeof(LogHeader), h->length_ - sizeof(LogHeader));
    blake3_hasher_finalize(&hasher, hash, BLAKE3_OUT_LEN);

    if (memcmp(hash, h->hash_, 32) != 0) {
      hdb()->warn("log corruption detected during recovery at offset %llu", wpos);
      break;
    }

    // No footer to check
    header_->write_serial_ = h->write_serial_;
    header_->write_position_ = h->write_position_;
    header_->phase_ = h->final_phase_;
    committed_write_position_ = header_->write_position_;
    committed_write_serial_ = header_->write_serial_;
    committed_phase_ = header_->phase_;
    log_position_ = wpos;
    uint8_t *b = buf + sizeof(LogHeader);
    while (1) {
      if ((size_t)(h->length_ - (b - buf)) < sizeof(LogEntry)) {
        keep = l - (b - buf);
        if (keep >= l) goto Lreturn;
        break;
      }
      LogEntry *e = (LogEntry *)b;
      // printf("recovering log entry %s %d keys\n", e->index()->next_ ? "remove" : "add", e->nkeys_);
      commit_log_entry(e);
      b += e->size();
    }
    if (keep) memmove(buf, buf + l - keep, keep);
    wpos += l - keep;
  }
Lreturn:
  // printf("recovered log absolute write position = %lld, wserial = %lld, phase = %d\n", header_->write_position_ +
  // data_offset_, header_->write_serial_, header_->phase_);
  delete_aligned(buf);
  return ret;
}

int Gen::recover_data() {
  int ret = 0;
  uint64_t wserial = header_->write_serial_;
  int l = hdb()->write_buffer_size_, keep = 0;
  uint8_t *buf = (uint8_t *)new_aligned(l);
  uint64_t wpos = header_->write_position_;
  while (1) {
  Lwrap:;
    int bytes = l - keep;
    if (wpos + bytes > data_size_) {
      bytes = data_size_ - wpos;
      if (!bytes) {
        header_->write_position_ = wpos = 0;
        header_->phase_ = !header_->phase_;
        committed_write_position_ = header_->write_position_;
        committed_write_serial_ = header_->write_serial_;
        committed_phase_ = header_->phase_;
        goto Lwrap;
      }
    }
    if (pread(slice_->fd_, buf + keep, bytes, data_offset_ + wpos + keep) < 0) {
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
      int len = size_to_length(d->size_);
      if (keep < (int)(len)) {
        if (keep >= l) goto Lreturn;
        break;
      }
      if (check_data(d, wpos + (b - buf), len, d->offset_, 1)) goto Lreturn;
      if (d->write_serial_ != wserial) {
        if (!(d->padding_ && d->write_serial_ == wserial - 1)) goto Lreturn;
      }
      if (!d->padding_) {
        // printf("recovering data %s %d keys at %lld\n",  d->remove_ ? "remove" : "add", d->nkeys_, data_offset + wpos
        // + (b - buf));
        commit_data(d, 1);
        wserial++;
      }
      b += len;
      header_->write_position_ = wpos + (b - buf);
      header_->write_serial_ = wserial;
      committed_write_position_ = header_->write_position_;
      committed_write_serial_ = header_->write_serial_;
      committed_phase_ = header_->phase_;
    }
    if (keep) memmove(buf, buf + l - keep, keep);
    wpos += l - keep;
  }
Lreturn:
  // printf("recovered data absolute write position = %lld, wserial = %lld, phase = %d\n", header_->write_position_ +
  // data_offset_, header_->write_serial_, header_->phase_);
  delete_aligned(buf);
  return ret;
}

int Gen::load_index() {
  alloc_index();
  memset(index_dirty_marks_, 0, index_parts_);
  if ((uint64_t)pread(slice_->fd_, raw_index_, index_size_, index_offset_) != index_size_) return -1;
  if (recovery() < 0) return -1;
  return 0;
}

int Gen::open() {
  mutex_.lock();
  compute_sizes();
  int lh = load_header();
  if (lh < 0 || header_->magic_ != HDB_MAGIC || header_->major_version_ != HDB_MAJOR_VERSION) {
    if (slice_->hdb_->reinit_on_open_error_ || header_->magic_ == 0 || lh < 0) {
      _init();
    } else
      goto Lerror;
  } else {  // normal load path
    compute_sizes(header_->size_, header_->data_per_index_);
    load_index();
  }
  for (int i = 0; i < LOG_BUFFERS; i++) {
    lbuf_[i].offset_ = std::numeric_limits<unsigned long long>::max();
    lbuf_[i].start_ = lbuf_[i].cur_ = (uint8_t *)new_aligned(log_buffer_size_);
    lbuf_[i].end_ = lbuf_[i].start_ + log_buffer_size_;
    lbuf_[i].cur_ += sizeof(LogHeader);
  }
  cur_log_ = 0;
  log_position_ = 0;
  for (int i = 0; i < WRITE_BUFFERS; i++) {
    wbuf_[i].offset_ = std::numeric_limits<unsigned long long>::max();  // short circuit test in Gen::read_data
    size_t sz = hdb()->write_buffer_size_;
    wbuf_[i].start_ = wbuf_[i].cur_ = (uint8_t *)new_aligned(sz);
    wbuf_[i].end_ = wbuf_[i].start_ + sz;
    wbuf_[i].next_offset_ = std::numeric_limits<unsigned long long>::max();
    wbuf_[i].next_phase_ = 0;
  }
  cur_write_ = 0;
  wbuf_[cur_write_].offset_ = data_offset_ + header_->write_position_;
  wbuf_[cur_write_].phase_ = header_->phase_;
  committed_write_position_ = header_->write_position_;
  committed_write_serial_ = header_->write_serial_;
  committed_phase_ = header_->phase_;
  for (int s = 0; s < sectors(); s++) clean_sector(s);
  snap_header();
  mutex_.unlock();
  return 0;
Lerror:
  mutex_.unlock();
  return -1;
}

static inline void wait_for_write_to_complete(WriteBuffer *b) {
  std::unique_lock<std::mutex> lk(b->gen_->mutex_, std::adopt_lock);
  while (b->writing_) b->gen_->write_condition_.wait(lk);
  lk.release();  // release ownership so mutex remains locked for caller
}

static inline int cmp_wpos(uint64_t wpos1, int phase1, uint64_t wpos2, int phase2) {
  if (phase1 == phase2) return wpos1 < wpos2 ? -1 : ((wpos1 == wpos2) ? 0 : 1);
  return wpos1 > wpos2 ? -1 : ((wpos1 == wpos2) ? 0 : 1);
}

static inline void wait_for_write_commit(Gen *g, uint64_t wpos, int phase) {
  while (cmp_wpos(wpos, phase, g->committed_write_position_, g->committed_phase_) < 0) {
    WriteBuffer &w = g->wbuf_[g->cur_write_];
    if (cmp_wpos(wpos, phase, w.offset_ - g->data_offset_ + (w.cur_ - w.start_), w.phase_) < 0) g->write_buffer();
    std::unique_lock<std::mutex> lock(g->mutex_, std::adopt_lock);
    g->write_condition_.wait(lock);
    lock.release();
  }
}

WriteBuffer *Gen::get_buffer(int nkeys, uint64_t l) {
Lagain:
  reserve_log_space(nkeys);
  WriteBuffer *b = &wbuf_[cur_write_];
  if (b->writing_) {
    wait_for_write_to_complete(b);
    goto Lagain;
  }
  if (l > hdb()->write_buffer_size_) return 0;
  int force_wrap = b->offset_ + (b->cur_ - b->start_) + l > data_offset_ + data_size_;
  if (b->cur_ + l > b->end_ || force_wrap) {
    write_buffer(force_wrap);
    goto Lagain;
  }
  return b;
}

void Gen::write_index_part(int p) {
  clean_index_part(p);
  uint64_t o = INDEX_BYTES_PER_PART * p;
  uint8_t *b = ((uint8_t *)raw_index_) + o;
  int l = INDEX_BYTES_PER_PART;
  if (p >= index_parts_ - 1) l = index_size_ - o;
  memcpy(sync_buffer_, b, l);
  mutex_.unlock();  // release lock around write
  if (pwrite(slice_->fd_, sync_buffer_, l, index_offset_ + o) != (ssize_t)l)
    hdb()->err("unable to save index part %d for gen %d '%s'", p, igen_, slice_->pathname_);
  mutex_.lock();
  // printf("write index part %d at %lld length %d\n", p, index_offset + o, l);
}

void Gen::write_upto_index_part(int p, int stop_on_marked) {
  while (syncing_) {  // prevent function from being reentered
    std::unique_lock<std::mutex> lock(mutex_, std::adopt_lock);
    write_condition_.wait(lock);
    lock.release();
  }
  syncing_ = 1;
  int broadcast = 0;
  if (p > index_parts_) p = index_parts_;
  for (; sync_part_ < p;) {
    if (is_marked_part(sync_part_)) {
      write_index_part(sync_part_);
      unmark_part(sync_part_);
      sync_part_++;
      broadcast = 1;
      if (stop_on_marked) break;
    } else
      sync_part_++;
  }
  syncing_ = 0;
  if (broadcast) write_condition_.notify_all();
}

void Gen::write_index() {
  if (pwrite(slice_->fd_, raw_index_, index_size_, index_offset_) != (ssize_t)index_size_)
    hdb()->err("unable to save index for gen %d '%s'", igen_, slice_->pathname_);
}

void Gen::write_header(Header *aheader) {
  if (pwrite(slice_->fd_, aheader, SAFE_BLOCK_SIZE, header_offset_) != (ssize_t)SAFE_BLOCK_SIZE)
    hdb()->err("unable to save header for gen %d '%s'", igen_, slice_->pathname_);
}

int Gen::save() {
  WriteBuffer &w = wbuf_[cur_write_];
  if (w.start_ != w.cur_) write_buffer();
  for (int i = 0; i < WRITE_BUFFERS; i++) wait_for_write_to_complete(&wbuf_[i]);
  write_upto_index_part(index_parts_);
  write_header(header_);
  return 0;
}

static int verify_offset(Gen *g, Index *i) {
  if (i->size_) {
    if (g->committed_phase_ == i->phase_) {
      uint64_t o = ((uint64_t)i->offset_) * ATOMIC_WRITE_SIZE + size_to_length(i->size_);
      if (g->committed_write_position_ < o) return -1;
    } else {
      uint64_t o = ((uint64_t)i->offset_) * ATOMIC_WRITE_SIZE;
      if (g->committed_write_position_ > o) return -1;
    }
  }
  return 0;
}

static int verify_element(Gen *g, int e) {
  Index *i = g->index(e);
  if (!i->size_) return 0;  // empty
  if (verify_offset(g, i)) return -1;
  Data *d = g->read_data(i);
  if (!d) return -1;
  printf("key %llu size %d off %d\n", (unsigned long long)d->chain_[0].key_, d->size_, i->offset_);
  delete_aligned(d);
  return 0;
}

int Gen::verify() {
  mutex_.lock();
  int ret = 0;
  for (uint32_t b = 0; b < buckets_; b++) {
    foreach_contiguous_element(this, e, b, tmp) ret = verify_element(this, e) | ret;
    foreach_overflow_element(this, e, b, tmp) ret = verify_element(this, e) | ret;
  }
  mutex_.unlock();
  return ret;
}

void Gen::free() {
  free_header();
  free_index();
  free_bufs();
  lookaside_.clear();
}

int Gen::close() {
  std::lock_guard<std::mutex> lock(mutex_);
  int res = save();
  free();
  return res;
}

void Gen::complete_index_sync() {
  sync_part_ = 0;
  if (log_position_) {
    // printf("complete_index_sync\n");
    header_->index_serial_++;
    log_position_ = 0;
    write_header(sync_header_);
    snap_header();
  }
}

void Gen::periodic_sync() {
  std::lock_guard<std::mutex> lock(mutex_);
  WriteBuffer &w = wbuf_[cur_write_];
  if (w.start_ != w.cur_ && !w.writing_) write_buffer();
  write_upto_index_part(sync_part_ + 1, 1);  // write one more
  if (sync_part_ >= index_parts_) complete_index_sync();
}

static void write_padding(WriteBuffer *b) {
  uint64_t left = b->gen_->data_size_ - b->pad_position_;
  Data *dlast = (Data *)b->last_;
  Data *d = (Data *)new_aligned(left);
  memset((void *)d, 0, left);
  d->magic_ = DATA_MAGIC;
  d->write_serial_ = dlast->write_serial_;
  d->slice_ = b->gen_->slice_->islice_;
  d->gen_ = b->gen_->igen_;
  d->reserved1_ = 0;
  ((&d->offset_)[1]) = 0;  // clear flags
  d->padding_ = 1;
  d->nkeys_ = 0;
  uint32_t s = length_to_size(left);
  uint32_t o = b->pad_position_ / ATOMIC_WRITE_SIZE;
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
    d->offset_ = o;
    d->length_ = left - sizeof(Data) - sizeof(DataFooter);
    d->size_ = length_to_size(left);
    if (b->gen_->pwrite(b->gen_->slice_->fd_, b->start_, left, b->pad_position_ + b->gen_->data_offset_) < 0)
      b->gen_->hdb()->err("write error, errno %d slice %d generation %d offset %lld length %d", errno,
                          b->gen_->slice_->islice_, b->gen_->igen_, b->pad_position_ + b->gen_->data_offset_, left);
    o += left;
    left = still_left;
    s = length_to_size(left);
  }
  delete_aligned(d);
}

static void *do_write_buffer(WriteBuffer *b) {
  Gen *g = b->gen_;
#ifdef HELGRIND
  pthread_mutex_lock(&g->mutex_);  // for halgrind
#endif
  // producer-consumer, no races
  ssize_t r = b->gen_->pwrite(g->slice_->fd_, b->start_, b->cur_ - b->start_, b->offset_);
  int padding = b->pad_position_;
#ifdef HELGRIND
  pthread_mutex_unlock(&g->mutex_);  // for halgrind
#endif
  if (padding) write_padding(b);
  g->mutex_.lock();
  if (r < 0)
    g->hdb()->err("write error, errno %d slice %d generation %d offset %lld length %d", errno, g->slice_->islice_,
                  g->igen_, b->offset_, b->cur_ - b->start_);
  b->result_ = r;
  // printf("write buffer absolute write position = %lld, wserial = %lld, phase = %d, offset = %d\n",
  // b->committed_write_position_ + g->data_offset_, b->committed_write_serial_, b->phase_, (int)((b->offset_ -
  // b->gen_->data_offset_)/512));
  g->committed_write_position_ = b->committed_write_position_;
  g->committed_write_serial_ = b->committed_write_serial_;
  g->committed_phase_ = b->committed_phase_;
  g->commit_buffer(b->start_, b->cur_);
  b->cur_ = b->start_;
  b->offset_ = b->next_offset_;
  b->phase_ = b->next_phase_;
  b->next_offset_ = std::numeric_limits<unsigned long long>::max();  // short circuit test in Gen::read_data
  b->writing_ = 0;
  g->write_condition_.notify_all();
  g->mutex_.unlock();
  return 0;
}

void Gen::write_buffer(int force_wrap) {
  WriteBuffer &w = wbuf_[cur_write_];
  assert(!w.writing_);
  w.writing_ = 1;
  w.pad_position_ = 0;

  uint64_t new_write_position = header_->write_position_ + (w.cur_ - w.start_);
  assert(new_write_position <= data_size_);
  if (new_write_position > data_size_ || force_wrap) {
    if (header_->write_position_ < data_size_) w.pad_position_ = header_->write_position_;
    header_->phase_ = (header_->phase_ + 1) & 1;
    header_->write_position_ = 0;
    for (int s = 0; s < sectors(); s++) clean_sector(s);
  } else
    header_->write_position_ = new_write_position;
  uint64_t done = MODULAR_DIFFERENCE(header_->write_position_, sync_header_->write_position_, data_size_);
  int parts_done = done / (data_size_ / index_parts_);
  // printf("done %lld part_size %lld parts_done %d sync_part %d\n", done, data_size / index_parts, parts_done,
  // sync_part);
  w.committed_write_position_ = header_->write_position_;
  w.committed_write_serial_ = header_->write_serial_;
  w.committed_phase_ = header_->phase_;
  int prev_write = (cur_write_ + (WRITE_BUFFERS - 1)) % WRITE_BUFFERS;
  cur_write_ = (cur_write_ + 1) % WRITE_BUFFERS;
  WriteBuffer &p = wbuf_[prev_write], &n = wbuf_[cur_write_];
  if (n.writing_) {
    n.next_offset_ = data_offset_ + header_->write_position_;
    n.next_phase_ = header_->phase_;
  } else {
    n.offset_ = data_offset_ + header_->write_position_;
    n.phase_ = header_->phase_;
  }
  while (p.writing_ && cmp_wpos(w.offset_, w.phase_, p.offset_, p.phase_) >= 0) {
    std::unique_lock<std::mutex> lock(p.gen_->mutex_, std::adopt_lock);
    p.gen_->write_condition_.wait(lock);
    lock.release();
  }
  if (parts_done > sync_part_) write_upto_index_part(parts_done);
  slice_->hdb_->thread_pool_->add_job((void *(*)(void *))do_write_buffer, (void *)&w);
  return;
}

static void add_log_header(WriteBuffer *b) {
  LogHeader *h = (LogHeader *)b->start_;
  // No footer
  assert(b->cur_ <= b->end_);
  uint32_t l = b->cur_ - b->start_;
  memset(h, 0, sizeof(LogHeader));
  h->magic_ = LOG_MAGIC;
  Gen *g = b->gen_;
  h->write_position_ = g->committed_write_position_;
  h->write_serial_ = g->committed_write_serial_;
  h->initial_phase_ = g->sync_header_->phase_;
  h->last_write_position_ = g->sync_header_->write_position_;
  h->last_write_serial_ = g->sync_header_->write_serial_;
  h->final_phase_ = g->header_->phase_;
  h->length_ = l;

  // Calculate Hash
  blake3_hasher hasher;
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, b->start_ + sizeof(LogHeader), l - sizeof(LogHeader));
  blake3_hasher_finalize(&hasher, h->hash_, BLAKE3_OUT_LEN);

  // padding
  uint32_t s = ROUND_TO(l, ATOMIC_WRITE_SIZE);
  if (s - l) memset(b->cur_, 0, s - l);
  b->cur_ += s - l;  // pad out
}

static void *do_write_log_buffer(WriteBuffer *b) {
  Gen *g = b->gen_;
#ifdef HELGRIND
  pthread_mutex_lock(&g->mutex_);  // for halgrind
#endif
  b->offset_ = g->log_offset_[g->log_phase()] + g->log_position_;
  b->result_ = b->gen_->pwrite(g->slice_->fd_, b->start_, b->cur_ - b->start_, b->offset_);
  if (b->result_ < 0)
    g->hdb()->err("write error, errno %d slice %d generation %d offset %lld length %d", errno, g->slice_->islice_,
                  g->igen_, b->offset_, b->cur_ - b->start_);
  // printf("wrote log %lld %d \n", b->offset_, (int)(b->cur_ - b->start_));
#ifndef HELGRIND
  g->mutex_.lock();
#endif
  g->log_position_ += b->cur_ - b->start_;
  b->writing_ = 0;
  b->cur_ = b->start_ + sizeof(LogHeader);
  g->write_condition_.notify_all();
  g->mutex_.unlock();
  return 0;
}

void Gen::write_log_buffer() {
  WriteBuffer &b = lbuf_[cur_log_];
  assert(b.start_ != b.cur_ && !b.writing_);
  b.writing_ = 1;
  if (log_position_ + (b.cur_ - b.start_) > log_size_) {
    write_upto_index_part(index_parts_);
    complete_index_sync();
  }
  add_log_header(&b);
  uint32_t prev_log_write = (cur_log_ + (LOG_BUFFERS - 1)) % LOG_BUFFERS;
  wait_for_write_to_complete(&lbuf_[prev_log_write]);  // log writes initiated in order
  slice_->hdb_->thread_pool_->add_job((void *(*)(void *))do_write_log_buffer, (void *)&b);
  cur_log_ = (cur_log_ + 1) % LOG_BUFFERS;
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
    i->size_ = 0;  // just blow it away
  return 0;
}

int Gen::delete_overflow_element(int e, int p, int b) {
  int s = bucket_to_sector(b);
  Index *i = index(e);
  debug_log_it(0, i, DEBUG_LOG_DELETE);
  int r = -1;
  dirty_sector(s);
  if (p == -1) {  // first element
    int n = (int)overflow_element(s, i->next_);
    if (e == n)  // end of list, only element
      overflow_present(b) = 0;
    else {
      r = n;
      overflow_head(b) = i->next_;
    }
  } else {
    int n = (int)overflow_element(s, i->next_);
    if (e == n)  // end of list
      index(p)->next_ = overflow_element_number(p);
    else {
      r = n;
      index(p)->next_ = i->next_;
    }
  }
  if (!freelist_present(s)) {
    freelist_present(s) = 1;
    i->next_ = overflow_element_number(e);
  } else {
    i->next_ = freelist_head(s);
  }
  freelist_head(s) = overflow_element_number(e);
  return r;
}

void Gen::set_element(Index *i, uint64_t key, bool phase, uint32_t size, uint32_t offset) {
  i->offset_ = offset;
  i->phase_ = phase;
  i->size_ = size;
  assert(i->size_);
  i->tag_ = KEY2TAG(key);
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
        index(p)->next_ = overflow_element_number(p);
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
  if (d->magic_ != DATA_MAGIC) {
    if (!recovery) hdb()->err("bad data magic, slice %d generation %d offset %lld", slice_->islice_, igen_, o);
    return -1;
  }
  if (((char *)(&d->chain_[d->nkeys_])) > ((char *)d) + l) {
    if (!recovery) hdb()->err("off the end, slice %d generation %d offset %lld", slice_->islice_, igen_, o);
    return -1;
  }
  if (d->length_ > l - (((char *)&d->chain_[d->nkeys_]) - ((char *)d))) {
    if (!recovery) hdb()->err("too small, slice %d generation %d offset %lld", slice_->islice_, igen_, o);
    return -1;
  }
  if (d->offset_ != offset) {
    if (!recovery)
      hdb()->err("bad data offset, slice %d generation %d offset %lld, data offset %d, offset %d", slice_->islice_,
                 igen_, o, d->offset_, offset);
    return -1;
  }
  if ((int)d->gen_ != igen_) {
    if (!recovery)
      hdb()->err("bad data gen, slice %d generation %d offset %lld, data gen %d", slice_->islice_, igen_, o, d->gen_);
    return -1;
  }
  if ((int)d->slice_ != slice_->islice_) {
    if (SLICE_INDEX_MISMATCH_RESULT) return 1;
    hdb()->warn("unexpected data slice index, slice %d generation %d offset %lld, data slice index %d", slice_->islice_,
                igen_, o, d->slice_);
  }
  return 0;
}

void Gen::commit_data(Data *d, int recovery) {
  for (uint32_t i = 0; i < d->nkeys_; i++) {
    if (d->remove_) {
      Index *index = (Index *)DATA_TO_PTR(d);
      if (recovery || delete_lookaside(d->chain_[i].key_, index))
        delete_key(d->chain_[i].key_, index->phase_, index->size_, index->offset_);
    } else {
      Index index(d->offset_, KEY2TAG(d->chain_[i].key_), d->size_, 0, d->phase_);
      if (recovery || delete_lookaside(d->chain_[i].key_, &index)) {
        if (d->chain_[i].next_.size_)  // !empty
          if (verify_offset(this, &d->chain_[i].next_) >= 0)
            delete_key(d->chain_[i].key_, d->chain_[i].next_.phase_, d->chain_[i].next_.size_,
                       d->chain_[i].next_.offset_);
        if (find_key(d->chain_[i].key_, d->phase_, d->size_, d->offset_) < 0)
          insert_key(d->chain_[i].key_, d->phase_, d->size_, d->offset_);
      }
    }
  }
}

void Gen::commit_log_entry(LogEntry *e) {
  Index *index = e->index();
  uint64_t *keys = e->keys();
  for (uint32_t i = 0; i < e->nkeys_; i++) {
    if (index->next_) {  // remove
      delete_key(keys[i], index->phase_, index->size_, index->offset_);
    } else {
      if (hdb()->chain_collisions_) delete_collision(keys[i]);
      if (find_key(keys[i], index->phase_, index->size_, index->offset_) < 0)
        insert_key(keys[i], index->phase_, index->size_, index->offset_);
    }
  }
}

void Gen::commit_buffer(uint8_t *start, uint8_t *end) {
  uint8_t *b = start;
  while (b < end) {
    Data *d = (Data *)b;
    int len = size_to_length(d->size_);
    commit_data(d);
    b += len;
  }
}

int Gen::find_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset) {
  int b = ((uint32_t)key) % buckets_;
  uint16_t tag = KEY2TAG(key);
  foreach_contiguous_element(this, e, b, tmp) {
    Index *i = index(e);
    if (i->tag_ == tag && i->phase_ == phase && i->size_ == size && i->offset_ == offset) return e;
  }
  foreach_overflow_element(this, e, b, tmp) {
    Index *i = index(e);
    if (i->tag_ == tag && i->phase_ == phase && i->size_ == size && i->offset_ == offset) return e;
  }
  return -1;
}

void Gen::insert_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset) {
  debug_log_it(key, 0, DEBUG_LOG_INS_KEY);
  int b = ((uint32_t)key) % buckets_;
  int s = bucket_to_sector(b);
  int retry = 0;
  dirty_sector(s);
  {
  Lretry:
    foreach_contiguous_element(this, e, b, tmp) {
      Index *i = index(e);
      if (!i->size_) {
        set_element(i, key, phase, size, offset);
        return;
      }
    }
    if (!freelist_present(s)) {
      if (retry) {
        // yes this can leave cruft around, but it should be rare and reads are verified
        hdb()->err("freelist empty, slice %d generation %d sector %d", slice_->islice_, igen_, s);
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
  int n = i->next_;
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
  i->next_ = nn;
  return;
}

void Gen::delete_key(uint64_t key, uint32_t phase, uint32_t size, uint32_t offset) {
  debug_log_it(key, 0, DEBUG_LOG_DEL_KEY);
  int b = ((uint32_t)key) % buckets_;
  int s = bucket_to_sector(b);
  uint16_t tag = KEY2TAG(key);
  foreach_contiguous_element(*this, e, b, tmp) {
    Index *i = index(e);
    if (i->offset_ == offset && i->tag_ == tag && i->size_ == size && i->phase_ == phase) {
      if (overflow_present(b)) {
        int n = overflow_head(b);
        int ee = overflow_element(s, n);
        copy_index(i, index(ee));
        delete_overflow_element(ee, -1, b);
      } else
        i->size_ = 0;  // just blow it away
      goto Ldeleted;
    }
  }
  foreach_overflow_element(this, e, b, p) {
    Index *i = index(e);
    if (i->offset_ == offset && i->tag_ == tag && i->size_ == size && i->phase_ == phase) {
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
  int b = ((uint32_t)key) % buckets_;
  int s = bucket_to_sector(b);
  uint16_t tag = KEY2TAG(key);
  foreach_contiguous_element(*this, e, b, tmp) {
    Index *i = index(e);
    if (i->tag_ == tag && i->size_) {
      if (overflow_present(b)) {
        int n = overflow_head(b);
        int ee = overflow_element(s, n);
        copy_index(i, index(ee));
        delete_overflow_element(ee, -1, b);
      } else
        i->size_ = 0;  // just blow it away
      goto Ldeleted;
    }
  }
  foreach_overflow_element(this, e, b, p) {
    Index *i = index(e);
    if (i->tag_ == tag && i->size_) {
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
  uint64_t wpos = header_->write_position_;
  int phase = header_->phase_;
  WriteBuffer *b = &lbuf_[cur_log_];
  if (b->writing_) {
    wait_for_write_to_complete(b);
    goto Lagain;
  }
  assert(l <= log_buffer_size_);
  if (b->cur_ != b->start_ && b->cur_ + l >= b->end_) {
    wait_for_write_commit(this, wpos, phase);
    write_log_buffer();
    goto Lagain;
  }
}

void Gen::insert_log(uint64_t *key, int nkeys, Index *i) {
  int l = sizeof(LogEntry) + sizeof(Index) + sizeof(*key) * nkeys;
  WriteBuffer *b = &lbuf_[cur_log_];
  assert(b->cur_ + l < b->end_);
  LogEntry *le = (LogEntry *)b->cur_;
  uint64_t *e = (uint64_t *)b->cur_;
  le->reserved_ = 0;
  le->nkeys_ = nkeys;
  int x = sizeof(LogEntry) / sizeof(uint64_t);
  memcpy(&e[x], i, sizeof(Index));
  x += sizeof(Index) / sizeof(uint64_t);
  memcpy(&e[x], key, nkeys * sizeof(*key));
  assert(l == ((uint8_t *)&e[x + nkeys]) - b->cur_);
  b->cur_ += l;
}

void Gen::insert_lookaside(uint64_t key, Index *i) {
  Lookaside l;
  l.key_ = key;
  l.index_ = *i;
  lookaside_.put(l);
  debug_log_it(key, i, DEBUG_LOG_INSERT_LA);
}

int Gen::delete_lookaside(uint64_t key, Index *i) {
  Lookaside l;
  l.key_ = key;
  l.index_ = *i;
  int r = lookaside_.del(l);
  if (r) debug_log_it(key, i, DEBUG_LOG_DELETE_LA);
  return r;
}

int Gen::write(uint64_t *key, int nkeys, uint64_t value_len, HashDB::SerializeFn serializer) {
  uint64_t len = value_len;
  uint64_t hsize = sizeof(Data) + sizeof(DataFooter) + (nkeys - 1) * sizeof(uint64_t);
  uint32_t size = length_to_size(len + hsize);
  uint64_t l = size_to_length(size);
  WriteBuffer *b = get_buffer(nkeys, l);
  debug_log_it(*key, 0, DEBUG_LOG_WRITE);
  if (!b) return -1;
  Data *d = (Data *)b->cur_;
  d->magic_ = DATA_MAGIC;
  d->write_serial_ = header_->write_serial_++;
  uint32_t o = (b->offset_ + (b->cur_ - b->start_) - data_offset_) / ATOMIC_WRITE_SIZE;
  d->slice_ = slice_->islice_;
  d->gen_ = igen_;
  d->reserved1_ = 0;
  d->offset_ = o;
  ((&d->offset_)[1]) = 0;  // clear flags
  d->remove_ = 0;          // Initialize remove flag!
  d->phase_ = b->phase_;
  d->nkeys_ = nkeys;
  memset((void *)d->chain_, 0, nkeys * sizeof(KeyChain));
  for (int i = 0; i < nkeys; i++) d->chain_[i].key_ = key[i];
  DATA_TO_FOOTER(d)->nkeys_ = nkeys;
  char *target = (char *)(b->cur_ + hsize);
  std::span<uint8_t> s((uint8_t *)target, len);
  uint64_t actual_len = serializer(s);
  if (l != actual_len + hsize) memset(target + actual_len, 0, l - (actual_len + hsize));
  d->length_ = actual_len;
  d->size_ = size;

  // Calculate Hash
  blake3_hasher hasher;
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, d, (uint8_t *)d->hash_ - (uint8_t *)d);
  // Hash from chain start to end of block
  blake3_hasher_update(&hasher, d->chain_, (uint8_t *)d + l - (uint8_t *)d->chain_);
  blake3_hasher_finalize(&hasher, d->hash_, BLAKE3_OUT_LEN);

  if (hdb()->chain_collisions_) chain_keys_for_write(d);
  Index index(d->offset_, KEY2TAG(*key), d->size_, 0, d->phase_);
  insert_log(key, nkeys, &index);
  for (int i = 0; i < nkeys; i++) insert_lookaside(key[i], &index);
  b->last_ = b->cur_;
  b->cur_ += l;
  return 0;
}

void Gen::chain_keys_for_write(Data *d) {
  for (uint32_t i = 0; i < d->nkeys_; i++) {
    std::vector<Index> rd;
    find_indexes(d->chain_[i].key_, rd);
    if (rd.size()) d->chain_[i].next_ = rd[0];
  }
}

int Gen::write_remove(uint64_t *key, int nkeys, Index *i) {
  uint64_t hsize = sizeof(Data) + sizeof(DataFooter) + (nkeys - 1) * sizeof(uint64_t);
  uint32_t size = length_to_size(hsize);
  uint64_t l = size_to_length(size);
  WriteBuffer *b = get_buffer(nkeys, l);
  if (!b) return -1;
  Data *d = (Data *)b->cur_;
  d->magic_ = DATA_MAGIC;
  d->write_serial_ = header_->write_serial_++;
  uint32_t o = (b->offset_ + (b->cur_ - b->start_) - data_offset_) / ATOMIC_WRITE_SIZE;
  d->slice_ = slice_->islice_;
  d->gen_ = igen_;
  d->reserved1_ = 0;
  d->offset_ = o;
  ((&d->offset_)[1]) = 0;  // clear flags
  d->remove_ = 1;
  d->phase_ = b->phase_;
  d->nkeys_ = nkeys;
  assert(nkeys);
  memset((void *)d->chain_, 0, nkeys * sizeof(KeyChain));
  for (int j = 0; j < nkeys; j++) d->chain_[j].key_ = key[j];
  DATA_TO_FOOTER(d)->nkeys_ = nkeys;
  *(Index *)DATA_TO_PTR(d) = *i;
  char *target = (char *)(b->cur_ + hsize + sizeof(Index));
  if (l != hsize + sizeof(Index)) memset(target, 0, l - hsize - sizeof(Index));
  d->length_ = sizeof(Index);
  d->size_ = size;

  // Calculate Hash
  blake3_hasher hasher;
  blake3_hasher_init(&hasher);
  blake3_hasher_update(&hasher, d, (uint8_t *)d->hash_ - (uint8_t *)d);
  blake3_hasher_update(&hasher, d->chain_, (uint8_t *)d + l - (uint8_t *)d->chain_);
  blake3_hasher_finalize(&hasher, d->hash_, BLAKE3_OUT_LEN);

  b->last_ = b->cur_;
  b->cur_ += l;
  return 0;
}

int Gen::read_element(Index *i, uint64_t key, std::vector<HashDB::Extent> &hit) {
  Data *d = read_data(i);
  if (d == BAD_DATA) return -1;
  if (!d) return 0;
  for (uint32_t x = 0; x < d->nkeys_; x++) {
    if (d->chain_[x].key_ == key) {
      hit.push_back(HashDB::Extent());
      HashDB::Extent &e = hit.back();
      e.data = DATA_TO_PTR(d);
      e.len = d->length_;
      return 0;
    }
  }
  delete_aligned(d);
  return 0;
}

Data *Gen::read_data(Index *i) {
  uint32_t l = size_to_length(i->size_);
  uint64_t o = data_offset_ + ((uint64_t)i->offset_) * ATOMIC_WRITE_SIZE;
  void *buf = new_aligned(l);
  for (int x = 0; x < WRITE_BUFFERS; x++) {
    if (wbuf_[x].offset_ <= o && o + l <= wbuf_[x].offset_ + (wbuf_[x].cur_ - wbuf_[x].start_)) {
      memcpy(buf, wbuf_[x].start_ + (o - wbuf_[x].offset_), l);
      goto Lfound;
    }
  }
  if (verify_offset(this, i)) {
    if (STALE_INDEX_RESULT)
      hdb()->warn("state index entry, slice %d generation %d offset %lld", slice_->islice_, igen_, o);
    goto Lreturn;
  }
  mutex_.unlock();
  if (pread(slice_->fd_, buf, l, (off_t)o) != l) {
    delete_aligned(buf);
    mutex_.lock();
    return (Data *)BAD_DATA;
  }
  mutex_.lock();
  if (verify_offset(this, i)) goto Lreturn;
Lfound: {
  Data *d = (Data *)buf;
  if (d->remove_) goto Lreturn;
  if (check_data(d, o, l, i->offset_)) goto Lreturn;

  // Verify Hash
  if (hdb()->check_hash_) {
    blake3_hasher hasher;
    uint8_t hash[32];
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, d, (uint8_t *)d->hash_ - (uint8_t *)d);
    blake3_hasher_update(&hasher, d->chain_, (uint8_t *)d + l - (uint8_t *)d->chain_);
    blake3_hasher_finalize(&hasher, hash, BLAKE3_OUT_LEN);
    if (memcmp(hash, d->hash_, 32) != 0) {
      hdb()->err("hash mismatch reading data at offset %llu slice %d gen %d", o, slice_->islice_, igen_);
      goto Lreturn_error;
    }
  }

  if (!SLICE_INDEX_MISMATCH_RESULT)  // fixup slice number
    d->slice_ = slice_->islice_;
  return d;
}
Lreturn_error:
  delete_aligned(buf);
  return (Data *)BAD_DATA;
Lreturn:
  delete_aligned(buf);
  return 0;
}

void Gen::find_indexes(uint64_t key, std::vector<Index> &rd) {
  int b = ((uint32_t)key) % buckets_;
  uint16_t tag = KEY2TAG(key);
  std::vector<Index> del;
  unsigned int h = ((uint32_t)(key >> 32) ^ ((uint32_t)key));
  Lookaside *la = &lookaside_.v_[(h % lookaside_.n_) * 4];
  for (int a = 0; a < 4; a++) {
    if (key == la[a].key_) {
      if (!la[a].index_.next_)
        rd.push_back(la[a].index_);
      else
        del.push_back(la[a].index_);
    }
  }
  if (!rd.size() || !hdb()->chain_collisions_) {
    foreach_contiguous_element(this, e, b, tmp) {
      Index *i = index(e);
      if (i->tag_ != tag || !i->size_) continue;
      unsigned int x = 0;
      for (; x < del.size(); x++)
        if (del[x].offset_ == i->offset_ && del[x].phase_ == i->phase_) break;
      if (x == del.size()) rd.push_back(*i);
    }
    foreach_overflow_element(this, e, b, tmp) {
      Index *i = index(e);
      if (i->tag_ != tag || !i->size_) continue;
      unsigned int x = 0;
      for (; x < del.size(); x++)
        if (del[x].offset_ == i->offset_ && del[x].phase_ == i->phase_) break;
      if (x == del.size()) rd.push_back(*i);
    }
  }
}

int Gen::read(uint64_t key, std::vector<HashDB::Extent> &hit) {
  int r = 0;
  std::vector<Index> rd;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    find_indexes(key, rd);
    if (!rd.size()) r = 2;
    for (size_t x = 0; x < rd.size(); x++) r = read_element(&rd[x], key, hit) | r;
  }
  return r;
}

int Gen::next(uint64_t key, Data *d, std::vector<HashDB::Extent> &hit) {
  int r = 0;
  for (uint32_t i = 0; i < d->nkeys_; i++) {
    if (d->chain_[i].key_ == key && d->chain_[i].next_.size_) {
      std::lock_guard<std::mutex> lock(mutex_);
      r = read_element(&d->chain_[i].next_, key, hit) | r;
      return r;
    }
  }
  return 0;
}
