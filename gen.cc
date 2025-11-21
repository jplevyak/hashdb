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

inline HDB *Gen::hdb() { return slice->hdb; }

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
  // memset((void*)this, 0, sizeof(*this)); // Unsafe for non-POD types
  slice = aslice;
  igen = aigen;
  header = nullptr;
  sync_header = nullptr;
  raw_index = nullptr;
  index_dirty_marks = nullptr;
  sync_buffer = nullptr;
  committed_write_position = 0;
  committed_write_serial = 0;
  committed_phase = 0;
  cur_log = 0;
  cur_write = 0;
  log_position = 0;
  sync_part = 0;
  debug_log = nullptr;
  debug_log_ptr = nullptr;

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
// #define debug_log_it(a, b, c) // Already defined as empty if needed or just use function
#endif

const static char *DEBUG_LOG_TYPE_NAME[] = {"empty ", "set   ", "delete ", "ins la",
                                            "del la", "inskey", "delkey",  "write "};

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

static void append_footer(WriteBuffer *b) {
  assert(b->cur + FOOTER_SIZE <= b->end);
  Data *dlast = (Data *)b->last;
  Data *d = (Data *)b->cur;
  memset((void*)d, 0, FOOTER_SIZE);
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
  memset((void*)d, 0, left);
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

int Gen::write(uint64_t *key, int nkeys, HashDB::Marshal *marshal) {
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
  memset((void*)d->chain, 0, nkeys * sizeof(KeyChain));
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
  memset((void*)d->chain, 0, nkeys * sizeof(KeyChain));
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

int Gen::read_element(Index *i, uint64_t key, Vec<HashDB::Extent> &hit) {
  Data *d = read_data(i);
  if (d == BAD_DATA) return -1;
  if (!d) return 0;
  for (uint32_t x = 0; x < d->nkeys; x++) {
    if (d->chain[x].key == key) {
      hit.push_back(HashDB::Extent());
      HashDB::Extent &e = hit.back();
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

int Gen::read(uint64_t key, Vec<HashDB::Extent> &hit) {
  int r = 0;
  Vec<Index> rd;
  pthread_mutex_lock(&mutex);
  find_indexes(key, rd);
  if (!rd.size()) r = 2;
  for (size_t x = 0; x < rd.size(); x++) r = read_element(&rd[x], key, hit) | r;
  pthread_mutex_unlock(&mutex);
  return r;
}

int Gen::next(uint64_t key, Data *d, Vec<HashDB::Extent> &hit) {
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
