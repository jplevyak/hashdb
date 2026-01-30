/*
  Copyright 2003-2022 John Plevyak, All Rights Reserved
*/

#include "hdb.h"
#include "slice.h"
#include "gen.h"
#include "prime.h"

#include <cerrno>
#include <cinttypes>
#include <assert.h>
#include <stdlib.h>
#include <new>
#include <new>
#include <unistd.h>
#include <stdio.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstdarg>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <limits>
#include <cstring>

#define SPECIAL_CALLBACK(_c) 0


void fail(const char *s, ...);

HashDB::HashDB() {}

HDB::HDB() {}

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

void HDB::free_slices() {
  slice.clear();
}

struct SafeLatch {
  std::mutex m;
  std::condition_variable cv;
  int count;
  SafeLatch(int n) : count(n) {}
  void count_down() {
    std::unique_lock<std::mutex> lock(m);
    if (--count == 0) {
      cv.notify_all();
    }
  }
  void wait() {
    std::unique_lock<std::mutex> lock(m);
    while (count > 0) {
      cv.wait(lock);
    }
  }
};

struct Doer {
  Slice *s;
  SafeLatch *latch;
  int res;

  void init(Slice *as, SafeLatch *alatch) {
    s = as;
    latch = alatch;
    res = 0;
  }
};

int HDB::foreach_slice(void *(*pfn)(Doer *)) {
  SafeLatch latch(slice.size());
  std::vector<Doer> doer(slice.size());
  int res = 0;
  for (size_t i = 0; i < slice.size(); i++) {
    doer[i].init(slice[i].get(), &latch);
    thread_pool->add_job((void *(*)(void *))pfn, (void *)&doer[i]);
  }
  latch.wait();
  for (size_t i = 0; i < slice.size(); i++) res |= doer[i].res;
  return res;
}

#define DO_SLICE(_op)              \
  static void *do_##_op(Doer *d) { \
    d->res = d->s->_op();          \
    d->latch->count_down();        \
    return nullptr;                \
  }                                \
  static int _op##_slices(HDB *hdb) { return hdb->foreach_slice(do_##_op); }

DO_SLICE(init);
DO_SLICE(open);
DO_SLICE(verify);
DO_SLICE(close);

std::unique_ptr<HashDB> HashDB::create() { return std::make_unique<HDB>(); }

int HashDB::slice(const char *pathname, uint64_t size, bool init) {
  HDB *hdb = ((HDB *)this);
  hdb->mutex.lock();
  auto s = std::make_unique<Slice>(hdb, hdb->slice.size(), pathname, size);
  for (int i = 0; i < hdb->init_generations; i++) s->gen.push_back(std::make_unique<Gen>(s.get(), i));
  if (init) s->init();
  hdb->slice.push_back(std::move(s));
  hdb->mutex.unlock();
  return 0;
}

static void *sync_main(void *data) {
  HDB *hdb = (HDB *)data;
  std::unique_lock<std::mutex> lock(hdb->mutex);
  while (1) {
    if (hdb->sync_condition.wait_for(lock, std::chrono::seconds(SYNC_PERIOD)) == std::cv_status::timeout) {
      continue;
    }
    if (hdb->exiting) break;
    for (const auto &s : hdb->slice)
      for (const auto &g : s->gen) g->periodic_sync();
  }
  return nullptr;
}

int HashDB::open(int aread_only) {
  HDB *hdb = ((HDB *)this);
  hdb->read_only = aread_only;
  assert(reinit_on_open_error == false);
  assert(hdb->separate_db_per_slice == true);
  assert(hdb->replication_factor == 0);
  if (!thread_pool) {
    hdb->thread_pool_max_threads = concurrency * hdb->slice.size();
    thread_pool = std::make_unique<ThreadPool>(hdb->thread_pool_max_threads);
    hdb->thread_pool_allocated = 1;
  } else
    hdb->thread_pool_allocated = 0;
  int r = open_slices(hdb);
  if (!r) {
    assert(!hdb->sync_thread.joinable());
    hdb->sync_thread = ThreadPool::thread_create(sync_main, this);
  }
  return r;
}

int HashDB::reinitialize() {
  HDB *hdb = ((HDB *)this);
  return init_slices(hdb);
}

int HashDB::read(uint64_t key, std::vector<Extent> &hit) {
  HDB *hdb = ((HDB *)this);
  int r = 0;
  for (const auto &s : hdb->slice) r = s->read(key, hit) | r;
  return 0;
}

int HashDB::next(uint64_t key, void *old_data, std::vector<Extent> &hit) {
  HDB *hdb = ((HDB *)this);
  Data *d = PTR_TO_DATA(old_data);
  if (d->magic != DATA_MAGIC) return -1;
  if ((int)d->slice >= hdb->slice.size()) return -1;
  Slice *s = hdb->slice[d->slice].get();
  if ((int)d->gen >= s->gen.size()) return -1;
  Gen *g = s->gen[d->gen].get();
  int r = 0;
  g->mutex.lock();
  r = g->next(key, d, hit);
  g->mutex.unlock();
  return r;
}

struct Reader {
  Slice *s;
  uint64_t key;
  std::atomic<int> *pending_count;
  Reader *head;
  std::vector<HashDB::Extent> hit;
  int found;  // only for master (0)
  ssize_t result;
  HashDB::ReadCallback callback;

  void init(Slice *as, uint64_t akey) {
    s = as;
    key = akey;
    hit.clear();
    result = 0;
  }
};

static void *do_read_job(Reader *r) {
  if (r->head->found > 0) {
    r->result = r->s->read(r->key, r->hit) | r->result;
  }
  if (r->pending_count->fetch_sub(1) == 1) {
    Reader *head = r->head;
    std::vector<HashDB::Extent> hits;
    ssize_t total_result = 0;
    for (int i = 0; i < head->found; i++) {
        hits.insert(hits.end(), head[i].hit.begin(), head[i].hit.end());
        total_result |= head[i].result;
    }
    // For found==0 case, head->found is 0, loops don't run, hits empty. result is head[0].result (set to 1).
    if (head->found == 0) total_result = head[0].result;
    
    if (head->callback) head->callback(total_result, hits);
    delete r->pending_count;
    delete[] head;
  }
  return nullptr;
}

int HashDB::read(uint64_t key, ReadCallback callback, bool immediate_miss) {
  HDB *hdb = ((HDB *)this);
  int n = hdb->slice.size(), found = 0;
  std::vector<Reader> reader(n);
  for (int i = 0; i < n; i++) {
    if (hdb->slice[i]->might_exist(key)) reader[found++].init(hdb->slice[i].get(), key);
  }
  if (immediate_miss && !found) return 1;
  int size = (found ? found : 1);
  Reader *areader = new Reader[size];
  for (int i = 0; i < found; i++) areader[i] = reader[i];
  
  std::atomic<int> *pending = new std::atomic<int>(size);
  
  areader[0].s = hdb->slice[0].get();
  areader[0].found = found;
  areader[0].callback = callback;
  areader[0].result = !found;

  for (int i = 0; i < size; i++) {
      areader[i].pending_count = pending;
      areader[i].head = areader;
      hdb->thread_pool->add_job((void *(*)(void *))do_read_job, (void *)&areader[i]);
  }
  return 0;
}

struct Writer {
  Slice *s;
  uint64_t *key;
  int nkeys;
  uint64_t value_len;
  HashDB::SerializeFn serializer;
  void *old_data;
  std::atomic<int> *pending_count;
  Writer *head;
  ssize_t result;
  HashDB::WriteCallback callback;
  HashDB::SyncMode mode;

  void init(Slice *as, uint64_t *akey, int ankeys, uint64_t avalue_len, HashDB::SerializeFn aserializer,
            std::atomic<int> *apending) {
    s = as;
    key = akey;
    nkeys = ankeys;
    value_len = avalue_len;
    serializer = aserializer;
    pending_count = apending;
    result = 0;
  }
};

static void *do_write_job(Writer *w) {
#ifdef INCOMPLETE_REPLICATION_CODE
  // Replication code logic would go here, adapted for job-based execution
#endif
  w->result = w->s->write(w->key, w->nkeys, w->value_len, w->serializer, w->mode);
  
  if (w->pending_count->fetch_sub(1) == 1) {
    Writer *head = w->head;
    ssize_t total_result = 0;
    // Aggregate results from multiple writes (if replication is enabled)

    int n = 1; // Default
#ifdef INCOMPLETE_REPLICATION_CODE
     n = w->s->hdb->replication_factor;
     if (!n) n = 1;
#endif
    for(int i=0; i<n; ++i) total_result |= head[i].result;

    if (head->callback) head->callback(total_result);


    delete head->pending_count;
    delete[] head;
  }
  return nullptr;
}

int HashDB::write(uint64_t *key, int nkeys, uint64_t value_len, SerializeFn serializer, WriteCallback callback,
                  SyncMode mode) {
  HDB *hdb = ((HDB *)this);
#ifdef HELGRIND
  hdb->mutex.lock();
#endif
  int s = hdb->current_write_slice;
  hdb->current_write_slice = s + 1;  // race is OK, just looking for distribution
#ifdef HELGRIND
  hdb->mutex.unlock();
#endif
  if (!callback) return hdb->slice[s % hdb->slice.size()]->write(key, nkeys, value_len, serializer, mode);
#ifdef INCOMPLETE_REPLICATION_CODE
  {
    int r = 0;
    assert(!"replication not implemented");
    if (mode == SyncMode::Async) {
      for (int i = 0; i < r; i++)  // incomplete
        r = hdb->slice[(s + i) % hdb->slice.size()]->write(key, nkeys, value_len, serializer, mode) | r;
    } else {
      barrier_t barrier;
      std::vector<Writer> writer(r);
      barrier_init(&barrier, n);
      for (int i = 0; i < r; i++) {  // incomplete
        writer[i].init(hdb->slice[(s + i) % hdb->slice.size()], key, nkeys, value_len, serializer, &barrier);
        hdb->thread_pool->add_job((void *(*)(void *))do_write, (void *)&writer[i]);
      }
      barrier_wait(&barrier);
      r = writer[0].result;
    }
    return r;
  }
#endif
  int nn = 1;
  Writer *awriter = new Writer[nn];
  std::atomic<int> *pending = new std::atomic<int>(nn);

  awriter[0].s = hdb->slice[s].get();
  awriter[0].callback = callback;
  awriter[0].mode = mode;
  awriter[0].key = key;
  awriter[0].nkeys = nkeys;
  awriter[0].value_len = value_len;
  awriter[0].serializer = serializer;
  awriter[0].pending_count = pending;
  awriter[0].head = awriter;
  awriter[0].old_data = nullptr;
  awriter[0].result = 0;
  hdb->thread_pool->add_job((void *(*)(void *))do_write_job, (void *)&awriter[0]);
  return 0;
}

static inline int cmp_wpos(uint64_t wpos1, int phase1, uint64_t wpos2, int phase2) {
  if (phase1 == phase2) return wpos1 < wpos2 ? -1 : ((wpos1 == wpos2) ? 0 : 1);
  return wpos1 > wpos2 ? -1 : ((wpos1 == wpos2) ? 0 : 1);
}

static inline void wait_for_write_commit(Gen *g, uint64_t wpos, int phase) {
  while (cmp_wpos(wpos, phase, g->committed_write_position, g->committed_phase) < 0) {
    WriteBuffer &w = g->wbuf[g->cur_write];
    if (cmp_wpos(wpos, phase, w.offset - g->data_offset + (w.cur - w.start), w.phase) < 0) g->write_buffer();
    std::unique_lock<std::mutex> lock(g->mutex, std::adopt_lock);
    g->write_condition.wait(lock);
    lock.release();
  }
}

static inline void wait_for_log_flush(Gen *g, int wait_msec) {
  uint64_t wpos = g->header->write_position;
  int phase = g->header->phase;
  auto startt = std::chrono::high_resolution_clock::now();
  auto donet = startt + std::chrono::milliseconds(wait_msec);
  while (std::chrono::high_resolution_clock::now() < donet &&
         cmp_wpos(wpos, phase, g->committed_write_position, g->committed_phase) < 0) {
    std::unique_lock<std::mutex> lock(g->mutex, std::adopt_lock);
    auto now = std::chrono::high_resolution_clock::now();
    auto remaining = donet - now;
    if (remaining.count() > 0) g->write_condition.wait_for(lock, remaining);
    lock.release();
  }
  if (cmp_wpos(wpos, phase, g->committed_write_position, g->committed_phase) < 0) {
    wait_for_write_commit(g, g->header->write_position, g->header->phase);
    g->write_log_buffer();
  }
}

static void *do_remove_job(Writer *w) {
  w->result = w->s->hdb->remove(w->old_data, nullptr, w->mode);
  if (w->pending_count->fetch_sub(1) == 1) {
      if (w->callback) w->callback(w->result);
      delete w->pending_count;

      delete_aligned(w->head);
  }
  return nullptr;
}

int HashDB::remove(void *old_data, WriteCallback callback, SyncMode mode) {
  HDB *hdb = ((HDB *)this);
  Data *d = PTR_TO_DATA(old_data);
  if (d->magic != DATA_MAGIC) return -1;
  if ((int)d->slice >= hdb->slice.size()) return -1;
  Slice *s = hdb->slice[d->slice].get();
  if ((int)d->gen >= s->gen.size()) return -1;
  if (!callback) {
    Gen *g = s->gen[d->gen].get();
    int r = 0;
    g->mutex.lock();
    Index dindex(d->offset, 0, d->size, 1, d->phase);
    Index iindex(d->offset, 0, d->size, 0, d->phase);
    std::vector<uint64_t> keys(d->nkeys);
    for (uint32_t j = 0; j < d->nkeys; j++) keys[j] = d->chain[j].key;
    r = g->write_remove(keys.data(), d->nkeys, &dindex) | r;
    for (int i = 0; i < (int)d->nkeys; i++)
      if (!g->delete_lookaside(keys[i], &iindex)) g->insert_lookaside(keys[i], &dindex);
    g->insert_log(keys.data(), d->nkeys, &dindex);
    if (mode == SyncMode::Sync || mode == SyncMode::Flush) {
      if (mode == SyncMode::Flush) {
        wait_for_write_commit(g, g->header->write_position, g->header->phase);
        if (g->lbuf[g->cur_log].start != g->lbuf[g->cur_log].cur) g->write_log_buffer();
      } else
        wait_for_log_flush(g, hdb->sync_wait_msec);
    }
    g->mutex.unlock();
    return r;
  }
  int nn = 1;
  int size = sizeof(Writer) * nn;
  Writer *awriter = (Writer *)new_aligned(size);
  std::atomic<int> *pending = new std::atomic<int>(nn);
  
  awriter[0].s = s;
  awriter[0].callback = callback;
  awriter[0].mode = mode;
  awriter[0].old_data = old_data;
  awriter[0].pending_count = pending;
  awriter[0].head = awriter;
  
  hdb->thread_pool->add_job((void *(*)(void *))do_remove_job, (void *)&awriter[0]);
  return 0;
}

int HashDB::get_keys(void *old_data, std::vector<uint64_t> &keys) {
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
  for (const auto &s : hdb->slice) {
    for (const auto &g : s->gen) {
      printf("Slice %d Gen %d\n", s->islice, g->igen);
      g->dump_debug_log();
    }
  }
  return 0;
}

int HashDB::close() {
  HDB *hdb = ((HDB *)this);
  {
    std::unique_lock<std::mutex> lock(hdb->mutex);
    if (hdb->sync_thread.joinable()) {
      hdb->exiting = 1;
      hdb->sync_condition.notify_all();
      lock.unlock();
      hdb->sync_thread.join();
    }
  }


  int res = close_slices(hdb);
  hdb->mutex.lock();
  for (const auto &s : hdb->slice) {
    s->gen.clear();  // no-race here
    ::close(s->fd);
    if (s->pathname != s->layout_pathname) free(s->pathname);
    s->pathname = nullptr;
    free(s->layout_pathname);
    s->layout_pathname = nullptr;
    s->fd = -1;
  }
  hdb->free_slices();
  if (hdb->thread_pool_allocated) {
    thread_pool.reset();
  }
  hdb->mutex.unlock();
  return res;
}

void HDB::crash() {
  HDB *hdb = ((HDB *)this);
  {
    std::unique_lock<std::mutex> lock(hdb->mutex);
    if (hdb->sync_thread.joinable()) {
      hdb->exiting = 1;
      hdb->sync_condition.notify_all();
      lock.unlock();
      hdb->sync_thread.join();
    }
  }
  // Explicitly do NOT close slices or sync
  // But we might want to close file descriptors to avoid resource leaks in test runner
  hdb->mutex.lock();
  for (const auto &s : hdb->slice) {
    if (s->fd != -1) {
      ::close(s->fd); 
        // Just close FD, no flush
       s->fd = -1;
    }
    // We don't free memory here to simulate "process exit" behavior (OS reclaims)
    // but in a test suite we leak. That's acceptable for a "crash" test helper?
    // User asked for "recovery from non-clean shutdown".
    // If we leak, Valgrind/ASAN might complain.
    // Ideally we should delete everything but *skip the flush*.
    // However, the `Slice` destructor or close path might flush.
    // `Slice::close()` calls `verify()`? No.
    // `Slice` destructor?
    // Let's look at `Slice` in `slice.cc` later if needed.
    // For now, just closing FD simulates the "stop writing" part.
  }
  // Clean up thread pool to avoid it running after we destroy/leak
  if (hdb->thread_pool_allocated) {
      thread_pool.reset();
  }
  hdb->mutex.unlock();
  // We don't delete HDB itself, test caller should handle object life cycle?
  // But caller can't delete it easily if we don't clean up well.
  // Actually, standard `delete db` calls `~HashDB` -> `~HDB`.
  // `~HDB`? It's not virtual destructor in base, so `delete db` only calls `~HashDB`.
  // `HashDB` has no destructor.
  // So `delete db` on `HashDB*` leaks `HDB` members.
  // But standard test code does `delete db`.
  // In our `crash` simulation, we want to emulate the state on disk being "as is".
  // So stopping sync thread and closing FDs is enough.
}

int HashDB::free_chunk(void *ptr) {
  Data *p = PTR_TO_DATA(ptr);
  delete_aligned(p);
  return 0;
}

void hashdb_print_data_header(void *p) {
  Data *d = PTR_TO_DATA(p);
  printf("offset %u phase %d remove %d\n", d->offset, d->phase, d->remove);
}

/* Test functions accessing internal data
 */
void hashdb_print_info(HashDB *dd) {
  HDB *d = (HDB *)dd;
  for (const auto &slice : d->slice) {
    for (const auto &g : slice->gen) {
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
  for (const auto &slice : hdb->slice) {
    for (const auto &g : slice->gen) {
      for (uint32_t b = 0; b < g->buckets; b++) {
        x = 0;
        foreach_contiguous_element(g.get(), e, b, tmp) if (g->index(e)->size) x++;
        assert(x < 9);
        bcount[x]++;
        x = 0;
        foreach_overflow_element(g.get(), e, b, tmp) x++;
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

void fail(const char *str, ...) {
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

int HashDB::write(uint64_t *key, int nkeys, const void *data, int len, WriteCallback callback, SyncMode mode) {
  return write(
      key, nkeys, len,
      [data, len](std::span<uint8_t> buf) {
        memcpy(buf.data(), data, len);
        return len;
      },
      callback, mode);
}

int HashDB::write(uint64_t key, const void *data, int len, WriteCallback callback, SyncMode mode) {
  if (mode == SyncMode::Sync) return write(&key, 1, data, len, callback, mode);
  uint64_t *k = new uint64_t(key);
  return write(
      k, 1, data, len,
      [k, callback](int res) {
        delete k;
        if (callback) callback(res);
      },
      mode);
}

int HashDB::write(uint64_t *key, int nkeys, uint64_t value_len, SerializeFn serializer, SyncMode mode) {
  return write(key, nkeys, value_len, serializer, nullptr, mode);
}

int HashDB::write(uint64_t *key, int nkeys, const void *data, int len, SyncMode mode) {
  return write(key, nkeys, data, len, nullptr, mode);
}

int HashDB::write(uint64_t key, const void *data, int len, SyncMode mode) {
  return write(&key, 1, data, len, nullptr, mode);
}

int HashDB::remove(void *old_data, SyncMode mode) { return remove(old_data, nullptr, mode); }
