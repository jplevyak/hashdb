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
#include <unistd.h>
#include <pthread.h>
#include <stdio.h>
#include <cstdarg>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <limits>
#include <cstring>

// Static members of HashDB
HashDB::Callback *HashDB::SYNC = (HashDB::Callback *)(uintptr_t)1;
HashDB::Callback *HashDB::FLUSH = (HashDB::Callback *)(uintptr_t)2;
HashDB::Callback *HashDB::ASYNC = (HashDB::Callback *)(uintptr_t)0;
#define SPECIAL_CALLBACK(_c) (((uintptr_t)(_c)) <= 2)

// HDB Implementation

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
  Vec<HashDB::Extent> hit;
  int found;  // only for master (0)
  ssize_t result;
  HashDB::Callback *callback;

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

struct Writer {
  Slice *s;
  uint64_t *key;
  int nkeys;
  HashDB::Marshal *marshal;
  void *old_data;
  pthread_barrier_t *barrier;
  ssize_t result;
  HashDB::Callback *callback;

  void init(Slice *as, uint64_t *akey, int ankeys, HashDB::Marshal *amarshal, pthread_barrier_t *abarrier) {
    s = as;
    key = akey;
    nkeys = ankeys;
    marshal = amarshal;
    barrier = abarrier;
    result = 0;
  }
};

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

void hashdb_print_data_header(void *p) {
  Data *d = PTR_TO_DATA(p);
  printf("offset %u phase %d remove %d\n", d->offset, d->phase, d->remove);
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
