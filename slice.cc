/*
  Copyright 2003-2022 John Plevyak, All Rights Reserved
*/

#include "slice.h"
#include "hdb.h"
#include "gen.h"

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
#ifdef linux
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/hdreg.h>
// #include "hdparm.h"
#endif

void fail(const char *s, ...);

Slice::Slice(HDB *ahdb, int aislice, const char *alayout_pathname, uint64_t alayout_size) {
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
#ifndef O_DIRECT
#define O_DIRECT 0
#endif
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

int Slice::init() {
  int res = 0;
  for (auto g : gen)
    if (!res)
      res = g->init();
    else
      g->init();
  return res;
}

int Slice::open() {
  int res = 0;
  for (auto g : gen)
    if (!res)
      res = g->open();
    else
      g->open();
  return res;
}

int Slice::read(uint64_t key, std::vector<HashDB::Extent> &hit) {
  int r = 0;
  for (auto g : gen) r = g->read(key, hit) | r;
  return r;
}

int Slice::might_exist(uint64_t key) {
  int buckets = gen[0]->buckets;
  int b = key % buckets;
  uint16_t tag = KEY2TAG(key);
  for (auto g : gen) {
    g->mutex.lock();
    foreach_contiguous_element(g, e, b, tmp) {
      Index *i = g->index(e);
      if (i->tag == tag && i->size) return 1;
    }
    foreach_overflow_element(g, e, b, tmp) {
      Index *i = g->index(e);
      if (i->tag == tag && i->size) return 1;
    }
    g->mutex.unlock();
  }
  return 0;
}

int Slice::write(uint64_t *key, int nkeys, HashDB::Marshal *marshal, HashDB::SyncMode mode) {
  Gen *g = gen[0];
  g->mutex.lock();
  int res = g->write(key, nkeys, marshal);
  if (!res) {
    if (mode == HashDB::SyncMode::Sync || mode == HashDB::SyncMode::Flush) {
      WriteBuffer *b = &g->wbuf[g->cur_write];
      if (mode == HashDB::SyncMode::Flush) {
        g->write_buffer();
        // wait_for_write_to_complete(b); // TODO: need to make this visible or reimplement?
        // Wait for write to complete logic
        std::unique_lock<std::mutex> lock(b->gen->mutex, std::adopt_lock);
        while (b->writing) b->gen->write_condition.wait(lock);
        lock.release();
      } else {
        // wait_for_flush(b, hdb->sync_wait_msec);
        // Wait for flush logic
        int wait_msec = hdb->sync_wait_msec;
        uint64_t o = b->next_offset;
        // Gen *g = b->gen; // Already have g
        auto startt = std::chrono::high_resolution_clock::now();
        auto donet = startt + std::chrono::milliseconds(wait_msec);
        // Note: we need to handle the lock carefully here for timedwait
        while (std::chrono::high_resolution_clock::now() < donet && b->next_offset == o && b->start != b->cur) {
          // pthread_cond_timedwait expects mutex locked.
          // b->gen->mutex is g->mutex which IS locked.
          std::unique_lock<std::mutex> lock(g->mutex, std::adopt_lock);
          auto now = std::chrono::high_resolution_clock::now();
          auto remaining = donet - now;
          if (remaining.count() > 0) g->write_condition.wait_for(lock, remaining);
          lock.release();  // release ownership
        }
        if (b->next_offset == o && b->start != b->cur) {
          if (!b->writing) g->write_buffer();
          std::unique_lock<std::mutex> lock(b->gen->mutex, std::adopt_lock);
          while (b->writing) b->gen->write_condition.wait(lock);
          lock.release();
        }
      }
    }
  }
  g->mutex.unlock();
  return res;
}

int Slice::verify() {
  int res = 0;
  for (auto g : gen)
    if (!res)
      res = g->verify();
    else
      g->verify();
  return res;
}

int Slice::close() {
  for (auto g : gen) {
    g->close();
    DELETE(g);
  }
  return 0;
}
