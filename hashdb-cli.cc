#include "hashdb.h"
#include "hdb.h"
#include "hashdb_internal.h"
#include "slice.h"
#include "gen.h"
#include "blake3.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <span>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <random>

using namespace std;

uint64_t parse_key(const string &key_str) {
  uint64_t key = 0;
  if (key_str.length() <= 8) {
    memcpy(&key, key_str.data(), key_str.length());
  } else {
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, key_str.data(), key_str.length());
    uint8_t hash[BLAKE3_OUT_LEN];
    blake3_hasher_finalize(&hasher, hash, BLAKE3_OUT_LEN);
    memcpy(&key, hash, sizeof(key));
  }
  return key;
}

void run_benchmark(HashDB *db, const string &name, bool is_write, int payload_size, vector<uint64_t> &keys) {
  cout << "Running benchmark: " << name << " for 5 seconds..." << endl;

  auto start_time = chrono::steady_clock::now();
  auto end_time = start_time + chrono::seconds(5);

  string payload(payload_size, 'x');
  uint64_t ops = 0;
  uint64_t bytes = 0;

  mt19937_64 rng(1337);

  while (chrono::steady_clock::now() < end_time) {
    if (is_write) {
      uint64_t key = rng();
      if (db->write(key, payload.data(), payload.size(), HashDB::SyncMode::Async) == 0) {
        keys.push_back(key);
        ops++;
        bytes += payload.size();
      }
    } else {
      if (keys.empty()) break;  // Cannot read if no keys were captured
      uint64_t key = keys[rng() % keys.size()];
      vector<HashDB::Extent> hits;
      if (db->read(key, hits) == 0 && !hits.empty()) {
        for (auto &h : hits) db->free_chunk(h.data);
        ops++;
        bytes += payload.size();
      }
    }
  }

  auto actual_end = chrono::steady_clock::now();
  chrono::duration<double> elapsed = actual_end - start_time;

  double ops_per_sec = ops / elapsed.count();
  double mb_per_sec = (bytes / (1024.0 * 1024.0)) / elapsed.count();

  cout << "  " << name << " results:\n"
       << "    Operations: " << ops << "\n"
       << "    Elapsed   : " << elapsed.count() << " s\n"
       << "    Ops/sec   : " << ops_per_sec << "\n"
       << "    Throughput: " << mb_per_sec << " MB/s\n\n";
}

void print_help() {
  cout << "Usage: hashdb-cli [-c config_file] <command> [args...]\n"
       << "\n"
       << "Commands:\n"
       << "  help\n"
       << "      Print this help message.\n"
       << "  init <size_mb> <path1> [path2...]\n"
       << "      Initialize a new database across the provided paths with maximum size `size_mb` MB each.\n"
       << "      This writes the config file specifying the slice parameters.\n"
       << "  status\n"
       << "      Open the DB in read-only mode, print metadata (size, phase), and dump debug info.\n"
       << "  verify\n"
       << "      Run verify() on the database to check the integrity of every Gen.\n"
       << "  benchmark\n"
       << "      Run read/write benchmarks for 5 seconds each with 100-byte and 256KB payloads.\n"
       << "  read <key>\n"
       << "      Read the given key (hex or decimal) and print its content as a string.\n"
       << "  write <key> <string_value>\n"
       << "      Write the given string value to the specified key.\n"
       << "  delete <key>\n"
       << "      Delete the specified key from the database.\n"
       << "\n"
       << "Configuration:\n"
       << "  -c <config_file>  Specify a custom configuration file (default is 'hashdb.config').\n"
       << "                    The config file contains lines of the form: `slice <path> <size_bytes>`.\n";
}

int main(int argc, char **argv) {
  if (argc < 2) {
    print_help();
    return 1;
  }

  string config_file = "hashdb.config";
  int arg_idx = 1;

  if (strcmp(argv[arg_idx], "-c") == 0) {
    if (argc < 4) {
      cerr << "Error: -c option requires a config file argument and a command.\n";
      print_help();
      return 1;
    }
    config_file = argv[arg_idx + 1];
    arg_idx += 2;
  }

  string command = argv[arg_idx];

  if (command == "help") {
    print_help();
    return 0;
  } else if (command == "init") {
    if (arg_idx + 2 >= argc) {
      cerr << "Error: init command requires <size_mb> and at least one <path>.\n";
      return 1;
    }

    uint64_t size_mb = strtoull(argv[arg_idx + 1], nullptr, 0);
    if (size_mb == 0) {
      cerr << "Error: size_mb must be greater than 0.\n";
      return 1;
    }

    uint64_t size_bytes = size_mb * 1024 * 1024;

    ofstream config_out(config_file);
    if (!config_out.is_open()) {
      cerr << "Error: Could not open config file " << config_file << " for writing.\n";
      return 1;
    }

    auto db = HashDB::create();
    for (int i = arg_idx + 2; i < argc; ++i) {
      string path = argv[i];
      config_out << "slice " << path << " " << size_bytes << "\n";
      db->slice(path.c_str(), size_bytes, true);  // init = true
      cout << "Initialized slice at " << path << " with size " << size_mb << " MB\n";
    }
    config_out.close();
    cout << "Wrote configuration to " << config_file << "\n";

    return 0;
  }

  // Determine if read-only
  int open_options = 0;
  if (command == "status" || command == "read") {
    open_options |= HashDB::HDB_READ_ONLY;
  }

  auto db = HashDB::create();

  // Read config file
  ifstream config_in(config_file);
  if (!config_in.is_open()) {
    cerr << "Error: Could not open config file " << config_file << ".\n";
    cerr << "Please run 'init' first, or specify a valid config file with '-c'.\n";
    return 1;
  }

  string type, path;
  uint64_t size;
  int num_slices = 0;
  while (config_in >> type >> path >> size) {
    if (type == "slice") {
      db->slice(path.c_str(), size, false);  // init = false
      num_slices++;
    }
  }
  config_in.close();

  if (num_slices == 0) {
    cerr << "Error: No slices found in config file " << config_file << ".\n";
    return 1;
  }

  if (db->open(open_options) != 0) {
    cerr << "Error: Failed to open database.\n";
    return 1;
  }

  if (command == "status") {
    cout << "Database opened successfully in read-only mode.\n";
    HDB *hdb = (HDB *)db.get();
    cout << "\nHashDB Status:\n"
         << "--------------\n"
         << "init_data_per_index_: " << hdb->init_data_per_index_ << "\n"
         << "write_buffer_size_  : " << hdb->write_buffer_size_ << "\n"
         << "concurrency_        : " << hdb->concurrency_ << "\n"
         << "sync_wait_msec_     : " << hdb->sync_wait_msec_ << "\n"
         << "chain_collisions_   : " << hdb->chain_collisions_ << "\n"
         << "check_hash_         : " << hdb->check_hash_ << "\n"
         << "read_only_          : " << hdb->read_only_ << "\n"
         << "init_generations_   : " << hdb->init_generations_ << "\n"
         << "threads             : " << (hdb->thread_pool_allocated_ ? hdb->thread_pool_max_threads_ : 0) << "\n\n";

    for (const auto &s : hdb->slice_) {
      cout << "Slice " << s->islice_ << ":\n"
           << "  pathname_       : " << (s->pathname_ ? s->pathname_ : "(null)") << "\n"
           << "  layout_pathname_: " << (s->layout_pathname_ ? s->layout_pathname_ : "(null)") << "\n"
           << "  size_           : " << s->size_ << "\n"
           << "  block_size_     : " << s->block_size_ << "\n"
           << "  is_raw_         : " << s->is_raw_ << "\n"
           << "  is_file_        : " << s->is_file_ << "\n"
           << "  is_dir_         : " << s->is_dir_ << "\n\n";

      for (const auto &g : s->gen_) {
        cout << "  Gen " << g->igen_ << ":\n"
             << "    size_           : " << g->size_ << "\n"
             << "    buckets_        : " << g->buckets_ << "\n"
             << "    index_parts_    : " << g->index_parts_ << "\n"
             << "    header_offset_  : " << g->header_offset_ << "\n"
             << "    index_offset_   : " << g->index_offset_ << "\n"
             << "    data_offset_    : " << g->data_offset_ << "\n";

        if (g->header_) {
          cout << "    Header:\n"
               << "      write_position_: " << g->header_->write_position_ << "\n"
               << "      write_serial_  : " << g->header_->write_serial_ << "\n"
               << "      phase_         : " << g->header_->phase_ << "\n"
               << "      data_per_index_: " << g->header_->data_per_index_ << "\n"
               << "      size_          : " << g->header_->size_ << "\n"
               << "      generations_   : " << g->header_->generations_ << "\n";
        }
        cout << "\n";
      }
    }

    db->dump_debug();
  } else if (command == "verify") {
    cout << "Running verify() on the database...\n";
    int res = db->verify();
    if (res == 0) {
      cout << "Database verification completed successfully.\n";
    } else {
      cerr << "Error: Database verification failed with code " << res << ".\n";
      return 1;
    }
  } else if (command == "benchmark") {
    vector<uint64_t> small_keys;
    vector<uint64_t> large_keys;
    run_benchmark(db.get(), "Write Small (100 bytes)", true, 100, small_keys);
    run_benchmark(db.get(), "Read Small (100 bytes)", false, 100, small_keys);
    run_benchmark(db.get(), "Write Large (256 KB)", true, 256 * 1024, large_keys);
    run_benchmark(db.get(), "Read Large (256 KB)", false, 256 * 1024, large_keys);
  } else if (command == "read") {
    if (arg_idx + 1 >= argc) {
      cerr << "Error: read command requires <key>.\n";
      return 1;
    }
    uint64_t key = parse_key(argv[arg_idx + 1]);
    vector<HashDB::Extent> hits;
    int res = db->read(key, hits);
    if (res == 0 && !hits.empty()) {
      cout << "Key: " << key << " (" << std::hex << "0x" << key << std::dec << ")\n";
      for (size_t i = 0; i < hits.size(); ++i) {
        cout << "Hit " << i << " (length " << hits[i].len << "): ";
        if (hits[i].data) {
          cout << string((char *)hits[i].data, hits[i].len);
        } else {
          cout << "(null data)";
        }
        cout << "\n";
        db->free_chunk(hits[i].data);
      }
    } else {
      cout << "Key " << key << " not found.\n";
    }
  } else if (command == "write") {
    if (arg_idx + 2 >= argc) {
      cerr << "Error: write command requires <key> and <string_value>.\n";
      return 1;
    }
    uint64_t key = parse_key(argv[arg_idx + 1]);
    string value = argv[arg_idx + 2];
    int res = db->write(key, (void *)value.c_str(), value.length() + 1, HashDB::SyncMode::Sync);
    if (res == 0) {
      cout << "Successfully wrote key " << key << ".\n";
    } else {
      cerr << "Error: Failed to write key " << key << ".\n";
      return 1;
    }
  } else if (command == "delete") {
    if (arg_idx + 1 >= argc) {
      cerr << "Error: delete command requires <key>.\n";
      return 1;
    }
    uint64_t key = parse_key(argv[arg_idx + 1]);
    vector<HashDB::Extent> hits;
    int res = db->read(key, hits);
    if (res == 0 && !hits.empty()) {
      for (auto &h : hits) {
        int del_res = db->remove(h.data, HashDB::SyncMode::Sync);
        if (del_res == 0) {
          cout << "Successfully deleted element for key " << key << ".\n";
        } else {
          cerr << "Error: Failed to delete element for key " << key << ".\n";
        }
        db->free_chunk(h.data);
      }
    } else {
      cout << "Key " << key << " not found for deletion.\n";
    }
  } else {
    cerr << "Error: Unknown command '" << command << "'.\n";
    print_help();
    return 1;
  }

  db->close();
  return 0;
}
