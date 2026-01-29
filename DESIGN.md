# HashDB Design Document

## Architecture Overview

HashDB is a high-performance, persistent key-value store optimized for concurrent read/write operations. It employs a hierarchical architecture designed for scalability and durability.

### Core Architecture Components

1.  **HDB (Hash Database)**: The top-level interface. It manages a collection of *Slices* and coordinates concurrent operations.
2.  **Slice**: A physical partition of the database. Each slice maps to a file or raw device. Slices allow for horizontal scaling and parallel I/O.
3.  **Gen (Generation)**: A temporal version within a Slice. Generations enable data management features like log-structured writes, snapshots, and potentially garbage collection (though primarily focused on appending and indexing).

## Data structures

### On-Disk Structures (Persistent)

*   **Header**: Stores metadata about a [Gen](gen.h#139-140), including write positions, versions, and configuration.
    *   `magic`: Verification signature.
    *   [write_position](hashdb.cc#532-536): Current append point in the log.
    *   [phase](gen.h#71-72): Flips (0/1) to detect partial writes during recovery.

*   **Index**: A 64-bit entry describing a key's location.
    *   [offset](gen.cc#539-551): Block offset of the data.
    *   [tag](hashdb_internal.h#241-242): 16-bit hash prefix for quick filtering.
    *   [size](hashdb_internal.h#200-201): Size of the data chunk.
    *   [next](gen.cc#1314-1326): Linkage for collision chains or overflow buckets.
    *   [phase](gen.h#71-72): Parity bit for consistency checking.

*   **Data Layout in [Gen](gen.h#139-140)**:
    1.  **Header**: At the start of the file.
    2.  **Index Area**: A hash table region mapping keys to Data offsets. Organized into *Sectors*, *Buckets*, and *Overflow Areas*.
    3.  **Log/Data Area**: Append-only log containing [LogHeaderFooter](hashdb_internal.h#185-196), [LogEntry](hashdb_internal.h#197-204) (metadata), and [Data](hashdb_internal.h#210-226) (payload).

*   **Data Chunk**:
    *   `magic`, [length](hashdb_internal.h#135-136), `nkeys`: Metadata.
    *   [KeyChain](hashdb_internal.h#205-209): List of keys associated with this data block (supporting aliases).
    *   Payload: The actual value bytes.

*   **LogEntry**:
    *   Records batch persistent updates (insertions/removals) for recovery.
    *   Contains keys and associated indices.

### In-Memory Structures (Volatile)

*   **WriteBuffer**: Buffers data before flushing to the persistent log. Each [Gen](gen.h#139-140) has double-buffered write buffers (`wbuf`).
*   **LookasideCache**: A small, fast hash table ([NBlockHash](hashdb_internal.h#80-113)) that tracks recent updates/deletions not yet fully committed to the persistent [Index](hashdb_internal.h#161-162). This bridges the gap between [write](hashdb.cc#627-630) and [sync](hashdb.cc#139-152), ensuring read-your-writes consistency.

## Lifecycle

### 1. Initialization and Open
*   **Init**: `HDB::slice()` creates [Slice](slice.cc#31-108) objects. `Slice::init()` creates [Gen](gen.h#139-140) objects.
*   **Open**: `HDB::open()` iterates through slices and generations to parallelize loading. Each [Gen](gen.h#139-140) memory-maps its index and header.
*   **Recovery**: If a crash is detected (mismatched phases/serials), `Gen::recovery()` scans the log from the last known good commit point to replay [LogEntry](hashdb_internal.h#197-204) records and rebuild the [Lookaside](hashdb_internal.h#166-173) or [Index](hashdb_internal.h#161-162).

### 2. Write Path (`HashDB::write`)
1.  **Hash computation**: Key is hashed to determine Slice and Bucket.
2.  **Buffering**: Data is appended to the current [WriteBuffer](gen.h#9-26).
3.  **Lookaside Insertion**: A mapping is added to the `LookasideCache` so the data is immediately visible.
4.  **Log Flush (Async/Sync)**:
    *   When the buffer fills or [sync](hashdb.cc#139-152) is requested, the [WriteBuffer](gen.h#9-26) is formatted into [Data](hashdb_internal.h#210-226) chunks and written to disk.
    *   A [LogEntry](hashdb_internal.h#197-204) is created to record the new indices.
5.  **Index Update**: Once data is durably on disk, the persistent [Index](hashdb_internal.h#161-162) (memory-mapped) is updated. The [Lookaside](hashdb_internal.h#166-173) entry is removed.

### 3. Read Path (`HashDB::read`)
1.  **Slice & Gen Selection**: Key determines the [Slice](slice.cc#31-108) (usually via modulo or consistent hashing, though currently simple modulo in `HDB::read`).
2.  **Lookaside Check**: First, check `LookasideCache`. If found (and not deleted), return data.
3.  **Index Lookup**:
    *   Calculate bucket: `key % buckets` (ModuloHash).
    *   Scan [Index](hashdb_internal.h#161-162): Check primary bucket and overflow chains.
    *   Verify `Tag`: Use `KEY2TAG` (XorHash high bits) to filter collisions.
4.  **Data Retrieval**: If index match found, use [offset](gen.cc#539-551) and [size](hashdb_internal.h#200-201) to read [Data](hashdb_internal.h#210-226) payload from disk. Verify collisions using the full key stored in [KeyChain](hashdb_internal.h#205-209).

### 4. Remove Path
*   Writes a "remove" record to the log.
*   Updates [Lookaside](hashdb_internal.h#166-173) with a "deleted" marker (`next=1`).
*   Eventually updates [Index](hashdb_internal.h#161-162) to size 0 to reclaim the slot.

## Concurrency
*   **Thread Pool**: [HDB](hdb.h#11-36) manages a thread pool for parallel operations across slices.
*   **Locking**:
    *   `HDB::mutex`: Protects global state (e.g., list of slices).
    *   `Gen::mutex`: Granular locking per Generation for index/log access.
*   **Sync Thread**: A background thread ([sync_main](hashdb.cc#139-152)) wakes up periodically to flush buffers and commit index changes to disk.

## Storage Format Constants
*   `SECTOR_SIZE`: 512 bytes (aligned for atomic sector writes).
*   `DATA_BLOCK_SIZE`: 512 bytes (unit of allocation).
*   `BUCKETS_PER_SECTOR`: 6.
*   `ELEMENTS_PER_SECTOR`: 64 (Index entries).
