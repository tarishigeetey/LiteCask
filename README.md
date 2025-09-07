# 📦 LiteCask

LiteCask is a **lightweight key-value store** in Java, inspired by [Bitcask](https://riak.com/assets/bitcask-intro.pdf).  
It uses an **append-only log** and an **in-memory index (KeyDir)** for fast reads and writes.  

---

## ✨ Features
- Append-only writes (no in-place updates)  
- In-memory index for O(1) lookups  
- Tombstone deletes (delete by writing a marker)  
- File rotation when data files grow too large  
- Merge (compaction) to reclaim space  
- Hint files for faster startup  
- Checkpointing for near-instant recovery  
- Interactive CLI (`put`, `get`, `delete`, `list`, `merge`)  
- REST API to expose LiteCask as a service  

---

## 🧩 Record Format

| Field       | Size    | Description                     |
|-------------|---------|---------------------------------|
| keyLength   | 4 bytes | Length of the key (int)         |
| valueLength | 4 bytes | Length of the value (int)       |
| flag        | 1 byte  | 0 = PUT, 1 = TOMBSTONE (delete) |
| key         | var     | UTF-8 encoded key bytes         |
| value       | var     | Raw value bytes                 |

---

 ## ⚡ Performance Considerations

LiteCask follows the **append-only log-structured** model, which has some important performance tradeoffs:

- **Fast Writes**  
  - Appending to a log file is sequential I/O → much faster than random disk updates.  
  - Each `put` or `delete` only appends bytes and updates the in-memory `KeyDir`.  

- **Fast Reads (O(1))**  
  - `get(key)` looks up metadata in `KeyDir` and jumps directly to the value’s file/offset.  
  - No need to scan files for reads in steady state.  

- **Deletes are Cheap**  
  - A delete writes a small tombstone entry instead of removing data in place.  
  - Real space is reclaimed later during **merge/compaction**.  

- **Recovery Tradeoffs**  
  - Without optimizations, startup requires scanning all `.dat` files → slow if 1000s of files.  
  - **Hint files** and **checkpointing** greatly reduce recovery time (to near-instant).  
  - In the worst case (no checkpoint, no hints), recovery falls back to full file scans.  

- **Compaction/Merge**  
  - Over time, old overwritten values and tombstones accumulate.  
  - `merge()` rewrites only the latest values, shrinking storage and improving read locality.  
  - Merge is I/O heavy and should be scheduled carefully in production use.  

- **Concurrency**  
  - Current implementation supports **single writer, multiple readers**.  
  - Multi-threaded recovery (parallel scanning) is planned to speed up startup further.  

---

## 📖 Background
LiteCask is a learning project inspired by the Bitcask storage engine, originally designed for Riak by Basho.
It’s not production-ready, but is a great way to understand log-structured storage engines:
- Append-only logs avoid random writes.
- An in-memory index (KeyDir) maps keys → file, offset, size.
- Tombstones handle deletes.
- Periodic merges clean up old data.

---


