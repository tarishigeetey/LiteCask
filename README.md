# 📦 LiteCask

LiteCask is a **lightweight key-value store** in Java, inspired by [Bitcask](https://riak.com/assets/bitcask-intro.pdf).  
It uses an **append-only log** and an **in-memory index (KeyDir)** for fast reads and writes.  

---

## ✨ Features
- Append-only writes (no in-place updates)
- In-memory index for O(1) lookups
- Simple API: `put`, `get`, `delete`, `close`
- Interactive CLI for experimenting
- Tombstone deletes (delete by writing a marker)

*(Planned: file rotation, recovery on restart, compaction/merge)*

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

## 📖 Background
LiteCask is a learning project inspired by the Bitcask storage engine, originally designed for Riak by Basho.
It’s not production-ready, but is a great way to understand log-structured storage engines.


---

✅ This is a **complete Markdown README** you can drop straight into your project as `README.md`.  


