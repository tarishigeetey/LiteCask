package com.litecask;

import java.util.concurrent.ConcurrentHashMap;

/**
 * KeyDir is an in-memory index mapping keys to their latest location on disk.
 * Each key points to EntryMeta: fileId, value offset, size, and flag.
 */
public class KeyDir {

    private final ConcurrentHashMap<String, EntryMeta> map = new ConcurrentHashMap<>();

    /** Lookup metadata for a key */
    public EntryMeta get(String key) {
        return map.get(key);
    }

    /** Insert or update metadata for a key */
    public void put(String key, EntryMeta meta) {
        map.put(key, meta);
    }

    /** Remove key completely from KeyDir (not from disk) */
    public void remove(String key) {
        map.remove(key);
    }

    /** Iterate over all entries (useful for checkpointing) */
    public Iterable<java.util.Map.Entry<String, EntryMeta>> entrySet() {
        return map.entrySet();
    }

    /** Metadata stored for each key */
    public static class EntryMeta {
        public final int fileId;       // which dataN.dat file
        public final long valueOffset; // offset inside file where value starts
        public final int valueSize;    // size of value in bytes
        public final byte flag;        // FLAG_PUT or FLAG_TOMBSTONE

        public EntryMeta(int fileId, long valueOffset, int valueSize, byte flag) {
            this.fileId = fileId;
            this.valueOffset = valueOffset;
            this.valueSize = valueSize;
            this.flag = flag;
        }
    }
}
