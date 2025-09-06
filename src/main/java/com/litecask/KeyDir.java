package com.litecask;

import java.util.concurrent.ConcurrentHashMap;

public class KeyDir {
    public static class EntryMeta {
        public final int fileId;
        public final long valueOffset;
        public final int valueSize;
        public final byte flag;

        public EntryMeta(int fileId, long valueOffset, int valueSize, byte flag) {
            this.fileId = fileId;
            this.valueOffset = valueOffset;
            this.valueSize = valueSize;
            this.flag = flag;
        }
    }

    private final ConcurrentHashMap<String, EntryMeta> map = new ConcurrentHashMap<>();

    public void put(String key, EntryMeta meta) {
        map.put(key, meta);
    }

    public EntryMeta get(String key) {
        return map.get(key);
    }
}
