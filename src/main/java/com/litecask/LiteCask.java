package com.litecask;

import java.io.*;

public class LiteCask {
    private final File dataDir;
    private final KeyDir keyDir = new KeyDir();
    private RandomAccessFile activeFile;
    private int activeFileId = 1;

    private LiteCask(String dirPath) throws IOException {
        this.dataDir = new File(dirPath);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        openActiveFile();
    }

    public static LiteCask open(String dir, boolean writable) throws IOException {
        return new LiteCask(dir);
    }

    private void openActiveFile() throws IOException {
        File file = new File(dataDir, "data" + activeFileId + ".dat");
        this.activeFile = new RandomAccessFile(file, "rw");
        this.activeFile.seek(activeFile.length()); // append mode
    }

    public void put(String key, byte[] value) throws IOException {
        long offset = activeFile.getFilePointer();

        // Serialize entry
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Entry entry = new Entry(key, value, Entry.FLAG_PUT);
        entry.writeTo(dos);

        byte[] bytes = baos.toByteArray();
        activeFile.write(bytes);
        activeFile.getFD().sync();

        // Update keydir (value starts after header + key)
        keyDir.put(key, new KeyDir.EntryMeta(
                activeFileId,
                offset + 4 + 4 + 1 + key.getBytes("UTF-8").length,
                value.length,
                Entry.FLAG_PUT
        ));
    }

    public byte[] get(String key) throws IOException {
        KeyDir.EntryMeta meta = keyDir.get(key);
        if (meta == null || meta.flag == Entry.FLAG_TOMBSTONE) {
            return null;
        }

        File file = new File(dataDir, "data" + meta.fileId + ".dat");
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(meta.valueOffset);
            byte[] value = new byte[meta.valueSize];
            raf.readFully(value);
            return value;
        }
    }

    public void close() throws IOException {
        if (activeFile != null) {
            activeFile.close();
        }
    }
}
