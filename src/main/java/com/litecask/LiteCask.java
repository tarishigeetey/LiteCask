package com.litecask;

import java.io.*;

public class LiteCask {
	private static final long MAX_FILE_SIZE = 64 * 1024 * 1024; // 64 MB

    private final File dataDir;
    private final KeyDir keyDir = new KeyDir();
    private RandomAccessFile activeFile;
    private int activeFileId = 1;

    private LiteCask(String dirPath) throws IOException {
    	 this.dataDir = new File(dirPath);
    	    if (!dataDir.exists()) dataDir.mkdirs();

    	    // Read existing files (if any), rebuild KeyDir and decide activeFileId
    	    rebuildFromDisk();

    	    // Open (or create) the active file and seek to end
    	    openActiveFile();
    }

    public static LiteCask open(String dir, boolean writable) throws IOException {
        // (writable flag can be used later for read-only mode)
        return new LiteCask(dir);
    }

    private void openActiveFile() throws IOException {
        File file = new File(dataDir, "data" + activeFileId + ".dat");
        this.activeFile = new RandomAccessFile(file, "rw");
        this.activeFile.seek(activeFile.length()); // append mode
    }

    public void put(String key, byte[] value) throws IOException {
        checkRotation();  // 🔹 ensure we rotate if file too big

        long offset = activeFile.getFilePointer();

        // Serialize entry
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Entry entry = new Entry(key, value, Entry.FLAG_PUT);
        entry.writeTo(dos);

        byte[] bytes = baos.toByteArray();
        activeFile.write(bytes);
        activeFile.getFD().sync();

        // Update KeyDir
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

    public void delete(String key) throws IOException {
        checkRotation();  // 🔹 ensure we rotate if file too big

        long offset = activeFile.getFilePointer();

        // Create tombstone entry
        Entry entry = new Entry(key, null, Entry.FLAG_TOMBSTONE);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        entry.writeTo(dos);

        byte[] bytes = baos.toByteArray();
        activeFile.write(bytes);
        activeFile.getFD().sync();

        // Update KeyDir with tombstone
        keyDir.put(key, new KeyDir.EntryMeta(
                activeFileId,
                offset,
                0,
                Entry.FLAG_TOMBSTONE
        ));
    }


    
    public void close() throws IOException {
        if (activeFile != null) {
            activeFile.close();
        }
    }
    
    /** List existing dataN.dat files, sorted by fileId ascending. */
    private File[] listDataFilesSorted() {
        File[] files = dataDir.listFiles((d, name) -> name.matches("data\\d+\\.dat"));
        if (files == null) return new File[0];
        java.util.Arrays.sort(files, (a, b) -> Integer.compare(parseFileId(a.getName()), parseFileId(b.getName())));
        return files;
    }

    /** Extract the numeric id from "data123.dat" -> 123 */
    private int parseFileId(String name) {
        // name guaranteed to match data\\d+\\.dat
        String num = name.substring(4, name.length() - 4); // between "data" and ".dat"
        return Integer.parseInt(num);
    }

    /** Scan all data files and rebuild KeyDir; set activeFileId accordingly. */
    private void rebuildFromDisk() throws IOException {
        File[] files = listDataFilesSorted();
        if (files.length == 0) {
            this.activeFileId = 1; // start fresh
            return;
        }

        // Scan each file sequentially
        for (File f : files) {
            int fileId = parseFileId(f.getName());
            try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
                long len = raf.length();
                while (raf.getFilePointer() < len) {
                    long start = raf.getFilePointer();

                    // Defensive: ensure we have at least header bytes ahead
                    if (len - start < 4 + 4 + 1) break; // partial tail

                    int keyLen = raf.readInt();
                    int valLen = raf.readInt();
                    byte flag = raf.readByte();

                    // Validate lengths (basic sanity to avoid nonsense)
                    if (keyLen < 0 || valLen < 0) break; // corrupt tail

                    // Ensure full record is present
                    long afterHeader = raf.getFilePointer();
                    long need = (long) keyLen + (long) valLen;
                    if (afterHeader + need > len) {
                        // not enough bytes (likely crash mid-write) -> stop scanning this file
                        break;
                    }

                    // Read key bytes
                    byte[] keyBytes = new byte[keyLen];
                    raf.readFully(keyBytes);
                    String key = new String(keyBytes, "UTF-8");

                    // Value position is right here (immediately after key)
                    long valuePos = raf.getFilePointer();

                    // Skip the value bytes (don’t need to read them now)
                    raf.seek(valuePos + valLen);

                    // Update KeyDir (latest write wins as we scan forward)
                    if (flag == Entry.FLAG_TOMBSTONE) {
                        keyDir.put(key, new KeyDir.EntryMeta(fileId, start, 0, flag));
                    } else {
                        keyDir.put(key, new KeyDir.EntryMeta(fileId, valuePos, valLen, flag));
                    }
                }
            }
        }

        // Set active file to the highest existing id + open next for appends
        this.activeFileId = parseFileId(files[files.length - 1].getName());
    }

    
    private void checkRotation() throws IOException {
        if (activeFile.length() >= MAX_FILE_SIZE) {
            activeFile.close();
            activeFileId++;   // move to next file
            openActiveFile(); // start new active file
        }
    }
    
    public void merge() throws IOException {
        Merger merger = new Merger(dataDir, keyDir);
        merger.merge(activeFileId);
    }
}
