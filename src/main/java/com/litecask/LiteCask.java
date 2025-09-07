package com.litecask;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LiteCask {
	private static final long MAX_FILE_SIZE = 64 * 1024 * 1024; // 64 MB

    private final File dataDir;
    private final KeyDir keyDir = new KeyDir();
    private RandomAccessFile activeFile;
    private int activeFileId = 1;
    
    private FileChannel lockChannel;
    private FileLock lock;

    private LiteCask(String dirPath, boolean writable) throws IOException {
    	 this.dataDir = new File(dirPath);
    	    if (!dataDir.exists()) dataDir.mkdirs();
    	    
    	 // Create/open lock file
    	    File lockFile = new File(dataDir, "LOCK");
    	    this.lockChannel = new RandomAccessFile(lockFile, "rw").getChannel();

    	    if (writable) {
    	        this.lock = lockChannel.tryLock();
    	        if (lock == null) {
    	            throw new IOException("LiteCask already opened in write mode by another process!");
    	        }
    	    }
    	    // Read existing files (if any), rebuild KeyDir and decide activeFileId
    	    int lastCheckpointed = loadCheckpoint(); 
    	    
    	    // Read existing files (if any), rebuild KeyDir and decide activeFileId
    	    rebuildFromDisk(lastCheckpointed);

    	    // Open (or create) the active file and seek to end
    	    openActiveFile();
    }

    public static LiteCask open(String dir, boolean writable) throws IOException {
        // (writable flag can be used later for read-only mode)
        return new LiteCask(dir, writable);
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
        
        writeHint(
                key,
                activeFileId,
                offset + 4 + 4 + 1 + key.getBytes("UTF-8").length,
                value.length,
                Entry.FLAG_PUT
            );
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
        
        writeHint(key, activeFileId, 0, 0, Entry.FLAG_TOMBSTONE);

    }

    public void close() throws IOException {
    	if (activeFile != null) {
            activeFile.close();
        }
    	// Save in-memory KeyDir snapshot
        checkpoint();
        if (lock != null) {
            lock.release();
        }
        if (lockChannel != null) {
            lockChannel.close();
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
    private void rebuildFromDisk(int minFileId) throws IOException {
        File[] files = listDataFilesSorted();
        if (files.length == 0) {
            this.activeFileId = 1;
            return;
        }

        // Gather files to process (newer than checkpoint)
        List<File> toScan = new ArrayList<>();
        for (File f : files) {
            int fileId = parseFileId(f.getName());
            if (fileId > minFileId) toScan.add(f);
        }
        if (toScan.isEmpty()) {
            this.activeFileId = parseFileId(files[files.length - 1].getName());
            return;
        }

        int threads = Math.min(Runtime.getRuntime().availableProcessors(), Math.max(1, toScan.size()));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Map<String, KeyDir.EntryMeta>>> futures = new ArrayList<>();

        for (File f : toScan) {
            futures.add(pool.submit(() -> scanFileFast(f)));
        }

        // Merge results with latest-wins
        for (Future<Map<String, KeyDir.EntryMeta>> fut : futures) {
            try {
                Map<String, KeyDir.EntryMeta> local = fut.get();
                for (var e : local.entrySet()) {
                    keyDir.put(e.getKey(), mergeLatest(keyDir.get(e.getKey()), e.getValue()));
                }
            } catch (ExecutionException | InterruptedException ex) {
                throw new IOException("Parallel rebuild failed", ex);
            }
        }

        pool.shutdown();

        this.activeFileId = parseFileId(files[files.length - 1].getName());
    }

    private KeyDir.EntryMeta mergeLatest(KeyDir.EntryMeta oldMeta, KeyDir.EntryMeta newMeta) {
        if (oldMeta == null) return newMeta;
        long oldV = versionOf(oldMeta);
        long newV = versionOf(newMeta);
        return (newV >= oldV) ? newMeta : oldMeta;
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
    
    private void writeHint(String key, int fileId, long valueOffset, int valueSize, byte flag) throws IOException {
        File hintFile = new File(dataDir, "data" + fileId + ".hint");
        try (RandomAccessFile raf = new RandomAccessFile(hintFile, "rw")) {
            raf.seek(raf.length()); // append
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(raf.getFD()));
            byte[] keyBytes = key.getBytes("UTF-8");
            dos.writeInt(keyBytes.length);
            dos.writeInt(valueSize);
            dos.writeInt(fileId);
            dos.writeLong(valueOffset);
            dos.writeByte(flag);
            dos.write(keyBytes);
            dos.flush();
        }
    }
    
    private void loadFromHint(File hint, int fileId) throws IOException {
        try (DataInputStream in = new DataInputStream(new FileInputStream(hint))) {
            while (in.available() > 0) {
                int keyLen = in.readInt();
                int valueSize = in.readInt();
                int fId = in.readInt();
                long valueOffset = in.readLong();
                byte flag = in.readByte();
                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);
                String key = new String(keyBytes, "UTF-8");

                keyDir.put(key, new KeyDir.EntryMeta(fId, valueOffset, valueSize, flag));
            }
        }
    }
    
    public void checkpoint() throws IOException {
        File chk = new File(dataDir, "keydir.chk");
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(chk))) {
            // Write header: last fileId we’ve fully processed
            out.writeInt(activeFileId);

            for (var e : keyDir.entrySet()) {
                byte[] keyBytes = e.getKey().getBytes("UTF-8");
                KeyDir.EntryMeta meta = e.getValue();
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeInt(meta.fileId);
                out.writeLong(meta.valueOffset);
                out.writeInt(meta.valueSize);
                out.writeByte(meta.flag);
            }
        }
    }

    
    private int loadCheckpoint() throws IOException {
        File chk = new File(dataDir, "keydir.chk");
        if (!chk.exists()) return 0;

        int lastCheckpointedFileId = 0;
        try (DataInputStream in = new DataInputStream(new FileInputStream(chk))) {
            in.mark(4);
            try {
                // Try to read header (new format)
                lastCheckpointedFileId = in.readInt();
            } catch (EOFException e) {
                // Old format → reset stream
                in.reset();
            }

            while (in.available() > 0) {
                int keyLen = in.readInt();
                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);
                String key = new String(keyBytes, "UTF-8");

                int fileId = in.readInt();
                long valueOffset = in.readLong();
                int valueSize = in.readInt();
                byte flag = in.readByte();

                keyDir.put(key, new KeyDir.EntryMeta(fileId, valueOffset, valueSize, flag));
            }
        }
        return lastCheckpointedFileId;
    }


    
    private long versionOf(KeyDir.EntryMeta m) {
        long off = (m.entryStart >= 0 ? m.entryStart : m.valueOffset);
        return ((long)m.fileId << 32) | (off & 0xffffffffL);
    }
    
    private Map<String, KeyDir.EntryMeta> scanFileFast(File dataFile) throws IOException {
        int fileId = parseFileId(dataFile.getName());
        File hint = new File(dataDir, "data" + fileId + ".hint");

        Map<String, KeyDir.EntryMeta> local = new HashMap<>();
        if (hint.exists()) {
            try (DataInputStream in = new DataInputStream(new FileInputStream(hint))) {
                while (in.available() > 0) {
                    int keyLen = in.readInt();
                    int valueSize = in.readInt();
                    int fId = in.readInt();
                    long valueOffset = in.readLong();
                    byte flag = in.readByte();
                    byte[] keyBytes = new byte[keyLen];
                    in.readFully(keyBytes);
                    String key = new String(keyBytes, "UTF-8");

                    // entryStart unknown in hints → use valueOffset as tie-breaker
                    KeyDir.EntryMeta m = new KeyDir.EntryMeta(fId, valueOffset, valueSize, flag, valueOffset);
                    local.merge(key, m, (a,b)-> mergeLatest(a,b));
                }
            }
            return local;
        }

        // Fallback: scan .dat
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
            long len = raf.length();
            while (raf.getFilePointer() < len) {
                long start = raf.getFilePointer();
                if (len - start < 4 + 4 + 1) break;

                int keyLen = raf.readInt();
                int valLen = raf.readInt();
                byte flag = raf.readByte();
                if (keyLen < 0 || valLen < 0) break;

                long afterHeader = raf.getFilePointer();
                long need = (long) keyLen + (long) valLen;
                if (afterHeader + need > len) break;

                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);
                String key = new String(keyBytes, "UTF-8");

                long valuePos = raf.getFilePointer();
                raf.seek(valuePos + valLen);

                KeyDir.EntryMeta m;
                if (flag == Entry.FLAG_TOMBSTONE) {
                    m = new KeyDir.EntryMeta(fileId, start, 0, flag, start);
                } else {
                    m = new KeyDir.EntryMeta(fileId, valuePos, valLen, flag, start);
                }
                local.merge(key, m, (a,b)-> mergeLatest(a,b));
            }
        }
        return local;
    }
    
    public java.util.Set<String> keys() {
        java.util.HashSet<String> s = new java.util.HashSet<>();
        for (var e : keyDir.entrySet()) s.add(e.getKey());
        return s;
    }
}
