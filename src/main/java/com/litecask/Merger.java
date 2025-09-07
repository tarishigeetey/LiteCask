package com.litecask;

import java.io.*;

public class Merger {
    private final File dataDir;
    private final KeyDir keyDir;

    public Merger(File dataDir, KeyDir keyDir) {
        this.dataDir = dataDir;
        this.keyDir = keyDir;
    }

    public void merge(int activeFileId) throws IOException {
        File[] files = dataDir.listFiles((d, name) -> name.matches("data\\d+\\.dat"));
        if (files == null || files.length <= 1) return;

        // New merged file
        int newFileId = activeFileId + 1;
        File merged = new File(dataDir, "data" + newFileId + ".dat");
        try (RandomAccessFile out = new RandomAccessFile(merged, "rw")) {
            for (File f : files) {
                int fileId = parseFileId(f.getName());
                if (fileId == activeFileId) continue; // skip active file

                try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
                    while (raf.getFilePointer() < raf.length()) {
                        long offset = raf.getFilePointer();

                        // Read entry
                        DataInputStream in = new DataInputStream(
                                new BufferedInputStream(new FileInputStream(raf.getFD())));
                        Entry e = Entry.readFrom(in);

                        // Is this entry still current?
                        KeyDir.EntryMeta meta = keyDir.get(e.key);
                        if (meta != null &&
                            meta.fileId == fileId &&
                            meta.flag == Entry.FLAG_PUT) {

                            // Copy into merged file
                            long newOffset = out.getFilePointer();
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            DataOutputStream dos = new DataOutputStream(baos);
                            e.writeTo(dos);
                            byte[] bytes = baos.toByteArray();
                            out.write(bytes);

                            // Update KeyDir to point to new file/offset
                            keyDir.put(e.key, new KeyDir.EntryMeta(
                                    newFileId,
                                    newOffset + 4 + 4 + 1 + e.key.getBytes("UTF-8").length,
                                    e.value.length,
                                    Entry.FLAG_PUT
                            ));
                        }
                    }
                }
            }
        }

        // Delete old files except active + merged
        for (File f : files) {
            int fileId = parseFileId(f.getName());
            if (fileId != activeFileId && fileId != newFileId) {
                f.delete();
            }
        }
    }

    private int parseFileId(String name) {
        String num = name.substring(4, name.length() - 4);
        return Integer.parseInt(num);
    }
}
