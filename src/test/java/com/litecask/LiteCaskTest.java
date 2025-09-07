package com.litecask;

import org.junit.jupiter.api.*;
import java.io.File;
import java.nio.file.Files;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class LiteCaskTest {
    private File tempDir;

    @BeforeEach
    public void setup() throws Exception {
        tempDir = Files.createTempDirectory("litecask").toFile();
    }

    @AfterEach
    public void cleanup() {
        deleteRecursively(tempDir);
    }

    private void deleteRecursively(File f) {
        if (f == null || !f.exists()) return;
        if (f.isDirectory()) {
            for (File c : f.listFiles()) deleteRecursively(c);
        }
        f.delete();
    }

    @Test
    public void testPutAndGet() throws Exception {
        LiteCask db = LiteCask.open(tempDir.getAbsolutePath(), true);
        db.put("foo", "bar".getBytes());
        byte[] val = db.get("foo");
        assertEquals("bar", new String(val));
        db.close();
    }

    @Test
    public void testOverwriteAndDelete() throws Exception {
        LiteCask db = LiteCask.open(tempDir.getAbsolutePath(), true);
        db.put("x", "1".getBytes());
        db.put("x", "2".getBytes());
        assertEquals("2", new String(db.get("x")));

        db.delete("x");
        assertNull(db.get("x"));
        db.close();
    }

    @Test
    public void testRotation() throws Exception {
        LiteCask db = LiteCask.open(tempDir.getAbsolutePath(), true);
        String big = "a".repeat(1024 * 1024); // 1MB
        for (int i = 0; i < 70; i++) {
            db.put("k" + i, big.getBytes()); // should trigger rotation
        }
        assertNotNull(db.get("k0"));
        db.close();
    }

    @Test
    public void testMerge() throws Exception {
        LiteCask db = LiteCask.open(tempDir.getAbsolutePath(), true);
        db.put("a", "1".getBytes());
        db.put("a", "2".getBytes());
        db.put("b", "x".getBytes());
        db.merge();
        assertEquals("2", new String(db.get("a")));
        assertEquals("x", new String(db.get("b")));
        db.close();
    }

    @Test
    public void testHintFileRecovery() throws Exception {
        LiteCask db1 = LiteCask.open(tempDir.getAbsolutePath(), true);
        db1.put("hello", "world".getBytes());
        db1.close();

        LiteCask db2 = LiteCask.open(tempDir.getAbsolutePath(), true);
        assertEquals("world", new String(db2.get("hello")));
        db2.close();
    }

    @Test
    public void testCheckpointRecovery() throws Exception {
        LiteCask db1 = LiteCask.open(tempDir.getAbsolutePath(), true);
        db1.put("chk", "val".getBytes());
        db1.checkpoint();
        db1.close();

        LiteCask db2 = LiteCask.open(tempDir.getAbsolutePath(), true);
        assertEquals("val", new String(db2.get("chk")));
        db2.close();
    }

    @Test
    public void testSingleWriterLock() throws Exception {
        LiteCask s1 = LiteCask.open(tempDir.getAbsolutePath(), true);
        assertThrows(Exception.class, () -> {
            LiteCask.open(tempDir.getAbsolutePath(), true);
        });
        s1.close();
    }

    @Test
    public void testListKeys() throws Exception {
        LiteCask db = LiteCask.open(tempDir.getAbsolutePath(), true);
        db.put("k1", "v1".getBytes());
        db.put("k2", "v2".getBytes());
        Set<String> keys = db.keys();
        assertTrue(keys.contains("k1"));
        assertTrue(keys.contains("k2"));
        db.close();
    }
}
