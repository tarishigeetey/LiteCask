package com.litecask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.*;
import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LiteCaskTest {
	 private static final String DIR = "testdata";

	    @BeforeEach
	    public void cleanDir() throws IOException {
	        Path path = Path.of(DIR);
	        if (Files.exists(path)) {
	            Files.walk(path)
	                .map(Path::toFile)
	                .forEach(f -> f.delete());
	        }
	        Files.createDirectories(path);
	    }

   
    @Test
    public void testOverwrite() throws Exception {
        LiteCask store = LiteCask.open("data", true);

        store.put("name", "Alice".getBytes());
        store.put("name", "Bob".getBytes());
        assertEquals("Bob", new String(store.get("name")));

        store.close();
    }

    @Test
    public void testPutGet() throws Exception {
        LiteCask store = LiteCask.open(DIR, true);
        store.put("k1", "hello".getBytes());
        assertEquals("hello", new String(store.get("k1")));
        store.close();
    }

    @Test
    public void testDelete() throws Exception {
        LiteCask store = LiteCask.open(DIR, true);
        store.put("k1", "hello".getBytes());
        store.delete("k1");
        assertNull(store.get("k1"));
        store.close();
    }
    
    @Test
    public void testFileRotation() throws Exception {
        LiteCask store = LiteCask.open("data", true);

        // shrink limit for quick test
        long MAX_FILE_SIZE = 50;

        for (int i = 0; i < 10; i++) {
            String key = "k" + i;
            String val = "v" + i;
            store.put(key, val.getBytes());
        }

        store.close();

        // Expect multiple files in "data" directory
        File dir = new File("data");
        File[] files = dir.listFiles((d, name) -> name.endsWith(".dat"));
        assertTrue(files.length > 1);
    }
    @Test
    public void testRecovery() throws Exception {
        LiteCask s1 = LiteCask.open("data", true);
        s1.put("a", "1".getBytes());
        s1.put("b", "2".getBytes());
        s1.close();

        LiteCask s2 = LiteCask.open("data", true);
        assertEquals("1", new String(s2.get("a")));
        assertEquals("2", new String(s2.get("b")));
        s2.close();
    }
    
    @Test
    public void testMerge() throws Exception {
        LiteCask store = LiteCask.open("data", true);

        store.put("k1", "a".getBytes());
        store.put("k1", "b".getBytes()); // old "a" should be discarded
        store.put("k2", "x".getBytes());
        store.delete("k2");              // tombstone should be discarded

        store.merge();

        assertEquals("b", new String(store.get("k1")));
        assertNull(store.get("k2"));

        store.close();
    }
    
    @Test
    public void testSingleWriterLock() throws Exception {
        LiteCask s1 = LiteCask.open("data", true);

        // Expect IOException when opening a second writer
        assertThrows(IOException.class, () -> {
            LiteCask s2 = LiteCask.open("data", true);
        });

        s1.close();
    }
    
    @Test
    public void testStressPutGet() throws Exception {
        LiteCask store = LiteCask.open(DIR, true);
        int N = 10_000;
        for (int i = 0; i < N; i++) {
            store.put("k" + i, ("val" + i).getBytes());
        }
        for (int i = 0; i < N; i++) {
            assertEquals("val" + i, new String(store.get("k" + i)));
        }
        store.close();
    }
    
    @Test
    public void benchmarkPutGet() throws Exception {
        LiteCask store = LiteCask.open(DIR, true);
        int N = 50_000;

        long startPut = System.nanoTime();
        for (int i = 0; i < N; i++) {
            store.put("k" + i, ("val" + i).getBytes());
        }
        long putTimeMs = (System.nanoTime() - startPut) / 1_000_000;
        System.out.println("PUT " + N + " entries in " + putTimeMs + " ms");

        long startGet = System.nanoTime();
        for (int i = 0; i < N; i++) {
            store.get("k" + i);
        }
        long getTimeMs = (System.nanoTime() - startGet) / 1_000_000;
        System.out.println("GET " + N + " entries in " + getTimeMs + " ms");

        store.close();
    }

}
