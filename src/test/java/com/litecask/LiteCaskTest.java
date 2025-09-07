package com.litecask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.Test;

public class LiteCaskTest {
    @Test
    public void testPutGet() throws Exception {
        LiteCask store = LiteCask.open("data", true);

        store.put("name", "Alice".getBytes());
        byte[] val = store.get("name");

        assertNotNull(val);
        assertEquals("Alice", new String(val));

        store.close();
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
    public void testDelete() throws Exception {
        LiteCask store = LiteCask.open("data", true);

        store.put("name", "Alice".getBytes());
        store.delete("name");
        assertNull(store.get("name"));

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

}
