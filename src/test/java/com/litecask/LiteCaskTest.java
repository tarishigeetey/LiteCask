package com.litecask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
