package com.litecask;

import java.io.*;

public class Entry {
    public static final byte FLAG_PUT = 0;
    public static final byte FLAG_TOMBSTONE = 1;


    public final String key;
    public final byte[] value;
    public final byte flag;

    public Entry(String key, byte[] value, byte flag) {
        this.key = key;
        this.value = value;
        this.flag = flag;
    }

    public void writeTo(DataOutputStream out) throws IOException {
        byte[] keyBytes = key.getBytes("UTF-8");
        int keyLen = keyBytes.length;
        int valueLen = (value == null) ? 0 : value.length;

        out.writeInt(keyLen);
        out.writeInt(valueLen);
        out.writeByte(flag);
        out.write(keyBytes);
        if (valueLen > 0) {
            out.write(value);
        }
    }

    public static Entry readFrom(DataInputStream in) throws IOException {
        int keyLen = in.readInt();
        int valueLen = in.readInt();
        byte flag = in.readByte();

        byte[] keyBytes = new byte[keyLen];
        in.readFully(keyBytes);
        String key = new String(keyBytes, "UTF-8");

        byte[] value = null;
        if (valueLen > 0) {
            value = new byte[valueLen];
            in.readFully(value);
        }

        return new Entry(key, value, flag);
    }
}
