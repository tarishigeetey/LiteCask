package com.litecask;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

public class LiteCaskCli {
    public static void main(String[] args) throws Exception {
        String dir = (args.length > 0) ? args[0] : "data";
        LiteCask db = LiteCask.open(dir, true);
        System.out.println("LiteCask CLI. Dir=" + dir + "  Commands: put k v | get k | del k | list | merge | exit");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        for (;;) {
            System.out.print("> ");
            String line = br.readLine();
            if (line == null) break;
            line = line.trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split("\\s+");
            String cmd = parts[0].toLowerCase();

            try {
                switch (cmd) {
                    case "put":
                        if (parts.length < 3) { System.out.println("Usage: put <key> <value>"); break; }
                        String key = parts[1];
                        // allow spaces in value
                        String value = String.join(" ", Arrays.copyOfRange(parts, 2, parts.length));
                        db.put(key, value.getBytes("UTF-8"));
                        System.out.println("OK");
                        break;
                    case "get":
                        if (parts.length != 2) { System.out.println("Usage: get <key>"); break; }
                        byte[] v = db.get(parts[1]);
                        System.out.println(v == null ? "(nil)" : new String(v, "UTF-8"));
                        break;
                    case "del":
                    case "delete":
                        if (parts.length != 2) { System.out.println("Usage: del <key>"); break; }
                        db.delete(parts[1]);
                        System.out.println("OK");
                        break;
                    case "list":
                        for (String k : db.keys()) System.out.println(k);
                        break;
                    case "merge":
                        db.merge();
                        System.out.println("Merged");
                        break;
                    case "exit":
                    case "quit":
                        db.close();
                        System.out.println("Bye");
                        return;
                    default:
                        System.out.println("Unknown command");
                }
            } catch (Exception ex) {
                System.out.println("ERR: " + ex.getMessage());
            }
        }
    }
}
