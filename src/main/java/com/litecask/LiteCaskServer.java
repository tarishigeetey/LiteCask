package com.litecask;

import io.javalin.Javalin;

public class LiteCaskServer {
    public static void main(String[] args) throws Exception {
        String dir = (args.length > 0) ? args[0] : "data";
        LiteCask db = LiteCask.open(dir, true);

        Javalin app = Javalin.create(cfg -> {
            cfg.showJavalinBanner = false;
        }).start(7070);

        app.get("/kv/{key}", ctx -> {
            String key = ctx.pathParam("key");
            byte[] v = db.get(key);
            if (v == null) ctx.status(404).result("");
            else ctx.result(new String(v, "UTF-8"));
        });

        app.put("/kv/{key}", ctx -> {
            String key = ctx.pathParam("key");
            byte[] body = ctx.bodyAsBytes();
            db.put(key, body);
            ctx.status(204);
        });

        app.delete("/kv/{key}", ctx -> {
            String key = ctx.pathParam("key");
            db.delete(key);
            ctx.status(204);
        });

        app.post("/merge", ctx -> {
            db.merge();
            ctx.status(204);
        });

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { db.checkpoint(); db.close(); } catch (Exception ignored) {}
        }));
    }
}
