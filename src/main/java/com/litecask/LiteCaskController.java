package com.litecask;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kv")
public class LiteCaskController {

    private final LiteCask db;

    public LiteCaskController() throws Exception {
        this.db = LiteCask.open("data", true);
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key) throws Exception {
        byte[] val = db.get(key);
        if (val == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new String(val));
    }

    @PutMapping("/{key}")
    public ResponseEntity<Void> put(@PathVariable String key, @RequestBody String body) throws Exception {
        db.put(key, body.getBytes());
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Void> delete(@PathVariable String key) throws Exception {
        db.delete(key);
        return ResponseEntity.noContent().build();
    }
}
