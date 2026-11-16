package com.miniredis.command.handlers;

import com.miniredis.command.CommandRegistry;
import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;

import java.util.ArrayList;
import java.util.List;

/** SET, GET, DEL, EXISTS, KEYS. */
public final class StringCommands {

    private StringCommands() {
    }

    public static void register(CommandRegistry registry, Database db) {
        registry.register("SET", args -> {
            if (args.size() != 3 && args.size() != 5) {
                return CommandRegistry.wrongArity("set");
            }
            String key = args.get(1);
            String value = args.get(2);
            long expiresAt = 0L;

            if (args.size() == 5) {
                if (!"EX".equalsIgnoreCase(args.get(3))) {
                    return CommandRegistry.syntaxError();
                }
                long seconds;
                try {
                    seconds = Long.parseLong(args.get(4));
                } catch (NumberFormatException ex) {
                    return Response.error("ERR value is not an integer or out of range");
                }
                if (seconds <= 0) {
                    return Response.error("ERR invalid expire time in 'set' command");
                }
                try {
                    expiresAt = Math.addExact(System.currentTimeMillis(),
                            Math.multiplyExact(seconds, 1000L));
                } catch (ArithmeticException ex) {
                    return Response.error("ERR invalid expire time in 'set' command");
                }
            }

            db.set(key, value, expiresAt);
            return Response.ok();
        });

        registry.register("GET", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("get");
            }
            String value = db.get(args.get(1));
            return value == null ? Response.nil() : Response.bulk(value);
        });

        registry.register("DEL", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("del");
            }
            return Response.integer(db.delete(args.get(1)) ? 1 : 0);
        });

        registry.register("EXISTS", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("exists");
            }
            return Response.integer(db.exists(args.get(1)) ? 1 : 0);
        });

        registry.register("KEYS", args -> {
            if (args.size() != 1) {
                return CommandRegistry.wrongArity("keys");
            }
            List<Response> keys = new ArrayList<>();
            for (String key : db.keys()) {
                keys.add(Response.bulk(key));
            }
            return Response.array(keys);
        });
    }
}
