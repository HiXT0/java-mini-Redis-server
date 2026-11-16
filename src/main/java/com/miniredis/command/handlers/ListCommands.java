package com.miniredis.command.handlers;

import com.miniredis.command.CommandRegistry;
import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;

/** LPUSH, RPOP, LLEN. */
public final class ListCommands {

    private ListCommands() {
    }

    public static void register(CommandRegistry registry, Database db) {
        registry.register("LPUSH", args -> {
            if (args.size() != 3) {
                return CommandRegistry.wrongArity("lpush");
            }
            return Response.integer(db.pushLeft(args.get(1), args.get(2)));
        });

        registry.register("RPOP", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("rpop");
            }
            String value = db.popRight(args.get(1));
            return value == null ? Response.nil() : Response.bulk(value);
        });

        registry.register("LLEN", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("llen");
            }
            return Response.integer(db.listLength(args.get(1)));
        });
    }
}
