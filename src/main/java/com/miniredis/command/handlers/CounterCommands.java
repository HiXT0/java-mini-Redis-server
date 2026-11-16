package com.miniredis.command.handlers;

import com.miniredis.command.CommandRegistry;
import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;

/** INCR and DECR. */
public final class CounterCommands {

    private CounterCommands() {
    }

    public static void register(CommandRegistry registry, Database db) {
        registry.register("INCR", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("incr");
            }
            return Response.integer(db.increment(args.get(1), 1));
        });

        registry.register("DECR", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("decr");
            }
            return Response.integer(db.increment(args.get(1), -1));
        });
    }
}
