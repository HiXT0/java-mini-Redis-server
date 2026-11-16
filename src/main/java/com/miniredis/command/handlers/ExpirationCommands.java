package com.miniredis.command.handlers;

import com.miniredis.command.CommandRegistry;
import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;

/** TTL and PERSIST. */
public final class ExpirationCommands {

    private ExpirationCommands() {
    }

    public static void register(CommandRegistry registry, Database db) {
        registry.register("TTL", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("ttl");
            }
            return Response.integer(db.ttl(args.get(1)));
        });

        registry.register("PERSIST", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("persist");
            }
            return Response.integer(db.persist(args.get(1)) ? 1 : 0);
        });
    }
}
