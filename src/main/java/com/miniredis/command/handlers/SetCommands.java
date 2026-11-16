package com.miniredis.command.handlers;

import com.miniredis.command.CommandRegistry;
import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;

/** SADD, SISMEMBER, SCARD. */
public final class SetCommands {

    private SetCommands() {
    }

    public static void register(CommandRegistry registry, Database db) {
        registry.register("SADD", args -> {
            if (args.size() != 3) {
                return CommandRegistry.wrongArity("sadd");
            }
            return Response.integer(db.addToSet(args.get(1), args.get(2)));
        });

        registry.register("SISMEMBER", args -> {
            if (args.size() != 3) {
                return CommandRegistry.wrongArity("sismember");
            }
            return Response.integer(db.isSetMember(args.get(1), args.get(2)) ? 1 : 0);
        });

        registry.register("SCARD", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("scard");
            }
            return Response.integer(db.setSize(args.get(1)));
        });
    }
}
