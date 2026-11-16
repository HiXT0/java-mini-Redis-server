package com.miniredis.command.handlers;

import com.miniredis.command.CommandRegistry;
import com.miniredis.protocol.Response;

/** Small commands useful for health checks and interactive clients. */
public final class UtilityCommands {

    private UtilityCommands() {
    }

    public static void register(CommandRegistry registry) {
        registry.register("PING", args -> {
            if (args.size() == 1) {
                return Response.simple("PONG");
            }
            if (args.size() == 2) {
                return Response.bulk(args.get(1));
            }
            return CommandRegistry.wrongArity("ping");
        });

        registry.register("ECHO", args -> {
            if (args.size() != 2) {
                return CommandRegistry.wrongArity("echo");
            }
            return Response.bulk(args.get(1));
        });

        registry.register("QUIT", args -> {
            if (args.size() != 1) {
                return CommandRegistry.wrongArity("quit");
            }
            return Response.ok();
        });
    }
}
