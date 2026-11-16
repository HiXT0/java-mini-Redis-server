package com.miniredis.command;

import com.miniredis.command.handlers.CounterCommands;
import com.miniredis.command.handlers.ExpirationCommands;
import com.miniredis.command.handlers.ListCommands;
import com.miniredis.command.handlers.SetCommands;
import com.miniredis.command.handlers.StringCommands;
import com.miniredis.command.handlers.UtilityCommands;
import com.miniredis.persistence.PersistenceException;
import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;
import com.miniredis.storage.InvalidValueException;
import com.miniredis.storage.WrongTypeException;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maps case-insensitive command names to handlers and translates domain
 * exceptions into readable RESP errors. Handlers return responses rather than
 * writing to sockets, which keeps them easy to unit test.
 */
public final class CommandRegistry {

    private static final Logger LOG = Logger.getLogger(CommandRegistry.class.getName());

    @FunctionalInterface
    public interface Handler {
        Response handle(List<String> args);
    }

    private final Database database;
    private final Map<String, Handler> handlers = new HashMap<>();

    public CommandRegistry(Database database) {
        this.database = database;
        StringCommands.register(this, database);
        ExpirationCommands.register(this, database);
        CounterCommands.register(this, database);
        ListCommands.register(this, database);
        SetCommands.register(this, database);
        UtilityCommands.register(this);
    }

    public void register(String name, Handler handler) {
        handlers.put(name.toUpperCase(Locale.ROOT), handler);
    }

    public Response execute(List<String> args) {
        if (args.isEmpty()) {
            return Response.error("ERR empty command");
        }
        String name = args.get(0).toUpperCase(Locale.ROOT);
        Handler handler = handlers.get(name);
        if (handler == null) {
            return Response.error("ERR unknown command '" + args.get(0).toLowerCase(Locale.ROOT) + "'");
        }
        try {
            return handler.handle(args);
        } catch (WrongTypeException ex) {
            return Response.error(ex.getMessage());
        } catch (InvalidValueException ex) {
            return Response.error("ERR " + ex.getMessage());
        } catch (PersistenceException ex) {
            return Response.error("ERR persistence failure: " + ex.getMessage());
        } catch (RuntimeException ex) {
            LOG.log(Level.WARNING, "Unexpected command failure: " + name, ex);
            return Response.error("ERR internal server error");
        }
    }

    public Set<String> commandNames() {
        return Set.copyOf(handlers.keySet());
    }

    public Database database() {
        return database;
    }

    public static Response wrongArity(String command) {
        return Response.error("ERR wrong number of arguments for '" + command + "' command");
    }

    public static Response syntaxError() {
        return Response.error("ERR syntax error");
    }
}
