package com.miniredis.client;

import com.miniredis.protocol.InlineCommandParser;
import com.miniredis.protocol.RespProtocol;
import com.miniredis.protocol.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * Interactive command-line client. Values containing spaces must be quoted:
 * {@code SET greeting "hello world"}.
 */
public final class MiniRedisClient {

    private static final String USAGE = """
            Usage: java -cp target/mini-redis-1.0.0.jar com.miniredis.client.MiniRedisClient [options]

            Options:
              --host HOST       Server host (default: localhost)
              --port PORT       Server port (default: 6379)
              --timeout MILLIS  Connect timeout in milliseconds (default: 5000)
              --command COMMAND Run one command and exit
              --help            Show this help
            """;

    private MiniRedisClient() {
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception ex) {
            System.err.println("Client error: " + ex.getMessage());
            System.exit(1);
        }
    }

    private static void run(String[] args) throws IOException {
        String host = "localhost";
        int port = 6379;
        int timeout = 5000;
        String oneShot = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = requireValue(args, ++i, "--host");
                case "--port" -> port = parsePort(requireValue(args, ++i, "--port"));
                case "--timeout" -> timeout = parsePositive(requireValue(args, ++i, "--timeout"), "--timeout");
                case "--command" -> oneShot = requireValue(args, ++i, "--command");
                case "--help", "-h" -> {
                    System.out.print(USAGE);
                    return;
                }
                default -> throw new IllegalArgumentException("unknown option: " + args[i]);
            }
        }

        RedisConnection connection;
        try {
            connection = RedisConnection.connect(host, port, timeout);
        } catch (IOException ex) {
            throw new IOException("could not connect to " + host + ":" + port + " (" + ex.getMessage() + ")");
        }

        if (oneShot != null) {
            try (connection) {
                List<String> command = parseLine(oneShot);
                executeAndPrint(connection, command);
            }
            return;
        }

        System.out.println("Connected to Mini Redis at " + host + ":" + port);
        System.out.println("Type commands, or 'exit' to quit. Quote values containing spaces.");
        try (connection;
             BufferedReader stdin = new BufferedReader(
                     new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            while (true) {
                System.out.print(host + ":" + port + "> ");
                System.out.flush();
                String line = stdin.readLine();
                if (line == null) {
                    System.out.println();
                    break;
                }
                List<String> command;
                try {
                    command = parseLine(line);
                } catch (IllegalArgumentException ex) {
                    System.out.println("(local error) " + ex.getMessage());
                    continue;
                }
                if (command.isEmpty()) {
                    continue;
                }
                String name = command.get(0).toLowerCase(Locale.ROOT);
                if ("exit".equals(name) && command.size() == 1) {
                    break;
                }
                if (!executeAndPrint(connection, command)) {
                    System.err.println("Server closed the connection.");
                    break;
                }
                if ("quit".equals(name) && command.size() == 1) {
                    break;
                }
            }
        } catch (IOException ex) {
            System.err.println("Connection lost: " + ex.getMessage());
        }
    }

    /** Returns false when the server closed the connection. */
    private static boolean executeAndPrint(RedisConnection connection, List<String> command)
            throws IOException {
        Response response = connection.send(command);
        if (response == null) {
            return false;
        }
        System.out.println(RespProtocol.format(response));
        return true;
    }

    private static List<String> parseLine(String line) {
        if (line.strip().equalsIgnoreCase("exit")) {
            return List.of("exit");
        }
        return InlineCommandParser.parse(line);
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException(option + " requires a value");
        }
        return args[index];
    }

    private static int parsePort(String value) {
        int port = parsePositive(value, "--port");
        if (port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        return port;
    }

    private static int parsePositive(String value, String option) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(option + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(option + " must be an integer");
        }
    }
}
