package com.miniredis.server;

import com.miniredis.protocol.RespProtocol;
import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;
import com.miniredis.support.TestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerIntegrationTest {

    private RedisServer server;
    private int port;

    @BeforeEach
    void startServer() throws IOException {
        Database db = new Database();
        ServerConfig config = new ServerConfig("127.0.0.1", 0, 64, 64, 10_000, 128);
        server = new RedisServer(db, config);
        server.start();
        port = server.port();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    private static String bulk(Response response) {
        return assertInstanceOf(Response.BulkString.class, response).value();
    }

    private static long number(Response response) {
        return assertInstanceOf(Response.IntegerReply.class, response).value();
    }

    private static String error(Response response) {
        return assertInstanceOf(Response.ErrorReply.class, response).message();
    }

    @Test
    void multipleCommandsOnOneConnection() throws Exception {
        try (TestClient client = new TestClient(port)) {
            assertInstanceOf(Response.SimpleString.class, client.send("SET", "name", "Rahul"));
            assertEquals("Rahul", bulk(client.send("GET", "name")));
            assertEquals(1, number(client.send("INCR", "visits")));
            assertEquals(2, number(client.send("INCR", "visits")));
            assertEquals("2", bulk(client.send("GET", "visits")));
        }
    }

    @Test
    void multipleClientsShareTheSameDatabase() throws Exception {
        try (TestClient first = new TestClient(port);
             TestClient second = new TestClient(port);
             TestClient third = new TestClient(port)) {
            first.send("SET", "shared", "from-first");
            assertEquals("from-first", bulk(second.send("GET", "shared")));
            second.send("SET", "other", "from-second");
            assertEquals("from-second", bulk(third.send("GET", "other")));
        }
    }

    @Test
    void invalidInputReturnsErrorsWithoutKillingTheConnection() throws Exception {
        try (TestClient client = new TestClient(port)) {
            assertTrue(error(client.send("NOT_A_COMMAND")).contains("unknown command"));
            assertTrue(error(client.send("GET")).contains("wrong number"));
            assertInstanceOf(Response.SimpleString.class, client.send("PING"));
            client.send("LPUSH", "list", "x");
            assertTrue(error(client.send("GET", "list")).startsWith("WRONGTYPE"));
            assertEquals(1, number(client.send("LLEN", "list")), "connection still works");
        }
    }

    @Test
    void fragmentedTcpCommandIsReassembled() throws Exception {
        byte[] command = RespProtocol.encodeCommand(List.of("SET", "fragmented", "hello world"));
        int split = command.length / 2;
        try (TestClient client = new TestClient(port)) {
            client.writeRaw(java.util.Arrays.copyOfRange(command, 0, split));
            Thread.sleep(75);
            client.writeRaw(java.util.Arrays.copyOfRange(command, split, command.length));
            assertInstanceOf(Response.SimpleString.class, client.read());
            assertEquals("hello world", bulk(client.send("GET", "fragmented")));
        }
    }

    @Test
    void multiplePipelinedCommandsAreProcessedInOrder() throws Exception {
        byte[] first = RespProtocol.encodeCommand(List.of("SET", "p", "1"));
        byte[] second = RespProtocol.encodeCommand(List.of("INCR", "p"));
        byte[] third = RespProtocol.encodeCommand(List.of("GET", "p"));
        try (TestClient client = new TestClient(port)) {
            byte[] all = new byte[first.length + second.length + third.length];
            System.arraycopy(first, 0, all, 0, first.length);
            System.arraycopy(second, 0, all, first.length, second.length);
            System.arraycopy(third, 0, all, first.length + second.length, third.length);
            client.writeRaw(all);

            assertInstanceOf(Response.SimpleString.class, client.read());
            assertEquals(2, number(client.read()));
            assertEquals("2", bulk(client.read()));
        }
    }

    @Test
    void inlineProtocolSupportsQuotedValues() throws Exception {
        try (TestClient client = new TestClient(port)) {
            client.writeRaw("SET greeting \"hello world\"\r\n");
            assertInstanceOf(Response.SimpleString.class, client.read());
            client.writeRaw("GET greeting\r\n");
            assertEquals("hello world", bulk(client.read()));
        }
    }

    @Test
    void malformedRespReturnsProtocolErrorAndClosesOnlyThatClient() throws Exception {
        try (TestClient broken = new TestClient(port)) {
            broken.writeRaw("*not-a-number\r\n");
            Response response = broken.read();
            assertTrue(error(response).contains("Protocol error"));
        }
        try (TestClient healthy = new TestClient(port)) {
            assertInstanceOf(Response.SimpleString.class, healthy.send("PING"));
        }
    }

    @Test
    void malformedInlineQuotesReturnAProtocolError() throws Exception {
        try (TestClient client = new TestClient(port)) {
            client.writeRaw("SET key \"unclosed\r\n");
            assertTrue(error(client.read()).contains("Protocol error"));
        }
    }

    @Test
    void blankInlineLinesAreIgnored() throws Exception {
        try (TestClient client = new TestClient(port)) {
            client.writeRaw("\r\n");
            client.writeRaw("PING\r\n");
            assertInstanceOf(Response.SimpleString.class, client.read());
        }
    }

    @Test
    void disconnectedClientMidCommandDoesNotAffectServer() throws Exception {
        Socket socket = new Socket("127.0.0.1", port);
        byte[] command = RespProtocol.encodeCommand(List.of("SET", "x", "y"));
        socket.getOutputStream().write(command, 0, 4);
        socket.getOutputStream().flush();
        socket.close();

        Thread.sleep(50);
        try (TestClient client = new TestClient(port)) {
            assertInstanceOf(Response.SimpleString.class, client.send("PING"));
            assertNull(bulk(client.send("GET", "x")));
        }
    }

    @Test
    void quitClosesConnection() throws Exception {
        try (TestClient client = new TestClient(port)) {
            assertInstanceOf(Response.SimpleString.class, client.send("QUIT"));
            assertNull(client.read());
        }
    }

    @Test
    @Timeout(10)
    void shutdownStopsAcceptingAndClosesConnections() throws Exception {
        TestClient openClient = new TestClient(port);
        openClient.send("PING");
        server.shutdown();
        assertFalse(server.isRunning());

        try (Socket socket = new Socket()) {
            assertThrows(IOException.class,
                    () -> socket.connect(new java.net.InetSocketAddress("127.0.0.1", port), 500));
        }
        // Reading from a connection closed by the server must not hang forever.
        assertNull(openClient.read());
        openClient.close();
    }

    @Test
    void utf8KeysAndValuesRoundTrip() throws Exception {
        try (TestClient client = new TestClient(port)) {
            client.send("SET", "greeting-\u00e9", "h\u00e9llo \u4e16\u754c");
            assertEquals("h\u00e9llo \u4e16\u754c", bulk(client.send("GET", "greeting-\u00e9")));
        }
    }
}
