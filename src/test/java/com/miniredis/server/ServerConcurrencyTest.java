package com.miniredis.server;

import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;
import com.miniredis.support.TestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerConcurrencyTest {

    private RedisServer server;
    private int port;

    @BeforeEach
    void startServer() throws IOException {
        Database db = new Database();
        ServerConfig config = new ServerConfig("127.0.0.1", 0, 128, 256, 10_000, 256);
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

    @ParameterizedTest
    @ValueSource(ints = {10, 50, 100})
    @Timeout(60)
    void simultaneousConnectionsAreServed(int clientCount) throws Exception {
        List<TestClient> clients = new ArrayList<>();
        try {
            for (int i = 0; i < clientCount; i++) {
                clients.add(new TestClient(port));
            }

            ExecutorService pool = Executors.newFixedThreadPool(clientCount);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(clientCount);
            for (int i = 0; i < clientCount; i++) {
                final int id = i;
                final TestClient client = clients.get(i);
                pool.submit(() -> {
                    try {
                        start.await();
                        TestClient c = client;
                        assertInstanceOf(Response.SimpleString.class, c.send("PING"));
                        c.send("SET", "client-" + id, "value-" + id);
                        Response reply = c.send("GET", "client-" + id);
                        assertEquals("value-" + id,
                                assertInstanceOf(Response.BulkString.class, reply).value());
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(45, TimeUnit.SECONDS));
            pool.shutdownNow();
        } finally {
            for (TestClient client : clients) {
                client.close();
            }
        }
    }

    @Test
    @Timeout(60)
    void oneHundredClientsIncrementTheSameCounterAtomically() throws Exception {
        int clientCount = 100;
        int incrementsPerClient = 20;
        List<TestClient> clients = new ArrayList<>();
        try {
            for (int i = 0; i < clientCount; i++) {
                clients.add(new TestClient(port));
            }

            ExecutorService pool = Executors.newFixedThreadPool(clientCount);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(clientCount);
            for (TestClient client : clients) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < incrementsPerClient; i++) {
                            Response reply = client.send("INCR", "global-counter");
                            assertInstanceOf(Response.IntegerReply.class, reply);
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(45, TimeUnit.SECONDS));
            pool.shutdownNow();

            try (TestClient verifier = new TestClient(port)) {
                Response reply = verifier.send("GET", "global-counter");
                assertEquals(Integer.toString(clientCount * incrementsPerClient),
                        assertInstanceOf(Response.BulkString.class, reply).value());
            }
        } finally {
            for (TestClient client : clients) {
                client.close();
            }
        }
    }
}
