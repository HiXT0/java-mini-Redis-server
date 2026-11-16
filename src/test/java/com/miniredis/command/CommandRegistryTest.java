package com.miniredis.command;

import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandRegistryTest {

    private Database db;
    private CommandRegistry registry;

    @BeforeEach
    void setUp() {
        db = new Database();
        registry = new CommandRegistry(db);
    }

    private Response exec(String... args) {
        return registry.execute(List.of(args));
    }

    private String bulk(Response response) {
        Response.BulkString bulk = assertInstanceOf(Response.BulkString.class, response);
        return bulk.value();
    }

    private long number(Response response) {
        return assertInstanceOf(Response.IntegerReply.class, response).value();
    }

    @Test
    void setGetAndNil() {
        assertInstanceOf(Response.SimpleString.class, exec("SET", "name", "Rahul"));
        assertEquals("Rahul", bulk(exec("GET", "name")));
        assertInstanceOf(Response.BulkString.class, exec("GET", "missing"));
        assertEquals(null, bulk(exec("GET", "missing")));
    }

    @Test
    void commandsAreCaseInsensitiveButKeysAndValuesAreNot() {
        exec("sEt", "Key", "Value");
        assertEquals("Value", bulk(exec("gEt", "Key")));
        assertEquals(null, bulk(exec("GET", "key")));
        assertEquals(0, number(exec("EXISTS", "key")));
        assertEquals(1, number(exec("EXISTS", "Key")));
    }

    @Test
    void valuesMayContainSpacesAndEmptyStrings() {
        exec("SET", "greeting", "hello world");
        assertEquals("hello world", bulk(exec("GET", "greeting")));
        exec("SET", "empty", "");
        assertEquals("", bulk(exec("GET", "empty")));
    }

    @Test
    void delExistsAndKeys() {
        exec("SET", "a", "1");
        exec("SET", "b", "2");
        assertEquals(1, number(exec("DEL", "a")));
        assertEquals(0, number(exec("DEL", "a")));
        assertEquals(0, number(exec("EXISTS", "a")));
        assertEquals(1, number(exec("EXISTS", "b")));

        Response.ArrayReply array = assertInstanceOf(Response.ArrayReply.class, exec("KEYS"));
        assertEquals(List.of("b"), array.values().stream().map(r -> ((Response.BulkString) r).value()).toList());
    }

    @Test
    void expirationCommands() {
        exec("SET", "token", "abc", "EX", "60");
        long ttl = number(exec("TTL", "token"));
        assertTrue(ttl >= 59 && ttl <= 60, "ttl=" + ttl);
        assertEquals(1, number(exec("PERSIST", "token")));
        assertEquals(-1, number(exec("TTL", "token")));
        assertEquals(0, number(exec("PERSIST", "token")));
        assertEquals(-2, number(exec("TTL", "missing")));
    }

    @Test
    void invalidSetExpirationIsRejected() {
        assertTrue(errorMessage(exec("SET", "k", "v", "EX", "0")).contains("invalid expire time"));
        assertTrue(errorMessage(exec("SET", "k", "v", "EX", "abc")).contains("not an integer"));
        assertTrue(errorMessage(exec("SET", "k", "v", "EX")).contains("wrong number"));
        assertTrue(errorMessage(exec("SET", "k", "v", "PX", "100")).contains("syntax error"));
        assertEquals(0, number(exec("EXISTS", "k")));
    }

    @Test
    void counterCommands() {
        assertEquals(1, number(exec("INCR", "visits")));
        assertEquals(2, number(exec("INCR", "visits")));
        assertEquals(1, number(exec("DECR", "visits")));
        assertEquals("1", bulk(exec("GET", "visits")));
    }

    @Test
    void invalidCounterIsRejected() {
        exec("SET", "counter", "abc");
        assertTrue(errorMessage(exec("INCR", "counter")).contains("not an integer"));
        exec("SET", "counter", Long.toString(Long.MAX_VALUE));
        assertTrue(errorMessage(exec("INCR", "counter")).contains("overflow"));
    }

    @Test
    void listCommands() {
        assertEquals(1, number(exec("LPUSH", "letters", "a")));
        assertEquals(2, number(exec("LPUSH", "letters", "b")));
        assertEquals(2, number(exec("LLEN", "letters")));
        assertEquals("a", bulk(exec("RPOP", "letters")));
        assertEquals("b", bulk(exec("RPOP", "letters")));
        assertEquals(0, number(exec("LLEN", "letters")));
        assertEquals(null, bulk(exec("RPOP", "letters")));
    }

    @Test
    void setCommands() {
        assertEquals(1, number(exec("SADD", "tags", "java")));
        assertEquals(0, number(exec("SADD", "tags", "java")));
        assertEquals(1, number(exec("SADD", "tags", "redis")));
        assertEquals(2, number(exec("SCARD", "tags")));
        assertEquals(1, number(exec("SISMEMBER", "tags", "java")));
        assertEquals(0, number(exec("SISMEMBER", "tags", "python")));
    }

    @Test
    void wrongTypeErrorsAreReadable() {
        exec("LPUSH", "list", "a");
        assertTrue(errorMessage(exec("GET", "list")).startsWith("WRONGTYPE"));
        assertTrue(errorMessage(exec("INCR", "list")).startsWith("WRONGTYPE"));
        exec("SET", "string", "x");
        assertTrue(errorMessage(exec("LPUSH", "string", "a")).startsWith("WRONGTYPE"));
        assertTrue(errorMessage(exec("SADD", "string", "a")).startsWith("WRONGTYPE"));
    }

    @Test
    void argumentValidationAndUnknownCommands() {
        assertTrue(errorMessage(exec("GET")).contains("wrong number"));
        assertTrue(errorMessage(exec("SET", "only-key")).contains("wrong number"));
        assertTrue(errorMessage(exec("LPUSH", "l", "a", "b")).contains("wrong number"));
        assertTrue(errorMessage(exec("SADD", "s")).contains("wrong number"));
        assertTrue(errorMessage(exec("NOPE")).contains("unknown command"));
    }

    @Test
    void utilityCommands() {
        assertInstanceOf(Response.SimpleString.class, exec("PING"));
        assertEquals("hello", bulk(exec("ECHO", "hello")));
        assertEquals("hello world", bulk(exec("PING", "hello world")));
        assertInstanceOf(Response.SimpleString.class, exec("QUIT"));
    }

    @Test
    void setOverwritesCollections() {
        exec("SADD", "key", "member");
        exec("SET", "key", "value");
        assertEquals("value", bulk(exec("GET", "key")));
        assertTrue(errorMessage(exec("SCARD", "key")).startsWith("WRONGTYPE"));
    }

    private static String errorMessage(Response response) {
        return assertInstanceOf(Response.ErrorReply.class, response).message();
    }
}
