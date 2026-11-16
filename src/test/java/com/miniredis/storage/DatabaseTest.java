package com.miniredis.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseTest {

    private Database db;

    @BeforeEach
    void setUp() {
        db = new Database();
    }

    @Test
    void setAndGetRoundTrip() {
        db.set("name", "Rahul", 0);
        assertEquals("Rahul", db.get("name"));
        assertTrue(db.exists("name"));
        assertNull(db.get("missing"));
    }

    @Test
    void keysAndValuesAreCaseSensitive() {
        db.set("Key", "Value", 0);
        assertEquals("Value", db.get("Key"));
        assertNull(db.get("key"));
        assertNull(db.get("KEY"));
    }

    @Test
    void setOverwritesPreviousValueOfAnyType() {
        db.pushLeft("k", "a");
        db.set("k", "now-a-string", 0);
        assertEquals("now-a-string", db.get("k"));
        assertThrows(WrongTypeException.class, () -> db.listLength("k"));
    }

    @Test
    void deleteRemovesKey() {
        db.set("a", "1", 0);
        assertTrue(db.delete("a"));
        assertFalse(db.delete("a"));
        assertFalse(db.exists("a"));
    }

    @Test
    void keysAreSortedAndExcludeMissingKeys() {
        db.set("b", "2", 0);
        db.set("a", "1", 0);
        db.delete("b");
        assertEquals(List.of("a"), db.keys());
    }

    @Test
    void wrongTypeIsReportedForStrings() {
        db.pushLeft("list", "x");
        assertThrows(WrongTypeException.class, () -> db.get("list"));
        assertThrows(WrongTypeException.class, () -> db.increment("list", 1));
    }

    @Test
    void wrongTypeIsReportedForCollections() {
        db.set("string", "x", 0);
        assertThrows(WrongTypeException.class, () -> db.pushLeft("string", "y"));
        assertThrows(WrongTypeException.class, () -> db.popRight("string"));
        assertThrows(WrongTypeException.class, () -> db.listLength("string"));
        assertThrows(WrongTypeException.class, () -> db.addToSet("string", "m"));
        assertThrows(WrongTypeException.class, () -> db.isSetMember("string", "m"));
        assertThrows(WrongTypeException.class, () -> db.setSize("string"));
    }

    @Test
    void listPushPopSemantics() {
        assertEquals(1, db.pushLeft("l", "a"));
        assertEquals(2, db.pushLeft("l", "b"));
        assertEquals(3, db.pushLeft("l", "c"));
        assertEquals(3, db.listLength("l"));
        assertEquals("a", db.popRight("l"));
        assertEquals("b", db.popRight("l"));
        assertEquals("c", db.popRight("l"));
        assertNull(db.popRight("l"));
        assertFalse(db.exists("l"), "an empty list key is deleted");
    }

    @Test
    void noneExistentCollectionsBehaveLikeEmptyOnes() {
        assertEquals(0, db.listLength("missing"));
        assertNull(db.popRight("missing"));
        assertFalse(db.isSetMember("missing", "x"));
        assertEquals(0, db.setSize("missing"));
    }

    @Test
    void setSemantics() {
        assertEquals(1, db.addToSet("s", "a"));
        assertEquals(0, db.addToSet("s", "a"));
        assertEquals(1, db.addToSet("s", "b"));
        assertEquals(2, db.setSize("s"));
        assertTrue(db.isSetMember("s", "a"));
        assertFalse(db.isSetMember("s", "z"));
    }

    @Test
    void countersIncrementAndPreserveTtl() {
        long expiresAt = System.currentTimeMillis() + 60_000;
        db.set("visits", "10", expiresAt);
        assertEquals(11, db.increment("visits", 1));
        assertEquals(10, db.increment("visits", -1));
        assertEquals(10, db.increment("visits", 0));
        assertEquals(11, db.increment("visits", 1));
        assertEquals("11", db.get("visits"));
        assertTrue(db.ttl("visits") > 0);
    }

    @Test
    void invalidCounterValueIsRejected() {
        db.set("counter", "not-a-number", 0);
        assertThrows(InvalidValueException.class, () -> db.increment("counter", 1));
    }

    @Test
    void counterOverflowIsRejectedWithoutMutation() {
        db.set("counter", Long.toString(Long.MAX_VALUE), 0);
        assertThrows(InvalidValueException.class, () -> db.increment("counter", 1));
        assertEquals(Long.toString(Long.MAX_VALUE), db.get("counter"));
    }

    @Test
    void ttlAndPersist() {
        db.set("token", "abc", System.currentTimeMillis() + 60_000);
        assertTrue(db.ttl("token") > 0);
        assertTrue(db.persist("token"));
        assertEquals(-1, db.ttl("token"));
        assertFalse(db.persist("token"));
        assertEquals(-2, db.ttl("missing"));
    }

    @Test
    void lazyExpirationHidesAndRemovesKey() {
        db.set("short", "lived", System.currentTimeMillis() + 50);
        try {
            Thread.sleep(100);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        assertNull(db.get("short"));
        assertFalse(db.exists("short"));
        assertEquals(-2, db.ttl("short"));
        assertFalse(db.keys().contains("short"));
    }
}
