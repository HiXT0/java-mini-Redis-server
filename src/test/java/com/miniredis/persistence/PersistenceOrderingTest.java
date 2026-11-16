package com.miniredis.persistence;

import com.miniredis.storage.Database;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies write-ahead ordering: if the log cannot accept a mutation, memory
 * must not change. This is tested with an injected failing mutation log.
 */
class PersistenceOrderingTest {

    private Database db;

    @BeforeEach
    void setUp() {
        db = new Database();
    }

    private void failAllWrites() {
        db.setMutationLog(command -> {
            throw new IOException("simulated disk full");
        });
    }

    @Test
    void failedSetDoesNotChangeMemory() {
        db.set("key", "old", 0);
        failAllWrites();

        assertThrows(PersistenceException.class, () -> db.set("key", "new", 0));
        assertEquals("old", db.get("key"));
        assertThrows(PersistenceException.class, () -> db.set("brand-new", "value", 0));
        assertNull(db.get("brand-new"));
    }

    @Test
    void failedIncrementDoesNotChangeCounter() {
        db.set("counter", "10", 0);
        failAllWrites();

        assertThrows(PersistenceException.class, () -> db.increment("counter", 1));
        assertEquals("10", db.get("counter"));
    }

    @Test
    void failedListMutationDoesNotChangeList() {
        db.pushLeft("list", "a");
        failAllWrites();

        assertThrows(PersistenceException.class, () -> db.pushLeft("list", "b"));
        assertEquals(1, db.listLength("list"));

        db.setMutationLog(MutationLog.NOOP);
        assertEquals("a", db.popRight("list"), "the failed push must not appear");

        db.pushLeft("other", "x");
        failAllWrites();
        assertThrows(PersistenceException.class, () -> db.popRight("other"));
        assertEquals(1, db.listLength("other"), "a failed RPOP must not remove data");
    }

    @Test
    void failedSetMutationDoesNotChangeSet() {
        db.addToSet("tags", "java");
        failAllWrites();

        assertThrows(PersistenceException.class, () -> db.addToSet("tags", "redis"));
        assertEquals(1, db.setSize("tags"));
        assertFalse(db.isSetMember("tags", "redis"));

        failAllWrites();
        assertThrows(PersistenceException.class, () -> db.addToSet("new-set", "member"));
        assertEquals(0, db.setSize("new-set"));
    }

    @Test
    void failedDeleteDoesNotRemoveKey() {
        db.set("key", "value", 0);
        failAllWrites();

        assertThrows(PersistenceException.class, () -> db.delete("key"));
        assertTrue(db.exists("key"));
    }

    @Test
    void failedPersistLeavesTtlIntact() {
        long expiresAt = System.currentTimeMillis() + 60_000;
        db.set("key", "value", expiresAt);
        failAllWrites();

        assertThrows(PersistenceException.class, () -> db.persist("key"));
        assertTrue(db.ttl("key") > 0);
    }
}
