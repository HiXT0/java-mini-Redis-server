package com.miniredis.persistence;

/** Wraps an I/O failure while writing the append-only file. */
public class PersistenceException extends RuntimeException {

    public PersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
