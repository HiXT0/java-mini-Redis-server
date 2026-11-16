package com.miniredis.storage;

/** Thrown when a command is applied to a key holding a different value type. */
public class WrongTypeException extends RuntimeException {

    public WrongTypeException() {
        super("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
