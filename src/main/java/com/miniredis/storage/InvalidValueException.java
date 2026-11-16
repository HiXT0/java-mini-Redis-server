package com.miniredis.storage;

/** Thrown when an existing string cannot be parsed as a counter. */
public class InvalidValueException extends RuntimeException {

    public InvalidValueException(String message) {
        super(message);
    }
}
