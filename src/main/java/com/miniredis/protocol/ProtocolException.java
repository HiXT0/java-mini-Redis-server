package com.miniredis.protocol;

/** Raised when an incoming client message cannot be parsed. */
public class ProtocolException extends RuntimeException {

    public ProtocolException(String message) {
        super(message);
    }
}
