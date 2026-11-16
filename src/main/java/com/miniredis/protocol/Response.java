package com.miniredis.protocol;

import java.util.List;

/**
 * A protocol-neutral representation of a reply sent to a client.
 * It maps directly onto the five RESP2 reply types.
 */
public sealed interface Response {

    record SimpleString(String value) implements Response {
    }

    record ErrorReply(String message) implements Response {
    }

    record IntegerReply(long value) implements Response {
    }

    /** A bulk string. A null value represents a RESP null bulk string. */
    record BulkString(String value) implements Response {
    }

    record ArrayReply(List<Response> values) implements Response {
        public ArrayReply {
            values = List.copyOf(values);
        }
    }

    static Response ok() {
        return new SimpleString("OK");
    }

    static Response simple(String value) {
        return new SimpleString(value);
    }

    static Response error(String message) {
        return new ErrorReply(message);
    }

    static Response integer(long value) {
        return new IntegerReply(value);
    }

    static Response bulk(String value) {
        return new BulkString(value);
    }

    static Response nil() {
        return new BulkString(null);
    }

    static Response array(List<Response> values) {
        return new ArrayReply(values);
    }
}
