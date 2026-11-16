package com.miniredis.protocol;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads commands (RESP arrays or inline commands) from a client and RESP
 * replies from a server. The stream is consumed byte-by-byte through a
 * buffered input stream, so commands may arrive fragmented across TCP reads.
 */
public final class RespReader {

    private static final int MAX_LINE = 64 * 1024;
    private static final int MAX_BULK = 1024 * 1024;
    private static final int MAX_ARGS = 1024;
    private static final int MAX_RESPONSE_BULK = 64 * 1024 * 1024;

    private final InputStream in;

    public RespReader(InputStream in) {
        this.in = in;
    }

    /**
     * Reads one command. Returns {@code null} on a clean end of stream.
     * A returned list may be empty for a blank inline line.
     */
    public List<String> readCommand() throws IOException {
        int first = in.read();
        if (first == -1) {
            return null;
        }
        if (first == '*') {
            return readCommandArray();
        }
        try {
            return InlineCommandParser.parse(readLine(first));
        } catch (IllegalArgumentException ex) {
            throw new ProtocolException(ex.getMessage());
        }
    }

    /** Reads one RESP reply. Returns {@code null} on a clean end of stream. */
    public Response readResponse() throws IOException {
        int first = in.read();
        if (first == -1) {
            return null;
        }
        return readResponse(first, MAX_RESPONSE_BULK);
    }

    private List<String> readCommandArray() throws IOException {
        long count = parseLong(readLine(-1), "array length");
        if (count < 0 || count > MAX_ARGS) {
            throw new ProtocolException("invalid multibulk length");
        }
        List<String> args = new ArrayList<>((int) count);
        for (int i = 0; i < count; i++) {
            int type = in.read();
            if (type == -1) {
                throw new ProtocolException("unexpected end of stream inside array");
            }
            if (type != '$') {
                throw new ProtocolException("expected a bulk string argument");
            }
            long length = parseLong(readLine(-1), "bulk length");
            if (length < 0 || length > MAX_BULK) {
                throw new ProtocolException("invalid bulk length");
            }
            args.add(readUtf8((int) length));
        }
        return args;
    }

    private Response readResponse(int type, int maxBulk) throws IOException {
        switch (type) {
            case '+' -> {
                return Response.simple(readLine(-1));
            }
            case '-' -> {
                return Response.error(readLine(-1));
            }
            case ':' -> {
                return Response.integer(parseLong(readLine(-1), "integer reply"));
            }
            case '$' -> {
                long length = parseLong(readLine(-1), "bulk length");
                if (length == -1) {
                    return Response.nil();
                }
                if (length < 0 || length > maxBulk) {
                    throw new ProtocolException("invalid bulk length");
                }
                return Response.bulk(readUtf8((int) length));
            }
            case '*' -> {
                long count = parseLong(readLine(-1), "array length");
                if (count == -1) {
                    return Response.nil();
                }
                if (count < 0 || count > MAX_ARGS) {
                    throw new ProtocolException("invalid array length");
                }
                List<Response> values = new ArrayList<>((int) count);
                for (int i = 0; i < count; i++) {
                    int next = in.read();
                    if (next == -1) {
                        throw new ProtocolException("unexpected end of stream inside array");
                    }
                    values.add(readResponse(next, maxBulk));
                }
                return Response.array(values);
            }
            default -> throw new ProtocolException("unknown reply type byte: " + (char) type);
        }
    }

    private String readUtf8(int length) throws IOException {
        byte[] bytes = in.readNBytes(length);
        if (bytes.length < length) {
            throw new ProtocolException("unexpected end of stream inside bulk string");
        }
        expectCrlf();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Reads a CRLF terminated line. The first byte may already have been
     * consumed by the caller. Accepts a bare LF for telnet friendliness.
     */
    private String readLine(int firstByte) throws IOException {
        if (firstByte == '\n') {
            return "";
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        if (firstByte != -1) {
            buffer.write(firstByte);
        }
        while (true) {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("connection closed in the middle of a line");
            }
            if (b == '\n') {
                break;
            }
            buffer.write(b);
            if (buffer.size() > MAX_LINE) {
                throw new ProtocolException("line too long");
            }
        }
        byte[] bytes = buffer.toByteArray();
        int length = bytes.length;
        if (length > 0 && bytes[length - 1] == '\r') {
            length--;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    private void expectCrlf() throws IOException {
        int first = in.read();
        if (first == '\r') {
            int second = in.read();
            if (second != '\n') {
                throw new ProtocolException("expected LF after CR");
            }
        } else if (first != '\n') {
            throw new ProtocolException("expected CRLF after bulk string");
        }
    }

    private static long parseLong(String text, String what) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ex) {
            throw new ProtocolException("invalid " + what);
        }
    }
}
