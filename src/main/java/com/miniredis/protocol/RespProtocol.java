package com.miniredis.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Encodes commands and replies using RESP2. */
public final class RespProtocol {

    private static final byte[] CRLF = {'\r', '\n'};

    private RespProtocol() {
    }

    public static byte[] encodeCommand(List<String> args) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            writeCommand(buffer, args);
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory write failed", impossible);
        }
        return buffer.toByteArray();
    }

    public static void writeCommand(OutputStream out, List<String> args) throws IOException {
        out.write('*');
        writeAscii(out, Integer.toString(args.size()));
        out.write(CRLF);
        for (String arg : args) {
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            out.write('$');
            writeAscii(out, Integer.toString(bytes.length));
            out.write(CRLF);
            out.write(bytes);
            out.write(CRLF);
        }
    }

    public static void writeResponse(OutputStream out, Response response) throws IOException {
        if (response instanceof Response.SimpleString simple) {
            out.write('+');
            writeAscii(out, simple.value());
            out.write(CRLF);
        } else if (response instanceof Response.ErrorReply error) {
            out.write('-');
            writeAscii(out, sanitize(error.message()));
            out.write(CRLF);
        } else if (response instanceof Response.IntegerReply integer) {
            out.write(':');
            writeAscii(out, Long.toString(integer.value()));
            out.write(CRLF);
        } else if (response instanceof Response.BulkString bulk) {
            if (bulk.value() == null) {
                writeAscii(out, "$-1\r\n");
            } else {
                byte[] bytes = bulk.value().getBytes(StandardCharsets.UTF_8);
                out.write('$');
                writeAscii(out, Integer.toString(bytes.length));
                out.write(CRLF);
                out.write(bytes);
                out.write(CRLF);
            }
        } else if (response instanceof Response.ArrayReply array) {
            out.write('*');
            writeAscii(out, Integer.toString(array.values().size()));
            out.write(CRLF);
            for (Response value : array.values()) {
                writeResponse(out, value);
            }
        } else {
            throw new IllegalArgumentException("Unknown response type: " + response);
        }
    }

    /** User-friendly rendering used by the command-line client. */
    public static String format(Response response) {
        StringBuilder builder = new StringBuilder();
        appendFormatted(builder, response, "");
        return builder.toString().stripTrailing();
    }

    private static void appendFormatted(StringBuilder builder, Response response, String indent) {
        if (response instanceof Response.SimpleString simple) {
            builder.append(simple.value()).append('\n');
        } else if (response instanceof Response.ErrorReply error) {
            builder.append("(error) ").append(error.message()).append('\n');
        } else if (response instanceof Response.IntegerReply integer) {
            builder.append("(integer) ").append(integer.value()).append('\n');
        } else if (response instanceof Response.BulkString bulk) {
            if (bulk.value() == null) {
                builder.append("(nil)\n");
            } else if (bulk.value().isEmpty()) {
                builder.append("(empty string)\n");
            } else {
                builder.append(bulk.value()).append('\n');
            }
        } else if (response instanceof Response.ArrayReply array) {
            if (array.values().isEmpty()) {
                builder.append("(empty array)\n");
                return;
            }
            for (int i = 0; i < array.values().size(); i++) {
                builder.append(indent).append(i + 1).append(") ");
                appendFormatted(builder, array.values().get(i), indent + "   ");
            }
        }
    }

    private static String sanitize(String message) {
        return message == null ? "ERR unknown error"
                : message.replace('\r', ' ').replace('\n', ' ');
    }

    private static void writeAscii(OutputStream out, String text) throws IOException {
        out.write(text.getBytes(StandardCharsets.UTF_8));
    }
}
