package com.miniredis.client;

import com.miniredis.protocol.RespProtocol;
import com.miniredis.protocol.RespReader;
import com.miniredis.protocol.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** A small blocking RESP2 client connection shared by the CLI and benchmark. */
public final class RedisConnection implements Closeable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final RespReader reader;

    private RedisConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());
        this.reader = new RespReader(in);
    }

    public static RedisConnection connect(String host, int port, int connectTimeoutMillis)
            throws IOException {
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            return new RedisConnection(socket);
        } catch (IOException ex) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Preserve the original connection failure.
            }
            throw ex;
        }
    }

    /**
     * Sends one command and waits for its reply. Returns {@code null} if the
     * server cleanly closed the connection.
     */
    public Response send(String... args) throws IOException {
        return send(List.of(args));
    }

    public Response send(List<String> args) throws IOException {
        RespProtocol.writeCommand(out, args);
        out.flush();
        return reader.readResponse();
    }

    public Response readResponse() throws IOException {
        return reader.readResponse();
    }

    public boolean isClosed() {
        return socket.isClosed();
    }

    public void sendUtf8Raw(String text) throws IOException {
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing a client connection is best effort.
        }
    }
}
