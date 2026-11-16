package com.miniredis.support;

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
import java.util.List;

/** Minimal RESP test client used by integration and concurrency tests. */
public final class TestClient implements Closeable {

    private final Socket socket;
    private final RespReader reader;
    private final OutputStream out;

    public TestClient(int port) throws IOException {
        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress("127.0.0.1", port), 3000);
        socket.setSoTimeout(10_000);
        InputStream in = new BufferedInputStream(socket.getInputStream());
        out = new BufferedOutputStream(socket.getOutputStream());
        reader = new RespReader(in);
    }

    public Response send(String... args) throws IOException {
        return send(List.of(args));
    }

    public Response send(List<String> args) throws IOException {
        RespProtocol.writeCommand(out, args);
        out.flush();
        return reader.readResponse();
    }

    public void writeRaw(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    public void writeRaw(String text) throws IOException {
        out.write(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        out.flush();
    }

    public Response read() throws IOException {
        return reader.readResponse();
    }

    public Socket socket() {
        return socket;
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Test cleanup is best effort.
        }
    }
}
