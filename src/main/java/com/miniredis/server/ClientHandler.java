package com.miniredis.server;

import com.miniredis.command.CommandRegistry;
import com.miniredis.protocol.ProtocolException;
import com.miniredis.protocol.RespProtocol;
import com.miniredis.protocol.RespReader;
import com.miniredis.protocol.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Handles one client connection: read command, execute, write reply. */
final class ClientHandler implements Runnable {

    private static final Logger LOG = Logger.getLogger(ClientHandler.class.getName());

    private final Socket socket;
    private final CommandRegistry registry;

    ClientHandler(Socket socket, CommandRegistry registry) {
        this.socket = socket;
        this.registry = registry;
    }

    @Override
    public void run() {
        String remote = String.valueOf(socket.getRemoteSocketAddress());
        try (socket;
             InputStream in = new BufferedInputStream(socket.getInputStream());
             OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            RespReader reader = new RespReader(in);
            while (true) {
                List<String> command;
                try {
                    command = reader.readCommand();
                } catch (ProtocolException ex) {
                    RespProtocol.writeResponse(out,
                            Response.error("ERR Protocol error: " + ex.getMessage()));
                    out.flush();
                    break;
                }

                if (command == null) {
                    break;
                }
                if (command.isEmpty()) {
                    continue;
                }

                Response response = registry.execute(command);
                RespProtocol.writeResponse(out, response);
                out.flush();

                if ("QUIT".equalsIgnoreCase(command.get(0))) {
                    break;
                }
            }
        } catch (SocketTimeoutException ex) {
            LOG.fine(() -> "Idle timeout for client " + remote);
        } catch (SocketException ex) {
            LOG.fine(() -> "Client connection closed: " + remote);
        } catch (IOException ex) {
            LOG.log(Level.FINE, "I/O error for client " + remote, ex);
        } catch (RuntimeException ex) {
            LOG.log(Level.WARNING, "Unexpected client handler failure for " + remote, ex);
        }
    }
}
