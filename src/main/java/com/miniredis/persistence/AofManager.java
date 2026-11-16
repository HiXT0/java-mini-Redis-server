package com.miniredis.persistence;

import com.miniredis.protocol.ProtocolException;
import com.miniredis.protocol.RespReader;
import com.miniredis.protocol.RespProtocol;
import com.miniredis.storage.Database;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 * A crash-tolerant append-only file.
 *
 * <p>File layout: a fixed text magic header, then records consisting of an
 * 8-byte header (payload length and CRC32) followed by a RESP-encoded command.
 * RESP provides unambiguous framing for values containing spaces, quotes or
 * newlines. Length and checksum make truncation and corruption detectable.</p>
 *
 * <p>A partial record at the end of the file (a crash during append) is
 * discarded and truncated away. A checksum failure stops replay at the last
 * consistent record and truncates the damaged suffix. Semantic errors in an
 * otherwise valid record are skipped with a warning.</p>
 */
public final class AofManager implements MutationLog, AutoCloseable {

    private static final Logger LOG = Logger.getLogger(AofManager.class.getName());
    private static final byte[] MAGIC = "MINIREDIS-AOF-1\n".getBytes(StandardCharsets.US_ASCII);
    private static final int RECORD_HEADER_BYTES = 8;
    private static final int MAX_RECORD_BYTES = 16 * 1024 * 1024;

    public enum FsyncPolicy {
        /** fsync before every successful mutation is acknowledged (default). */
        ALWAYS,
        /** fsync at most once per second; at most ~1 second of writes may be lost. */
        EVERYSEC,
        /** Never call fsync; the operating system decides when to flush. */
        NO;

        public static FsyncPolicy parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "always" -> ALWAYS;
                case "everysec" -> EVERYSEC;
                case "no" -> NO;
                default -> throw new IllegalArgumentException(
                        "fsync policy must be always, everysec, or no");
            };
        }
    }

    public record ReplayStats(int recordsApplied, int recordsSkipped, long bytesTruncated) {
    }

    private final Path path;
    private final FsyncPolicy fsyncPolicy;
    private final Object writeLock = new Object();

    private FileOutputStream output;
    private long lastSyncNanos;
    private volatile boolean closed;
    private volatile boolean dirty;
    private Thread syncThread;
    private volatile IOException writeFailure;

    public AofManager(Path path, FsyncPolicy fsyncPolicy) {
        this.path = path.toAbsolutePath().normalize();
        this.fsyncPolicy = fsyncPolicy;
    }

    /**
     * Validates/creates the file, replays all complete records into the
     * database, truncates an incomplete tail, then opens the file for append.
     */
    public ReplayStats load(Database database) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (!Files.exists(path)) {
            Files.createFile(path);
        }

        if (Files.size(path) == 0) {
            try (FileOutputStream initial = new FileOutputStream(path.toFile())) {
                initial.write(MAGIC);
                initial.getFD().sync();
            }
        }
        verifyMagic();

        int applied = 0;
        int skipped = 0;
        long validOffset = MAGIC.length;
        boolean stop = false;

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(path.toFile())))) {
            byte[] magic = in.readNBytes(MAGIC.length);
            if (magic.length != MAGIC.length) {
                throw new IOException("AOF header is incomplete");
            }

            while (!stop) {
                byte[] header = in.readNBytes(RECORD_HEADER_BYTES);
                if (header.length == 0) {
                    break;
                }
                if (header.length < RECORD_HEADER_BYTES) {
                    LOG.warning("AOF has an incomplete record header; truncating tail");
                    break;
                }

                ByteBuffer headerBuffer = ByteBuffer.wrap(header);
                int payloadLength = headerBuffer.getInt();
                int expectedCrc = headerBuffer.getInt();

                if (payloadLength <= 0 || payloadLength > MAX_RECORD_BYTES) {
                    LOG.warning("AOF record has an invalid length; truncating from here");
                    break;
                }

                byte[] payload = in.readNBytes(payloadLength);
                if (payload.length < payloadLength) {
                    LOG.warning("AOF ends with an incomplete record; truncating tail");
                    break;
                }

                if (crc32(payload) != expectedCrc) {
                    LOG.warning("AOF record checksum mismatch; truncating damaged suffix");
                    break;
                }

                List<String> command;
                try {
                    command = decode(payload);
                } catch (ProtocolException ex) {
                    LOG.warning("Skipping AOF record with invalid command encoding: " + ex.getMessage());
                    skipped++;
                    validOffset += RECORD_HEADER_BYTES + payloadLength;
                    continue;
                }

                try {
                    apply(database, command);
                    applied++;
                } catch (RuntimeException ex) {
                    LOG.warning("Skipping AOF record that could not be applied: " + ex.getMessage());
                    skipped++;
                }
                validOffset += RECORD_HEADER_BYTES + payloadLength;
            }
        }

        long fileSize = Files.size(path);
        long truncated = 0;
        if (fileSize > validOffset) {
            truncated = fileSize - validOffset;
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
                channel.truncate(validOffset);
                channel.force(true);
            }
            LOG.warning("Truncated " + truncated + " bytes of incomplete/corrupt AOF data");
        }

        output = new FileOutputStream(path.toFile(), true);
        lastSyncNanos = System.nanoTime();
        if (fsyncPolicy == FsyncPolicy.EVERYSEC) {
            syncThread = new Thread(this::syncLoop, "mini-redis-aof-sync");
            syncThread.setDaemon(true);
            syncThread.start();
        }
        return new ReplayStats(applied, skipped, truncated);
    }

    @Override
    public void append(List<String> command) throws IOException {
        byte[] payload = RespProtocol.encodeCommand(command);
        if (payload.length > MAX_RECORD_BYTES) {
            throw new IOException("AOF record is too large: " + payload.length + " bytes");
        }
        byte[] header = ByteBuffer.allocate(RECORD_HEADER_BYTES)
                .putInt(payload.length)
                .putInt(crc32(payload))
                .array();

        synchronized (writeLock) {
            if (closed || output == null) {
                throw new IOException("AOF is not open");
            }
            if (writeFailure != null) {
                throw new IOException("AOF is in a failed state after an earlier I/O error", writeFailure);
            }
            try {
                output.write(header);
                output.write(payload);
                output.flush();
                dirty = true;
                syncIfNeeded();
            } catch (IOException ex) {
                // A partially written record may now sit at the tail. Refuse
                // further appends so recovery only sees one damaged suffix.
                writeFailure = ex;
                throw ex;
            }
        }
    }

    private void syncIfNeeded() throws IOException {
        switch (fsyncPolicy) {
            case ALWAYS -> syncLocked();
            case EVERYSEC -> {
                long now = System.nanoTime();
                if (now - lastSyncNanos >= 1_000_000_000L) {
                    syncLocked();
                }
            }
            case NO -> {
                // Deliberately rely on the OS page cache.
            }
        }
    }

    /** Caller must hold writeLock. */
    private void syncLocked() throws IOException {
        output.getFD().sync();
        lastSyncNanos = System.nanoTime();
        dirty = false;
    }

    /** Background fsync makes the everysec policy hold even when writes stop. */
    private void syncLoop() {
        while (!closed) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                return;
            }
            synchronized (writeLock) {
                if (closed || output == null) {
                    return;
                }
                if (dirty && System.nanoTime() - lastSyncNanos >= 1_000_000_000L) {
                    try {
                        syncLocked();
                    } catch (IOException ex) {
                        LOG.warning("Background AOF sync failed: " + ex.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        Thread threadToJoin;
        synchronized (writeLock) {
            if (closed) {
                return;
            }
            closed = true;
            threadToJoin = syncThread;
            if (output != null) {
                try {
                    output.flush();
                    output.getFD().sync();
                    output.close();
                } catch (IOException ex) {
                    LOG.warning("Error while closing AOF: " + ex.getMessage());
                }
            }
        }
        if (threadToJoin != null && threadToJoin != Thread.currentThread()) {
            threadToJoin.interrupt();
            try {
                threadToJoin.join(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Path path() {
        return path;
    }

    private void verifyMagic() throws IOException {
        byte[] actual;
        try (FileInputStream in = new FileInputStream(path.toFile())) {
            actual = in.readNBytes(MAGIC.length);
        }
        if (actual.length != MAGIC.length) {
            throw new IOException("Not a Mini Redis AOF (header is incomplete): " + path);
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (actual[i] != MAGIC[i]) {
                throw new IOException("Not a Mini Redis AOF (bad magic header): " + path);
            }
        }
    }

    private static List<String> decode(byte[] payload) {
        try {
            List<String> command = new RespReader(new ByteArrayInputStream(payload)).readCommand();
            if (command == null || command.isEmpty()) {
                throw new ProtocolException("empty command");
            }
            return command;
        } catch (IOException ex) {
            throw new ProtocolException(ex.getMessage());
        }
    }

    /** Applies an internal AOF command with logging disabled. */
    private static void apply(Database database, List<String> command) {
        String name = command.get(0).toUpperCase(Locale.ROOT);
        switch (name) {
            case "SET" -> {
                if (command.size() != 3 && command.size() != 5) {
                    throw new IllegalArgumentException("bad SET record");
                }
                long expiresAt = 0L;
                if (command.size() == 5) {
                    if (!"PXAT".equalsIgnoreCase(command.get(3))) {
                        throw new IllegalArgumentException("bad SET expiration record");
                    }
                    expiresAt = Long.parseLong(command.get(4));
                }
                database.set(require(command, 1), require(command, 2), expiresAt);
            }
            case "DEL" -> {
                requireArity(command, 2);
                database.delete(command.get(1));
            }
            case "LPUSH" -> {
                requireArity(command, 3);
                database.pushLeft(command.get(1), command.get(2));
            }
            case "RPOP" -> {
                requireArity(command, 2);
                database.popRight(command.get(1));
            }
            case "SADD" -> {
                requireArity(command, 3);
                database.addToSet(command.get(1), command.get(2));
            }
            case "PERSIST" -> {
                requireArity(command, 2);
                database.persist(command.get(1));
            }
            default -> throw new IllegalArgumentException("unknown AOF command: " + name);
        }
    }

    private static String require(List<String> command, int index) {
        if (index >= command.size()) {
            throw new IllegalArgumentException("AOF record is missing an argument");
        }
        return command.get(index);
    }

    private static void requireArity(List<String> command, int expected) {
        if (command.size() != expected) {
            throw new IllegalArgumentException("bad " + command.get(0) + " record");
        }
    }

    private static int crc32(byte[] payload) {
        CRC32 crc = new CRC32();
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }
}
