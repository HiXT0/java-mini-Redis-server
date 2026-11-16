package com.miniredis.persistence;

import java.io.IOException;
import java.util.List;

/** Receives canonical commands for successful mutations. */
@FunctionalInterface
public interface MutationLog {

    MutationLog NOOP = command -> {
    };

    void append(List<String> command) throws IOException;
}
