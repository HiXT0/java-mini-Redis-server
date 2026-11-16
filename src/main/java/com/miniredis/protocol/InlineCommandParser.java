package com.miniredis.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses the Redis "inline" command syntax, including single quotes, double
 * quotes, adjacent quoted sections, and backslash escapes. A value such as
 * {@code SET greeting "hello world"} is split into exactly three arguments.
 */
public final class InlineCommandParser {

    private InlineCommandParser() {
    }

    public static List<String> parse(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder token = new StringBuilder();
        boolean hasToken = false;
        boolean inSingle = false;
        boolean inDouble = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (inSingle) {
                if (c == '\\' && i + 1 < line.length()) {
                    i = appendEscape(line, i, token);
                } else if (c == '\'') {
                    inSingle = false;
                } else {
                    token.append(c);
                }
                continue;
            }

            if (inDouble) {
                if (c == '\\' && i + 1 < line.length()) {
                    i = appendEscape(line, i, token);
                } else if (c == '"') {
                    inDouble = false;
                } else {
                    token.append(c);
                }
                continue;
            }

            if (Character.isWhitespace(c)) {
                if (hasToken) {
                    tokens.add(token.toString());
                    token.setLength(0);
                    hasToken = false;
                }
            } else if (c == '"') {
                inDouble = true;
                hasToken = true;
            } else if (c == '\'') {
                inSingle = true;
                hasToken = true;
            } else if (c == '\\') {
                hasToken = true;
                i = appendEscape(line, i, token);
            } else {
                token.append(c);
                hasToken = true;
            }
        }

        if (inSingle || inDouble) {
            throw new IllegalArgumentException("unbalanced quotes in command");
        }
        if (hasToken) {
            tokens.add(token.toString());
        }
        return tokens;
    }

    /** Appends the escaped character at index i and returns the new index. */
    private static int appendEscape(String line, int i, StringBuilder token) {
        if (i + 1 >= line.length()) {
            throw new IllegalArgumentException("trailing backslash in command");
        }
        char escaped = line.charAt(i + 1);
        switch (escaped) {
            case 'n' -> token.append('\n');
            case 'r' -> token.append('\r');
            case 't' -> token.append('\t');
            case 'b' -> token.append('\b');
            case 'a' -> token.append('\u0007');
            case 'x' -> {
                if (i + 3 >= line.length()) {
                    throw new IllegalArgumentException("invalid hex escape");
                }
                String hex = line.substring(i + 2, i + 4);
                try {
                    token.append((char) Integer.parseInt(hex, 16));
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("invalid hex escape: " + hex);
                }
                return i + 3;
            }
            default -> token.append(escaped);
        }
        return i + 1;
    }
}
