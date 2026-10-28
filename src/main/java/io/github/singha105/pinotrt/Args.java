package io.github.singha105.pinotrt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Minimal {@code --key value} parser. Unknown keys are rejected so a typo cannot silently fall back to a default. */
final class Args {

    private final Map<String, String> values;

    private Args(Map<String, String> values) {
        this.values = values;
    }

    static Args parse(String[] argv, Set<String> allowed) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String token = argv[i];
            if (!token.startsWith("--")) {
                throw new IllegalArgumentException("expected --option, got: " + token);
            }
            String key = token.substring(2);
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("unknown option --" + key + " (allowed: " + allowed + ")");
            }
            boolean hasValue = i + 1 < argv.length && !argv[i + 1].startsWith("--");
            values.put(key, hasValue ? argv[++i] : "true");
        }
        return new Args(values);
    }

    String str(String key, String def) {
        return values.getOrDefault(key, def);
    }

    int integer(String key, int def) {
        return values.containsKey(key) ? Integer.parseInt(values.get(key)) : def;
    }

    long lng(String key, long def) {
        return values.containsKey(key) ? Long.parseLong(values.get(key)) : def;
    }

    double dbl(String key, double def) {
        return values.containsKey(key) ? Double.parseDouble(values.get(key)) : def;
    }

    boolean flag(String key) {
        return Boolean.parseBoolean(values.getOrDefault(key, "false"));
    }

    List<String> list(String key, String def) {
        List<String> out = new ArrayList<>();
        for (String part : str(key, def).split(",")) {
            if (!part.isBlank()) {
                out.add(part.trim());
            }
        }
        return out;
    }

    static Set<String> keys(String... keys) {
        return Set.copyOf(Arrays.asList(keys));
    }
}
