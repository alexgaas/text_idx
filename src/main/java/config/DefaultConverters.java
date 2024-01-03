package config;

import java.util.HashMap;
import java.util.Map;

public class DefaultConverters {
    private static final Map<String, Long> TIMES = new HashMap<>();
    private static final Map<String, Long> SIZES = new HashMap<>();

    static {
        TIMES.put("ms", 1L);
        TIMES.put("s", 1000L);
        TIMES.put("m", 60 * 1000L);
        TIMES.put("h", 3600 * 1000L);
        TIMES.put("d", 24 * 3600 * 1000L);

        SIZES.put("k", 1024L);
        SIZES.put("m", 1024 * 1024L);
        SIZES.put("g", 1024 * 1024 * 1024L);
    }

    public static int time(String s) {
        return (int) longTime(s);
    }

    public static long longTime(String s) {
        return parseMultiplier(s, TIMES);
    }

    public static int size(String s) {
        return (int) longSize(s);
    }

    public static long longSize(String s) {
        return parseMultiplier(s, SIZES);
    }

    public static long parseMultiplier(String s, Map<String, Long> multipliers) {
        int length = s.length();
        for (int i = 0; i < length; i++) {
            if (s.charAt(i) > '9') {
                String suffix = s.substring(i).toLowerCase();
                Long multiplier = multipliers.get(suffix);
                if (multiplier == null) {
                    throw new IllegalArgumentException("Unknown suffix: " + suffix);
                }
                return Long.parseLong(s.substring(0, i)) * multiplier;
            }
        }
        return Long.parseLong(s);
    }
}