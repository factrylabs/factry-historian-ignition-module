package io.factry.historian.gateway;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which tags' refusals (see {@link MeasurementCache.WriteRefusal}) the gateway log has
 * reported, so it says so once per tag rather than once per value.
 */
final class RefusalLog {
    private final Map<String, MeasurementCache.WriteRefusal> reported = new ConcurrentHashMap<>();

    /** Record a refusal of {@code tagPath}; true when it is new, so it should be reported. */
    boolean refused(String tagPath, MeasurementCache.WriteRefusal refusal) {
        return reported.put(tagPath, refusal) != refusal;
    }

    /**
     * Record that {@code tagPath} was written, so a later refusal is reported again. Returns the
     * refusal it had, or null.
     */
    MeasurementCache.WriteRefusal written(String tagPath) {
        return reported.remove(tagPath);
    }

    boolean isReported(String tagPath) {
        return reported.containsKey(tagPath);
    }

    void clear() {
        reported.clear();
    }

    /** The first {@code shown} tag paths, and how many more: "a, b, c and 2 more". */
    static String names(List<String> tagPaths, int shown) {
        if (tagPaths.size() <= shown) {
            return String.join(", ", tagPaths);
        }
        return String.join(", ", tagPaths.subList(0, shown)) + " and " + (tagPaths.size() - shown) + " more";
    }
}
