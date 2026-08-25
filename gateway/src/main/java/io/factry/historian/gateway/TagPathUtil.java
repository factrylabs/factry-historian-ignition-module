package io.factry.historian.gateway;

/**
 * Utility methods for converting between Ignition QualifiedPath strings
 * and Factry measurement names.
 * <p>
 * Measurement name format: {@code "provider/tagPath"}
 * <p>
 * Example: {@code "default/FactrySim/ff1"}
 * <p>
 * No Ignition SDK dependencies — pure string operations, easy to unit test.
 */
final class TagPathUtil {

    static final String CATEGORY_MEASUREMENTS = "Measurements";
    static final String CATEGORY_ASSETS = "Assets";

    private TagPathUtil() {}

    /**
     * Extract a named component value from a QualifiedPath string representation.
     * Components are separated by {@code ":/"}. The match must occur at a component
     * boundary (start of string or after {@code "/"}) to avoid false positives
     * like matching {@code "prov:"} inside {@code "histprov:"}.
     *
     * @param path   the full QualifiedPath string
     * @param prefix the component prefix including colon, e.g. {@code "sys:"}, {@code "prov:"}, {@code "tag:"}
     * @return the component value, or null if not found
     */
    static String extractComponent(String path, String prefix) {
        int searchFrom = 0;
        while (true) {
            int idx = path.indexOf(prefix, searchFrom);
            if (idx < 0) return null;
            // Must be at a component boundary: start of string or after "/"
            if (idx == 0 || path.charAt(idx - 1) == '/') {
                int start = idx + prefix.length();
                int end = path.indexOf(":/", start);
                return end >= 0 ? path.substring(start, end) : path.substring(start);
            }
            searchFrom = idx + 1;
        }
    }

    /**
     * Build the measurement name from components.
     * Format: {@code "prov/tag"}
     *
     * @return measurement name, e.g. {@code "default/FactrySim/ff1"}
     */
    static String buildStoredPath(String prov, String tag) {
        if (prov == null) prov = "default";
        return prov + "/" + tag;
    }

    /**
     * Convert a QualifiedPath string to the Factry measurement name format.
     * <p>
     * Handles multiple input formats:
     * <ul>
     *   <li>Storage path: {@code sys:X:/prov:Y:/tag:Z} → {@code Y/Z}</li>
     *   <li>Browse with folders: {@code histprov:xxx:/folder:A:/folder:B:/tag:C} → {@code A/B/C}
     *       (category prefix stripped if present)</li>
     *   <li>Browse without folders: {@code histprov:xxx:/tag:prov/tag}
     *       → {@code prov/tag} (already in correct format)</li>
     * </ul>
     *
     * @param qualifiedPathStr the QualifiedPath.toString() result
     * @return the Factry measurement name
     */
    /**
     * Sentinel substituted for a literal '/' in a measurement name so the browse
     * tree (which always splits on '/') does not treat it as a path separator.
     */
    static final String FRACTION_SLASH = " \u2215 ";

    /**
     * Restore unicode fraction slash (U+2044) back to real '/' in stored paths.
     * Used for measurements where '/' was replaced to prevent the browse tree
     * from splitting the path into segments.
     */
    static String restoreFractionSlash(String path) {
        if (path == null) return null;
        return path.replace(FRACTION_SLASH, "/").replace('\u2215', '/').replace('\u2044', '/');
    }

    /**
     * Convert a stored Factry measurement name into the name used for browsing,
     * driven by the user-configured tag-path {@code delimiter}.
     * <p>
     * The browse tree always splits leaf paths on {@code '/'}, so the delimiter
     * decides what becomes a tree separator:
     * <ul>
     *   <li>{@code "/"} (default): no-op \u2014 Ignition splits the path into a tree as-is.</li>
     *   <li>empty/null: flat \u2014 every {@code '/'} is escaped to {@link #FRACTION_SLASH}
     *       so the whole name stays a single leaf.</li>
     *   <li>any other character (e.g. {@code "."}): escape literal {@code '/'} first,
     *       then promote the delimiter to {@code '/'} so the tree splits on it.</li>
     * </ul>
     * Order matters in the last case: escape before promoting, otherwise the freshly
     * promoted slashes would be re-escaped.
     *
     * @see #fromBrowseName(String, String) for the exact inverse
     */
    static String toBrowseName(String name, String delimiter) {
        if (name == null) return null;
        if ("/".equals(delimiter)) {
            return name;
        }
        String escaped = name.replace("/", FRACTION_SLASH);
        if (delimiter == null || delimiter.isEmpty()) {
            return escaped;
        }
        return escaped.replace(delimiter, "/");
    }

    /**
     * Inverse of {@link #toBrowseName(String, String)}: reconstruct the stored
     * measurement name from a browse name resolved out of a query path.
     * <p>
     * Mirrors the forward transform in reverse order: former-delimiter {@code '/'}
     * are restored to the delimiter first, then escaped slashes are restored to
     * real {@code '/'}.
     */
    static String fromBrowseName(String browseName, String delimiter) {
        if (browseName == null) return null;
        if ("/".equals(delimiter)) {
            return browseName;
        }
        String result = browseName;
        if (delimiter != null && !delimiter.isEmpty()) {
            result = result.replace("/", delimiter);
        }
        return restoreFractionSlash(result);
    }

    /**
     * True if a query QualifiedPath refers to the Assets tree rather than the
     * Measurements tree. Asset property tags carry their measurement's real name
     * (which natively contains '/'), so the delimiter reverse-transform must never
     * be applied to them.
     */
    static boolean isAssetQueryPath(String qualifiedPathStr) {
        if (qualifiedPathStr == null) return false;
        if (CATEGORY_ASSETS.equals(extractCategory(parseFolderPrefix(qualifiedPathStr)))) {
            return true;
        }
        String tag = extractComponent(qualifiedPathStr, "tag:");
        return tag != null && CATEGORY_ASSETS.equals(extractCategory(tag));
    }

    /**
     * Convert a storage-pipeline QualifiedPath to the Factry measurement name.
     * <p>
     * The tag value is always a genuine path relative to the provider.
     * No composite-path guessing is needed — the components are trusted as-is.
     */
    static String storagePathToStoredPath(String qualifiedPathStr) {
        String prov = extractComponent(qualifiedPathStr, "prov:");
        String tag = extractComponent(qualifiedPathStr, "tag:");

        if (tag == null) {
            return qualifiedPathStr;
        }

        return buildStoredPath(prov, tag);
    }

    /**
     * Convert a query/browse QualifiedPath to the Factry measurement name.
     * <p>
     * In cross-gateway scenarios the tag value may already be a full measurement
     * name (prov/tagPath) that must not be wrapped again.
     */
    static String queryPathToStoredPath(String qualifiedPathStr) {
        String prov = extractComponent(qualifiedPathStr, "prov:");
        String tag = extractComponent(qualifiedPathStr, "tag:");

        if (prov != null && tag != null) {
            String sys = extractComponent(qualifiedPathStr, "sys:");
            if (sys != null) {
                if (tag.startsWith(prov + "/")) {
                    return tag;
                }
                return buildStoredPath(prov, tag);
            }
            return tag;
        }

        if (tag != null) {
            String folderPrefix = parseFolderPrefix(qualifiedPathStr);
            if (!folderPrefix.isEmpty()) {
                String strippedPrefix = stripCategory(folderPrefix);
                if (strippedPrefix.length() < folderPrefix.length()) {
                    String category = extractCategory(folderPrefix);
                    if (CATEGORY_ASSETS.equals(category)) {
                        return strippedPrefix + tag;
                    }
                    folderPrefix = strippedPrefix;
                }
                return folderPrefix + tag;
            }

            String category = extractCategory(tag);
            if (category != null) {
                return stripCategory(tag);
            }

            return tag;
        }

        return qualifiedPathStr;
    }

    /**
     * Extract the category prefix from a display/browse path.
     * Returns "Measurements", "Assets", or null if no category prefix.
     */
    static String extractCategory(String displayPath) {
        if (displayPath == null) return null;
        if (displayPath.startsWith(CATEGORY_MEASUREMENTS + "/") || displayPath.equals(CATEGORY_MEASUREMENTS)) {
            return CATEGORY_MEASUREMENTS;
        }
        if (displayPath.startsWith(CATEGORY_ASSETS + "/") || displayPath.equals(CATEGORY_ASSETS)) {
            return CATEGORY_ASSETS;
        }
        return null;
    }

    /**
     * Strip the category prefix from a display/browse path.
     * {@code "Measurements/Ignition/default/Tag"} → {@code "Ignition/default/Tag"}
     */
    static String stripCategory(String displayPath) {
        if (displayPath == null) return null;
        String category = extractCategory(displayPath);
        if (category == null) return displayPath;
        if (displayPath.length() == category.length()) return "";
        return displayPath.substring(category.length() + 1); // +1 for the "/"
    }

    /**
     * Parse all {@code folder:} components from a QualifiedPath string
     * and join them into a slash-separated prefix.
     * <p>
     * {@code "histprov:xxx:/folder:Ignition:/folder:default"}
     * → {@code "Ignition/default/"}
     * <p>
     * Returns empty string if no folder components are found.
     */
    static String parseFolderPrefix(String qualifiedPathStr) {
        if (qualifiedPathStr == null || qualifiedPathStr.isEmpty()) {
            return "";
        }

        StringBuilder prefix = new StringBuilder();
        int idx = 0;
        while (true) {
            idx = qualifiedPathStr.indexOf("folder:", idx);
            if (idx < 0) break;
            // Ensure we're at a component boundary (start of string or after "/")
            if (idx > 0 && qualifiedPathStr.charAt(idx - 1) != '/') {
                idx += 7;
                continue;
            }
            int start = idx + 7; // "folder:".length()
            int end = qualifiedPathStr.indexOf(":/", start);
            String folderName = end >= 0
                    ? qualifiedPathStr.substring(start, end)
                    : qualifiedPathStr.substring(start);
            if (prefix.length() > 0) prefix.append("/");
            prefix.append(folderName);
            idx = start;
        }

        if (prefix.length() > 0) {
            prefix.append("/");
        }
        return prefix.toString();
    }
}
