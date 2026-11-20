package io.factry.historian.gateway;

import com.inductiveautomation.historian.gateway.api.config.HistorianSettings;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.annotations.NonSecret;
import com.inductiveautomation.ignition.gateway.secrets.SecretConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactryHistorianSettings implements HistorianSettings {
    private static final Logger logger = LoggerFactory.getLogger(FactryHistorianSettings.class);

    private String collectorUUID = "";
    private String collectorName = "";
    private static final String DEFAULT_GRPC_HOST = "localhost";
    private static final int DEFAULT_GRPC_PORT = 9876;
    private String grpcHost = DEFAULT_GRPC_HOST;
    private int grpcPort = DEFAULT_GRPC_PORT;
    // grpcHost and grpcPort as stored, kept at the first applyTokenDefaults: that one replaces the
    // defaults with the token's values, and a later token (from a secret provider) must start from
    // the stored values again.
    private transient String storedGrpcHost;
    private transient Integer storedGrpcPort;
    private boolean debugLogging = false;
    private boolean useTls = false;
    private boolean skipTlsVerification = false;
    // The collector token: encrypted with the gateway's key (Embedded) or read from a secret
    // provider (Referenced).
    private SecretConfig collectorToken;
    // The token in plain text, as profiles saved before collectorToken existed hold it. Only read
    // while collectorToken is not set; FactryHistorianExtensionPoint.encode leaves it out then, so
    // the first save with collectorToken removes the plain copy from config.json.
    // @NonSecret: deliberately a plain string, so Ignition does not warn at every start that a
    // field named like a secret is not a SecretConfig.
    @NonSecret
    private String token = "";
    // Optional PEM-encoded CA certificate(s) to trust in addition to the system and bundled
    // Factry CAs — for Historians fronted by an internal/enterprise CA or a self-signed cert.
    private String customCaCert = "";
    // Tag-path delimiter for tree-view splitting in the browser/Power Chart.
    // Default is empty (flat list). An Ignition 8.3 bug prevents defaultSettings()
    // from pre-filling the create form, so a blank field must mean what it shows —
    // flat — rather than silently falling back to "/" (tree). Set "/" for a tree.
    private String delimiter = "";

    public FactryHistorianSettings() {
    }

    public String getCollectorUUID() {
        return collectorUUID;
    }

    public void setCollectorUUID(String collectorUUID) {
        this.collectorUUID = collectorUUID;
    }

    public String getCollectorName() {
        return collectorName;
    }

    public void setCollectorName(String collectorName) {
        this.collectorName = collectorName;
    }

    public String getGrpcHost() {
        return grpcHost;
    }

    public void setGrpcHost(String grpcHost) {
        this.grpcHost = grpcHost;
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public boolean isDebugLogging() {
        return debugLogging;
    }

    public void setDebugLogging(boolean debugLogging) {
        this.debugLogging = debugLogging;
    }

    public boolean isUseTls() {
        return useTls;
    }

    public void setUseTls(boolean useTls) {
        this.useTls = useTls;
    }

    public boolean isSkipTlsVerification() {
        return skipTlsVerification;
    }

    public void setSkipTlsVerification(boolean skipTlsVerification) {
        this.skipTlsVerification = skipTlsVerification;
    }

    public SecretConfig getCollectorToken() {
        return collectorToken;
    }

    public void setCollectorToken(SecretConfig collectorToken) {
        this.collectorToken = collectorToken;
    }

    /** The token in plain text from a profile saved before {@link #getCollectorToken()} existed. */
    public String getLegacyToken() {
        return token;
    }

    public void setLegacyToken(String token) {
        this.token = token;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getCustomCaCert() {
        return customCaCert;
    }

    public void setCustomCaCert(String customCaCert) {
        this.customCaCert = customCaCert;
    }

    /**
     * Extract collectorUUID, grpcHost, and grpcPort from the JWT token payload.
     * These fields are no longer exposed in the UI — the token is the single
     * source of truth for connection settings.
     *
     * @param resolvedToken the plaintext JWT string, as pasted
     * @return the token to send to Factry, see {@link JwtTokenParser#normalize(String)}
     * @throws IllegalArgumentException if the token is not a valid Factry collector token
     */
    public String applyTokenDefaults(String resolvedToken) {
        Claims claims = parseClaims(resolvedToken);
        this.collectorUUID = claims.collectorUUID();
        this.collectorName = claims.collectorName();

        if (storedGrpcHost == null) {
            storedGrpcHost = grpcHost;
            storedGrpcPort = grpcPort;
        }
        // Only apply host/port from token if config still has defaults.
        if (DEFAULT_GRPC_HOST.equals(storedGrpcHost)) {
            this.grpcHost = claims.host();
        } else {
            this.grpcHost = storedGrpcHost;
            logger.info("Config grpcHost '{}' overrides token host '{}'", storedGrpcHost, claims.host());
        }
        if (DEFAULT_GRPC_PORT == storedGrpcPort) {
            this.grpcPort = claims.port();
        } else {
            this.grpcPort = storedGrpcPort;
            logger.info("Config grpcPort '{}' overrides token port '{}'", storedGrpcPort, claims.port());
        }

        logger.info("Extracted from token: collectorName={}, collectorUUID={}, host={}, port={}",
                collectorName, collectorUUID, grpcHost, grpcPort);
        return claims.token();
    }

    /**
     * Whether {@code token} is a Factry collector token this module can use, without logging or
     * changing anything: for the check when a profile is saved.
     *
     * @throws IllegalArgumentException with what is wrong; the message leaves the token out
     */
    static void checkToken(String token) {
        parseClaims(token);
    }

    private record Claims(String token, String collectorUUID, String collectorName, String host, int port) {}

    private static Claims parseClaims(String resolvedToken) {
        String token = JwtTokenParser.normalize(resolvedToken);
        var payload = JwtTokenParser.parsePayload(token)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid token: not a valid JWT. Paste the collector token generated by Factry Historian."));
        String uuid = JwtTokenParser.getCollectorUUID(payload)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid token: missing collector UUID. Ensure this is a Factry collector token."));
        String name = JwtTokenParser.getCollectorName(payload)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid token: missing collector name. Ensure this is a Factry collector token."));
        String host = JwtTokenParser.getHost(payload)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid token: missing host (aud claim). Ensure this is a Factry collector token."));
        int port = JwtTokenParser.getGrpcPort(payload)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid token: missing or invalid gRPC port. Ensure this is a Factry collector token."));
        return new Claims(token, uuid, name, host, port);
    }

    /**
     * Validate settings and throw IllegalArgumentException if invalid.
     * Called after applyTokenDefaults, before creating or reconfiguring the gRPC client.
     */
    public void validate() {
        if (collectorUUID == null || collectorUUID.trim().isEmpty()) {
            throw new IllegalArgumentException("Collector ID is required (should be extracted from token)");
        }
        if (collectorName == null || collectorName.trim().isEmpty()) {
            throw new IllegalArgumentException("Collector name is required (should be extracted from token)");
        }
        if (grpcHost == null || grpcHost.trim().isEmpty()) {
            throw new IllegalArgumentException("Host is required (should be extracted from token)");
        }
        if (grpcPort < 1 || grpcPort > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        // Surface a bad custom CA certificate at save time rather than as an opaque TLS
        // handshake failure later. Throws IllegalArgumentException with a clear message.
        FactryGrpcClient.parseCaCertificates(customCaCert);
    }

    @Override
    public String toString() {
        return "FactryHistorianSettings{" +
                "collectorName='" + collectorName + '\'' +
                ", collectorUUID='" + collectorUUID + '\'' +
                ", collectorToken=" + (collectorToken == null ? (token == null || token.isBlank() ? "none" : "plain text")
                        : collectorToken.getType()) +
                ", grpcHost='" + grpcHost + '\'' +
                ", grpcPort=" + grpcPort +
                ", debugLogging=" + debugLogging +
                ", useTls=" + useTls +
                ", skipTlsVerification=" + skipTlsVerification +
                ", delimiter='" + delimiter + '\'' +
                ", customCaCert='" + (customCaCert == null || customCaCert.isBlank() ? "" : "<set>") + '\'' +
                '}';
    }
}
