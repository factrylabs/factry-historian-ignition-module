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
    // Where the gateway reaches Factry when that is not what the token says (Docker, NAT, a DMZ
    // name). Blank or 0: the token's host and grpc-port.
    private String hostOverride = "";
    private Integer portOverride;
    // Host and port as profiles saved before the overrides existed hold them, then hidden. Moved
    // to the overrides when the profile is decoded. "localhost" and 9876 were the class defaults,
    // written with every save, so they mean "from the token".
    private static final String LEGACY_DEFAULT_HOST = "localhost";
    private static final int LEGACY_DEFAULT_PORT = 9876;
    private String grpcHost;
    private Integer grpcPort;
    // Resolved from the token and the overrides by applyTokenDefaults; never stored, so a resolved
    // value cannot turn into an override.
    private transient String host = "";
    private transient int port;
    private transient boolean factryCertificate;
    // Off only for a Factry whose gRPC TLS is off (USE_SELF_SIGNED_CERTIFICATE=false) with nothing in
    // between that ends TLS. Profiles saved before the switch came back hold it as they had it.
    private boolean useTls = true;
    // Resolved by applyTokenDefaults: TLS when Use TLS is on; also when the token says Factry serves
    // its built-in certificate, which Factry's own collectors then always use TLS for, and when Skip
    // TLS Verification or a Custom CA is set, which only make sense with TLS.
    private transient boolean tls = true;
    private boolean debugLogging = false;
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
    // Optional PEM-encoded CA certificate(s) to trust in addition to the system CAs, for a
    // Historian (or a proxy in front of it) with a certificate of its own.
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

    public String getHostOverride() {
        return hostOverride;
    }

    public void setHostOverride(String hostOverride) {
        this.hostOverride = hostOverride;
    }

    public Integer getPortOverride() {
        // 0, not empty, is "not set" in the form; a profile saved before the overrides holds null.
        return isSet(portOverride) ? portOverride : 0;
    }

    public void setPortOverride(Integer portOverride) {
        this.portOverride = portOverride;
    }

    /** The host the client connects to; set by {@link #applyTokenDefaults(String)}. */
    public String getHost() {
        return host;
    }

    /** The port the client connects to; set by {@link #applyTokenDefaults(String)}. */
    public int getPort() {
        return port;
    }

    /** The token says Factry serves its built-in certificate; set by {@link #applyTokenDefaults(String)}. */
    public boolean usesFactryCertificate() {
        return factryCertificate;
    }

    /**
     * Moves the host and port of a profile saved before the overrides existed into the overrides,
     * so they show in the form and the next save stores them under the new names.
     */
    void moveLegacyConnectionSettings() {
        if (isBlank(hostOverride) && !isBlank(grpcHost) && !LEGACY_DEFAULT_HOST.equals(grpcHost)) {
            hostOverride = grpcHost;
        }
        if (!isSet(portOverride) && grpcPort != null && grpcPort != LEGACY_DEFAULT_PORT && grpcPort != 0) {
            portOverride = grpcPort;
        }
        grpcHost = null;
        grpcPort = null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static boolean isSet(Integer port) {
        return port != null && port != 0;
    }

    public boolean isUseTls() {
        return useTls;
    }

    public void setUseTls(boolean useTls) {
        this.useTls = useTls;
    }

    /** Whether the client uses TLS; set by {@link #applyTokenDefaults(String)}. */
    public boolean usesTls() {
        return tls;
    }

    public boolean isDebugLogging() {
        return debugLogging;
    }

    public void setDebugLogging(boolean debugLogging) {
        this.debugLogging = debugLogging;
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
     * Extract the collector, host, port and certificate from the JWT token payload. Host Override
     * and Port Override, when set, win over the token's host and port.
     *
     * @param resolvedToken the plaintext JWT string, as pasted
     * @return the token to send to Factry, see {@link JwtTokenParser#normalize(String)}
     * @throws IllegalArgumentException if the token is not a valid Factry collector token
     */
    public String applyTokenDefaults(String resolvedToken) {
        Claims claims = parseClaims(resolvedToken);
        this.collectorUUID = claims.collectorUUID();
        this.collectorName = claims.collectorName();

        this.factryCertificate = claims.factryCertificate();
        this.tls = useTls || factryCertificate || skipTlsVerification || !isBlank(customCaCert);
        boolean hostOverridden = !isBlank(hostOverride);
        boolean portOverridden = isSet(portOverride);
        this.host = hostOverridden ? hostOverride.trim() : claims.host();
        this.port = portOverridden ? portOverride : claims.port();

        logger.info("Collector {} ({}): connecting to {}:{} (host {}, port {}), {}", collectorName, collectorUUID,
                host, port,
                hostOverridden ? "from Host Override, the token says " + claims.host() : "from the token",
                portOverridden ? "from Port Override, the token says " + claims.port() : "from the token",
                FactryGrpcClient.tlsLabel(tls, skipTlsVerification, customCaCert, factryCertificate));
        if (!useTls && tls) {
            logger.info("Collector {}: Use TLS is off, but {}. Connecting with TLS.", collectorName,
                    factryCertificate ? "the token says Factry serves its built-in certificate, which always uses TLS"
                            : "Skip TLS Verification or a Custom CA Certificate is set, which need TLS");
        } else if (!tls) {
            logger.warn("Collector {}: Use TLS is off, so the collector token is sent unencrypted to {}:{} and to "
                    + "Factry's REST port. Turn it on unless Factry's gRPC TLS is off and nothing in between can "
                    + "read the traffic.", collectorName, host, port);
        }
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

    private record Claims(String token, String collectorUUID, String collectorName, String host, int port,
                          boolean factryCertificate) {}

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
        return new Claims(token, uuid, name, host, port, JwtTokenParser.usesFactryCertificate(payload));
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
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("Host is required (should be extracted from token)");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        // Surface a bad custom CA certificate at save time rather than as an opaque TLS
        // handshake failure later. Throws IllegalArgumentException with a clear message.
        FactryGrpcClient.parseCaCertificates(customCaCert);
    }

    /**
     * Whether {@code other} holds the same settings: the same form (every field the operator
     * edits, compared as {@link FactryHistorianConfig}, which new form fields join) and the same
     * values taken from the token. SettingsSameAsTest checks that every field counts.
     */
    boolean sameAs(FactryHistorianSettings other) {
        // The form compares the token secret (SecretConfig equals), the overrides (a port of 0 or
        // none is "not set") and the TLS settings. The rest: from the token, or legacy fields of
        // a profile saved before the form had them (moved or ignored on decode, so normally empty).
        return other != null
                && FactryHistorianConfig.fromSettings(this).equals(FactryHistorianConfig.fromSettings(other))
                && java.util.Objects.equals(collectorUUID, other.collectorUUID)
                && java.util.Objects.equals(collectorName, other.collectorName)
                && java.util.Objects.equals(token, other.token)
                && java.util.Objects.equals(grpcHost, other.grpcHost)
                && java.util.Objects.equals(grpcPort, other.grpcPort);
    }

    @Override
    public String toString() {
        return "FactryHistorianSettings{" +
                "collectorName='" + collectorName + '\'' +
                ", collectorUUID='" + collectorUUID + '\'' +
                ", collectorToken=" + (collectorToken == null ? (token == null || token.isBlank() ? "none" : "plain text")
                        : collectorToken.getType()) +
                ", hostOverride='" + hostOverride + '\'' +
                ", portOverride=" + portOverride +
                ", useTls=" + useTls +
                ", debugLogging=" + debugLogging +
                ", skipTlsVerification=" + skipTlsVerification +
                ", delimiter='" + delimiter + '\'' +
                ", customCaCert='" + (customCaCert == null || customCaCert.isBlank() ? "" : "<set>") + '\'' +
                '}';
    }
}
