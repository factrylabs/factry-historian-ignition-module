package io.factry.historian.gateway;

import io.factry.historian.proto.AssetProperties;
import io.factry.historian.proto.Assets;
import io.factry.historian.proto.Collector;
import io.factry.historian.proto.Collectors;
import io.factry.historian.proto.CreateMeasurementsReply;
import io.factry.historian.proto.CreateMeasurementsRequest;
import io.factry.historian.proto.CreatePointsReply;
import io.factry.historian.proto.GetAssetPropertiesRequest;
import io.factry.historian.proto.GetCollectorsRequest;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.GetTimeseriesDatabasesRequest;
import io.factry.historian.proto.GetAssetsRequest;
import io.factry.historian.proto.HealthUpdate;
import io.factry.historian.proto.HealthUpdateReply;
import io.factry.historian.proto.HealthUpdates;
import com.google.protobuf.Empty;
import com.google.protobuf.Struct;
import io.factry.historian.proto.HistorianGrpc;
import io.factry.historian.proto.MeasurementRequest;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.Points;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.factry.historian.proto.RegisterCollectorSchema;
import io.factry.historian.proto.TimeseriesDatabases;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FactryGrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(FactryGrpcClient.class);
    private static final Metadata.Key<String> COLLECTOR_UUID_KEY =
            Metadata.Key.of("collectoruuid", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final long WRITE_DEADLINE_SECONDS = ModuleProperties.getWriteDeadlineSeconds();
    private static final long WRITE_DEADLINE_MILLIS_PER_POINT = ModuleProperties.getWriteDeadlineMillisPerPoint();
    private static final long WRITE_DEADLINE_MAX_SECONDS = ModuleProperties.getWriteDeadlineMaxSeconds();
    private static final long WRITE_MIN_BYTES_PER_SECOND = ModuleProperties.getWriteMinBytesPerSecond();
    private static final boolean WRITE_GZIP = ModuleProperties.isWriteGzip();
    private static final long READ_DEADLINE_SECONDS = ModuleProperties.getReadDeadlineSeconds();
    private static final long CALL_DEADLINE_SECONDS = ModuleProperties.getCallDeadlineSeconds();
    private static final long SHUTDOWN_GRACE_SECONDS = ModuleProperties.getShutdownGraceSeconds();
    /**
     * Factry's own root CA ("FACTRY BVBA CA", valid to 2041), which signs the certificate every
     * Factry Historian serves when it uses its built-in one. That certificate and its private key
     * are inside every Factry server binary, so checking it shows the server is a Factry with the
     * built-in certificate, not that it is yours. It still stops a man in the middle that offers
     * any other certificate, which skipping the check does not.
     */
    private static final String FACTRY_CA_RESOURCE = "factry-root-ca.crt";
    /** The only name in Factry's built-in certificate (besides historian.factry.dev). */
    static final String FACTRY_CERTIFICATE_NAME = "historian.factry.local";
    private static final String FRAME_LOGGER = "io.grpc.netty.shaded.io.grpc.netty.NettyClientHandler";

    /**
     * Largest answer the client accepts. grpc-java defaults to 4 MiB, which a day of 1-second data
     * for a few pens already passes. Factry Historian sends up to its GRPC_MAX_MESSAGE_SIZE, 64 MiB
     * by default, so accept the same.
     */
    static final int MAX_INBOUND_MESSAGE_BYTES = 64 * 1024 * 1024;

    /**
     * Guards channel/stub access during reconfiguration.
     * gRPC calls take the read lock (concurrent), reconfigure takes the write lock (exclusive).
     */
    private final ReadWriteLock channelLock = new ReentrantReadWriteLock();

    private volatile ManagedChannel channel;
    private volatile HistorianGrpc.HistorianBlockingStub blockingStub;
    private volatile boolean connected = true;
    private final AtomicBoolean writeGzip = new AtomicBoolean(WRITE_GZIP);
    private volatile String collectorUUID;
    /** The CA of Factry's built-in certificate; another one in tests. */
    private final List<X509Certificate> factryCa;

    /** Plain text ({@code useTls} false) only for a Factry whose gRPC TLS is turned off. */
    public FactryGrpcClient(String host, int port, String collectorUUID, String token,
                            boolean useTls, boolean skipTlsVerification, String customCaCert) {
        this(host, port, collectorUUID, token, useTls, skipTlsVerification, customCaCert, false);
    }

    /**
     * @param factryCertificate the token says Factry uses its built-in certificate
     *                          ({@code use-self-signed-certificate}); then, without a custom CA,
     *                          the certificate passes the normal check (system CAs, the host) or
     *                          Factry's CA with the name {@value #FACTRY_CERTIFICATE_NAME}, see
     *                          {@link FactryOrSystemTrustManager}
     */
    public FactryGrpcClient(String host, int port, String collectorUUID, String token,
                            boolean useTls, boolean skipTlsVerification, String customCaCert,
                            boolean factryCertificate) {
        this(host, port, collectorUUID, token, useTls, skipTlsVerification, customCaCert, factryCertificate,
                bundledFactryCa());
    }

    FactryGrpcClient(String host, int port, String collectorUUID, String token,
                     boolean useTls, boolean skipTlsVerification, String customCaCert,
                     boolean factryCertificate, List<X509Certificate> factryCa) {
        this.factryCa = factryCa;
        keepTokenOutOfFrameLog();
        buildChannel(host, port, collectorUUID, token, useTls, skipTlsVerification, customCaCert, factryCertificate);
        logger.info("gRPC client created ({}), target={}:{}, collectorUUID={}",
                tlsLabel(useTls, skipTlsVerification, customCaCert, factryCertificate), host, port, collectorUUID);
    }

    static List<X509Certificate> bundledFactryCa() {
        try (InputStream in = FactryGrpcClient.class.getClassLoader().getResourceAsStream(FACTRY_CA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Factry CA certificate '" + FACTRY_CA_RESOURCE + "' missing from the module");
            }
            return parseCaCertificates(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Cannot read Factry CA certificate '" + FACTRY_CA_RESOURCE + "'", e);
        }
    }

    /**
     * Netty's HTTP/2 frame logger writes every request header at DEBUG, the collector token
     * included. Giving it its own INFO level stops a broad io.grpc or root DEBUG from reaching it.
     * A level an admin set on this exact logger is kept: then the frames are wanted.
     */
    static void keepTokenOutOfFrameLog() {
        if (LoggerFactory.getLogger(FRAME_LOGGER) instanceof ch.qos.logback.classic.Logger frameLogger
                && frameLogger.getLevel() == null) {
            frameLogger.setLevel(ch.qos.logback.classic.Level.INFO);
        }
    }

    public CreatePointsReply createPoints(Points points) {
        logger.debug("Sending CreatePoints with {} points", points.getPointsCount());
        channelLock.readLock().lock();
        try {
            CreatePointsReply reply;
            boolean gzip = writeGzip.get();
            try {
                reply = sendPoints(points, gzip);
            } catch (StatusRuntimeException e) {
                if (!gzip || !isGzipRejected(e)) {
                    throw e;
                }
                // Without this the batch would fail the same way on every attempt.
                if (writeGzip.compareAndSet(true, false)) {
                    logger.warn("Factry does not accept gzip-compressed writes ({}), sending them uncompressed from now on",
                            e.getStatus().getDescription());
                }
                reply = sendPoints(points, false);
            }
            connected = true;
            return reply;
        } catch (StatusRuntimeException e) {
            Status.Code code = e.getStatus().getCode();
            if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
                connected = false;
            }
            throw e;
        } finally {
            channelLock.readLock().unlock();
        }
    }

    private CreatePointsReply sendPoints(Points points, boolean gzip) {
        HistorianGrpc.HistorianBlockingStub stub = blockingStub
                .withDeadlineAfter(writeDeadlineMillis(points.getPointsCount(), points.getSerializedSize()),
                        TimeUnit.MILLISECONDS);
        if (gzip) {
            stub = stub.withCompression("gzip");
        }
        return stub.createPoints(points);
    }

    /**
     * A server without a gzip decompressor answers UNIMPLEMENTED: grpc-go with "grpc:
     * Decompressor is not installed for grpc-encoding \"gzip\"", grpc-java with "Can't find
     * decompressor for gzip". An RPC the server does not know is also UNIMPLEMENTED ("unknown
     * method", "Method not found"), so the message tells the two apart.
     */
    static boolean isGzipRejected(StatusRuntimeException e) {
        String description = e.getStatus().getDescription();
        return e.getStatus().getCode() == Status.Code.UNIMPLEMENTED
                && description != null
                && description.toLowerCase(Locale.ROOT).contains("decompressor");
    }

    /**
     * A fixed deadline that fits one point is too short for a full store and forward batch on a
     * slow link: the same batch would time out on every retry and the backlog would never
     * shrink. So the deadline grows with the batch (points, or measurements to create) and with
     * its size in bytes, at an assumed minimum link speed (a few big strings are few points but
     * many bytes), up to a ceiling.
     */
    private static long writeDeadlineMillis(int itemCount) {
        return writeDeadlineMillis(itemCount, 0);
    }

    private static long writeDeadlineMillis(int itemCount, long bytes) {
        long millis = WRITE_DEADLINE_SECONDS * 1000 + itemCount * WRITE_DEADLINE_MILLIS_PER_POINT
                + bytes * 1000 / WRITE_MIN_BYTES_PER_SECOND;
        return Math.min(millis, WRITE_DEADLINE_MAX_SECONDS * 1000);
    }

    public Measurements getMeasurements(MeasurementRequest request) {
        logger.debug("Sending GetMeasurements");
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .getMeasurements(request);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    /**
     * Fetch all measurements across all collectors using the filter endpoint.
     * Unlike {@link #getMeasurements}, this is not scoped to the current collector.
     */
    public Measurements getMeasurementsByFilter(GetMeasurementsByFilterRequest request) {
        logger.debug("Sending GetMeasurementsByFilter");
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(READ_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .getMeasurementsByFilter(request);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    /**
     * Same call as {@link #getMeasurementsByFilter}, bounded by the write deadline. For the small
     * single-name lookups on the store-and-forward path, so a Factry that accepts the connection
     * but never answers cannot block the write. Not for the full cache refresh, which pages
     * through every measurement and can take longer.
     */
    public Measurements getMeasurementsByFilterWithDeadline(GetMeasurementsByFilterRequest request) {
        logger.debug("Sending GetMeasurementsByFilter with deadline");
        channelLock.readLock().lock();
        try {
            Measurements reply = blockingStub
                    .withDeadlineAfter(WRITE_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .getMeasurementsByFilter(request);
            connected = true;
            return reply;
        } catch (StatusRuntimeException e) {
            Status.Code code = e.getStatus().getCode();
            if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
                connected = false;
            }
            throw e;
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public QueryTimeseriesResponse queryTimeseries(QueryTimeseriesRequest request) {
        logger.debug("Sending QueryTimeseries for {} measurements", request.getMeasurementUUIDsCount());
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(READ_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .queryTimeseries(request);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public Assets getAssets() {
        logger.debug("Sending GetAssets");
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(READ_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .getAssets(GetAssetsRequest.newBuilder().build());
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public AssetProperties getAssetProperties(GetAssetPropertiesRequest request) {
        logger.debug("Sending GetAssetProperties for {} assets", request.getAssetUUIDsCount());
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(READ_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .getAssetProperties(request);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public Collectors getCollectors() {
        logger.debug("Sending GetCollectors");
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .getCollectors(GetCollectorsRequest.newBuilder().build());
        } finally {
            channelLock.readLock().unlock();
        }
    }

    /**
     * The UUID of this collector's default time-series database, from Factry's REST API (gRPC does
     * not give it); null when Factry answers without one. Throws when Factry could not be asked
     * (no REST port in the token, not reachable within {@link FactryRestClient#TIMEOUT}, refused).
     */
    public String getDefaultDatabaseUUID() {
        RestTarget target = restTarget;
        if (target == null) {
            throw new IllegalStateException("no REST API settings");
        }
        try {
            FactryRestClient rest = target.client();
            if (rest == null) {
                throw new IllegalStateException("the collector token names no REST port");
            }
            return rest.defaultDatabaseUUID();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e.toString(), e);
        }
    }

    /** Where {@link #getDefaultDatabaseUUID} asks, and its client, built once per channel settings. */
    private static final class RestTarget {
        private final String host;
        private final String token;
        private final boolean useTls;
        /** The gRPC channel's own trust decision, so REST checks certificates exactly as gRPC does. */
        private final X509TrustManager trust;
        private FactryRestClient client;
        private boolean built;

        RestTarget(String host, String token, boolean useTls, X509TrustManager trust) {
            this.host = host;
            this.token = token;
            this.useTls = useTls;
            this.trust = trust;
        }

        synchronized FactryRestClient client() throws Exception {
            if (!built) {
                client = FactryRestClient.forToken(host, token, useTls, trust);
                built = true;
            }
            return client;
        }
    }

    private volatile RestTarget restTarget;

    public TimeseriesDatabases getTimeseriesDatabases() {
        logger.debug("Sending GetTimeseriesDatabases");
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .getTimeseriesDatabases(GetTimeseriesDatabasesRequest.getDefaultInstance());
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public Collector registerCollector(RegisterCollectorSchema schema) {
        logger.debug("Sending RegisterCollector");
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .registerCollector(schema);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public HealthUpdateReply updateHealth(HealthUpdates updates) {
        logger.debug("Sending UpdateHealth");
        channelLock.readLock().lock();
        try {
            return blockingStub
                    .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .updateHealth(updates);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public void updateCollectorState(Struct state) {
        logger.debug("Sending UpdateCollectorState");
        channelLock.readLock().lock();
        try {
            blockingStub
                    .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .updateCollectorState(state);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public CreateMeasurementsReply createMeasurements(CreateMeasurementsRequest request) {
        logger.debug("Sending CreateMeasurements with {} measurements", request.getMeasurementsCount());
        channelLock.readLock().lock();
        try {
            // A create that times out may still be carried out by Factry, and creating the same
            // names again adds Discovered duplicates. So it gets as long as a write of as many
            // points, which lets thousands of new tags in one flush through on a slow link.
            CreateMeasurementsReply reply = blockingStub
                    .withDeadlineAfter(writeDeadlineMillis(request.getMeasurementsCount()), TimeUnit.MILLISECONDS)
                    .createMeasurements(request);
            connected = true;
            return reply;
        } catch (StatusRuntimeException e) {
            Status.Code code = e.getStatus().getCode();
            if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
                connected = false;
            }
            throw e;
        } finally {
            channelLock.readLock().unlock();
        }
    }

    /**
     * Reconfigure the client with new connection settings. Shuts down the old
     * gRPC channel and creates a new one.
     */
    public void reconfigure(String host, int port, String collectorUUID, String token,
                            boolean useTls, boolean skipTlsVerification, String customCaCert) {
        reconfigure(host, port, collectorUUID, token, useTls, skipTlsVerification, customCaCert, false);
    }

    /** See {@link #FactryGrpcClient(String, int, String, String, boolean, boolean, String, boolean)}. */
    public void reconfigure(String host, int port, String collectorUUID, String token,
                            boolean useTls, boolean skipTlsVerification, String customCaCert,
                            boolean factryCertificate) {
        logger.info("Reconfiguring gRPC client: {}:{}", host, port);
        lockChannel();
        try {
            shutdownChannel();
            buildChannel(host, port, collectorUUID, token, useTls, skipTlsVerification, customCaCert,
                    factryCertificate);
            // New settings may point at another Factry, or the same one after an upgrade, so
            // gzip is tried again instead of staying off for the life of the client.
            writeGzip.set(WRITE_GZIP);
            logger.info("gRPC client reconfigured ({}), target={}:{}, collectorUUID={}",
                    tlsLabel(useTls, skipTlsVerification, customCaCert, factryCertificate), host, port, collectorUUID);
        } finally {
            channelLock.writeLock().unlock();
        }
    }

    // No gRPC keepalive pings. Factry's server keeps the grpc-go defaults: a ping while no call is
    // running counts as abuse, and after three it closes the connection with "too_many_pings"
    // (seen on v8.2.3 with pings every 30 s and every 5 minutes). Deadlines on every call and
    // enterIdle() in testConnection() deal with connections the network dropped.
    private void buildChannel(String host, int port, String collectorUUID, String token,
                              boolean useTls, boolean skipTlsVerification, String customCaCert,
                              boolean factryCertificate) {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port)
                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES);
        X509TrustManager trust = null;
        if (!useTls) {
            builder.usePlaintext();
        } else {
            try {
                trust = trustManager(skipTlsVerification, customCaCert, factryCertificate, factryCa);
                builder.sslContext(GrpcSslContexts.forClient().trustManager(trust).build());
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create TLS-enabled gRPC channel", e);
            }
        }
        this.channel = builder.build();

        this.collectorUUID = collectorUUID;
        this.restTarget = new RestTarget(host, token, useTls, trust);
        Metadata headers = new Metadata();
        headers.put(COLLECTOR_UUID_KEY, collectorUUID);
        headers.put(AUTHORIZATION_KEY, "Bearer " + token);

        this.blockingStub = HistorianGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers), FRAME_LOG_GUARD);
    }

    /**
     * The one trust decision for a TLS connection to Factry, used by the gRPC channel and by the
     * REST call ({@link FactryRestClient}), so both check certificates the same way: none with Skip
     * TLS Verification; the system CAs plus a custom CA against the host; with the token's
     * built-in-certificate claim, {@link FactryOrSystemTrustManager}; else the system CAs against
     * the host. Always an {@link javax.net.ssl.X509ExtendedTrustManager} except for Skip, so the TLS
     * engine (Netty's, or the JDK's under HttpClient) leaves the host name check to it.
     */
    static X509TrustManager trustManager(boolean skipTlsVerification, String customCaCert,
                                         boolean factryCertificate, List<X509Certificate> factryCa) throws Exception {
        if (skipTlsVerification) {
            return (X509TrustManager) InsecureTrustManagerFactory.INSTANCE.getTrustManagers()[0];
        }
        if (hasCustomCa(customCaCert) || !factryCertificate) {
            // A custom CA means Factry, or a proxy in front of it, has a certificate of its own,
            // so it is checked against the host the client connects to.
            for (TrustManager tm : buildCombinedTrustManagerFactory(null, customCaCert).getTrustManagers()) {
                if (tm instanceof X509TrustManager x509) {
                    return x509;
                }
            }
            throw new IllegalStateException("no X509TrustManager");
        }
        return new FactryOrSystemTrustManager(factryCa);
    }

    /**
     * Checks the frame logger before every call: a logback reload (a scan, or levels reset on the
     * Logs page) clears its level, and the next call would log the token again.
     */
    private static final ClientInterceptor FRAME_LOG_GUARD = new ClientInterceptor() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            keepTokenOutOfFrameLog();
            return next.newCall(method, callOptions);
        }
    };

    /**
     * Build a TrustManagerFactory that trusts the JVM's default (system/public) CAs, the given
     * extra CA certificate(s), and any user-supplied custom CA certificate(s) in PEM form.
     * This lets the module verify servers using a publicly-signed certificate (e.g. Let's
     * Encrypt), or one issued by a customer's internal/enterprise CA (or a self-signed
     * certificate pasted directly), without having to turn off verification.
     *
     * <p>Trust is purely additive: the custom CA is added alongside, never in place of, the
     * system CAs. If {@code extraCaCerts} is null only the system CAs and any custom
     * CA are trusted; if {@code customCaCertPem} is null/blank no custom CA is added.
     *
     * @throws IllegalArgumentException if {@code customCaCertPem} is non-blank but not valid PEM
     */
    static TrustManagerFactory buildCombinedTrustManagerFactory(InputStream extraCaCerts, String customCaCertPem)
            throws Exception {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null); // start with an empty keystore
        int idx = 0;

        // 1. System/public CAs — take the accepted issuers from the default trust manager.
        TrustManagerFactory systemTmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        systemTmf.init((KeyStore) null); // null → JVM default trust store
        for (TrustManager tm : systemTmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                for (X509Certificate cert : ((X509TrustManager) tm).getAcceptedIssuers()) {
                    ks.setCertificateEntry("system-ca-" + (idx++), cert);
                }
            }
        }

        // 2. Extra CA certificate(s).
        if (extraCaCerts != null) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            for (Certificate cert : cf.generateCertificates(extraCaCerts)) {
                ks.setCertificateEntry("extra-ca-" + (idx++), cert);
            }
        }

        // 3. User-supplied custom CA certificate(s) (internal/enterprise CA or self-signed).
        for (X509Certificate cert : parseCaCertificates(customCaCertPem)) {
            ks.setCertificateEntry("user-ca-" + (idx++), cert);
        }

        TrustManagerFactory combined =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        combined.init(ks);
        return combined;
    }

    /**
     * Parse PEM-encoded X.509 certificate(s) from the given text. Returns an empty list for
     * null/blank input; otherwise the text must contain at least one valid certificate.
     *
     * @throws IllegalArgumentException if the text is non-blank but does not parse to at least
     *                                  one X.509 certificate
     */
    static List<X509Certificate> parseCaCertificates(String pem) {
        List<X509Certificate> result = new ArrayList<>();
        if (pem == null || pem.isBlank()) {
            return result;
        }
        try (InputStream in = new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8))) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            for (Certificate cert : cf.generateCertificates(in)) {
                result.add((X509Certificate) cert);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Custom CA certificate is not valid PEM: " + e.getMessage(), e);
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException(
                    "Custom CA certificate is set but contains no certificates. Paste a PEM-encoded "
                    + "certificate beginning with '-----BEGIN CERTIFICATE-----'.");
        }
        return result;
    }

    private static boolean hasCustomCa(String customCaCert) {
        return customCaCert != null && !customCaCert.isBlank();
    }

    /** Which certificate check the client makes, for the log. */
    static String tlsLabel(boolean useTls, boolean skipTlsVerification, String customCaCert,
                           boolean factryCertificate) {
        if (!useTls) return "plaintext, the collector token is sent unencrypted";
        if (skipTlsVerification) return "TLS, certificate not checked";
        if (hasCustomCa(customCaCert)) return "TLS, certificate checked against the system CAs and the custom CA";
        if (factryCertificate) return "TLS, certificate checked against the system CAs, or as Factry's built-in"
                + " certificate (the same on every Factry) against Factry's CA as " + FACTRY_CERTIFICATE_NAME;
        return "TLS, certificate checked against the system CAs";
    }

    /** {@link #tlsHint(Status, boolean, boolean)} for a TLS client without the token's certificate claim. */
    static String tlsHint(Status status) {
        return tlsHint(status, true, false);
    }

    /**
     * What to change when the connection check fails on TLS, from the cause gRPC reports, or empty.
     * Ends without a space; starts with one when not empty.
     *
     * @param tls               whether the client used TLS
     * @param factryCertificate the token says Factry serves its built-in certificate, which makes
     *                          the client use TLS whatever Use TLS says
     */
    static String tlsHint(Status status, boolean tls, boolean factryCertificate) {
        String causes = causes(status);
        if (causes.contains("not an SSL/TLS record") || causes.contains("NotSslRecordException")) {
            if (factryCertificate) {
                return " The server does not answer with TLS, but the token says Factry serves its built-in"
                        + " certificate, so the module uses TLS. If Factry's gRPC TLS was turned off after the token"
                        + " was made, make a new collector token. Otherwise check Host Override and Port Override, or"
                        + " the grpc-port in the token.";
            }
            return " The server does not answer with TLS. Check Host Override and Port Override, or the grpc-port in"
                    + " the token. If Factry's gRPC TLS is turned off (USE_SELF_SIGNED_CERTIFICATE=false) and nothing in"
                    + " between ends TLS, turn off Use TLS, and also Skip TLS Verification and Custom CA Certificate,"
                    + " which use TLS too.";
        }
        // A plain text client against a TLS port gets only this; so does any dropped connection, so
        // it is a hint only when the client did not use TLS.
        if (!tls && causes.contains("Network closed for unknown reason")) {
            return " Use TLS is off. If Factry, or a proxy in front of it, expects TLS, turn Use TLS on.";
        }
        if (causes.contains("signed by Factry's CA")) {
            return " The certificate is signed by Factry's CA but is not Factry's built-in certificate, which names"
                    + " only " + FACTRY_CERTIFICATE_NAME + " (the host the module connects to does not matter for"
                    + " it). Check what serves this host and port.";
        }
        if (causes.contains("No subject alternative") || causes.contains("No name matching")) {
            return " Factry's certificate does not name the host the module connects to. Set Host Override to a"
                    + " name in the certificate, or give Factry a certificate for this host. If Custom CA"
                    + " Certificate holds Factry's own CA, clear it: Factry's built-in certificate is checked"
                    + " without it.";
        }
        if (causes.contains("CertificateExpiredException") || causes.contains("CertificateNotYetValidException")
                || causes.contains("validity check failed")) {
            return " Factry's certificate has expired or is not valid yet. Renew the certificate of Factry (or of"
                    + " the proxy in front of it), and check the gateway's clock.";
        }
        if (causes.contains("PKIX path building failed") || causes.contains("unable to find valid certification path")) {
            return " Factry's certificate is not signed by a CA the module trusts. If Factry, or a proxy in front"
                    + " of it, has a certificate of its own, paste the CA that signed it in Custom CA Certificate.";
        }
        return "";
    }

    private static String causes(Status status) {
        StringBuilder sb = new StringBuilder(String.valueOf(status.getDescription()));
        for (Throwable t = status.getCause(); t != null && sb.length() < 10_000; t = t.getCause()) {
            sb.append(' ').append(t.getClass().getName()).append(": ").append(t.getMessage());
        }
        return sb.toString();
    }

    public void shutdown() {
        lockChannel();
        try {
            shutdownChannel();
        } finally {
            channelLock.writeLock().unlock();
        }
    }

    /**
     * Takes the write lock. Running calls hold the read lock, so this waits for them, but only for
     * the grace period: then it cancels them, so a Factry that never answers cannot hold up a
     * settings save or gateway shutdown until the calls time out. A cancelled call fails with
     * UNAVAILABLE, which store and forward retries.
     */
    private void lockChannel() {
        try {
            if (channelLock.writeLock().tryLock(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.warn("gRPC calls still running after {} s, cancelling them", SHUTDOWN_GRACE_SECONDS);
        channel.shutdownNow();
        channelLock.writeLock().lock();
    }

    /** Shuts down the channel. Caller must hold the write lock. */
    private void shutdownChannel() {
        logger.info("Shutting down gRPC channel");
        if (channel == null) {
            return;
        }
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("gRPC channel shutdown interrupted, forcing shutdown");
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public boolean isShutdown() {
        return channel.isShutdown();
    }

    /** The collector this client writes as: Factry only stores points for this collector's measurements. */
    public String getCollectorUUID() {
        return collectorUUID;
    }

    /** Whether {@code e} says Factry could not be reached in time: UNAVAILABLE or DEADLINE_EXCEEDED. */
    static boolean isConnectionError(Throwable e) {
        if (!(e instanceof StatusRuntimeException)) {
            return false;
        }
        Status.Code code = ((StatusRuntimeException) e).getStatus().getCode();
        return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;
    }

    public boolean isConnected() {
        return connected;
    }

    public void markConnected() {
        connected = true;
    }

    /**
     * Test connectivity and the collector token with GetCollector.
     * <p>
     * Not GetMeasurements: that returns every measurement of the collector, which on a slow
     * link or a large site does not fit in the deadline, so the check would fail forever and
     * store and forward would never resume. GetCollector returns one small record and still
     * fails when Factry rejects the token.
     *
     * @return true if the server answers for this collector
     */
    public boolean testConnection() {
        return checkConnection().isOk();
    }

    /**
     * {@link #testConnection()}, with Factry's answer: OK, or why not (unreachable, a refused
     * token, a deleted collector), for the profile status.
     */
    public Status checkConnection() {
        channelLock.readLock().lock();
        try {
            blockingStub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getCollector(Empty.getDefaultInstance());
            connected = true;
            return Status.OK;
        } catch (Exception e) {
            connected = false;
            logger.debug("Connection test failed: {}", e.getMessage());
            if (e instanceof StatusRuntimeException sre
                    && sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                // A connection the network dropped without closing it (firewall or NAT timeout)
                // still looks ready, and every call on it times out until TCP gives up, about
                // 16 minutes on Linux. Drop it, so the next call opens a new connection.
                channel.enterIdle();
            }
            return Status.fromThrowable(e);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    /**
     * Whether Factry says the collector of the token was deleted. Factry v8.2.3 answers every
     * call with NOT_FOUND "collector is deleted" then; there is no distinct code for it.
     */
    static boolean isCollectorDeleted(Status status) {
        return status.getCode() == Status.Code.NOT_FOUND
                && status.getDescription() != null
                && status.getDescription().contains("collector is deleted");
    }
}
