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
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FactryGrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(FactryGrpcClient.class);
    private static final Metadata.Key<String> COLLECTOR_UUID_KEY =
            Metadata.Key.of("collectoruuid", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final long WRITE_DEADLINE_SECONDS = ModuleProperties.getWriteDeadlineSeconds();
    private static final String CA_CERT_RESOURCE = "factry-historian-ca.crt";

    /**
     * Guards channel/stub access during reconfiguration.
     * gRPC calls take the read lock (concurrent), reconfigure takes the write lock (exclusive).
     */
    private final ReadWriteLock channelLock = new ReentrantReadWriteLock();

    private volatile ManagedChannel channel;
    private volatile HistorianGrpc.HistorianBlockingStub blockingStub;
    private volatile boolean connected = true;

    public FactryGrpcClient(String host, int port, String collectorUUID, String token,
                            boolean useTls, boolean skipTlsVerification, String customCaCert) {
        buildChannel(host, port, collectorUUID, token, useTls, skipTlsVerification, customCaCert);
        logger.info("gRPC client created ({}), target={}:{}, collectorUUID={}",
                tlsLabel(useTls, skipTlsVerification), host, port, collectorUUID);
    }

    public CreatePointsReply createPoints(Points points) {
        logger.debug("Sending CreatePoints with {} points", points.getPointsCount());
        channelLock.readLock().lock();
        try {
            CreatePointsReply reply = blockingStub
                    .withDeadlineAfter(WRITE_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .createPoints(points);
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

    public Measurements getMeasurements(MeasurementRequest request) {
        logger.debug("Sending GetMeasurements");
        channelLock.readLock().lock();
        try {
            return blockingStub.getMeasurements(request);
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
            return blockingStub.getMeasurementsByFilter(request);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public QueryTimeseriesResponse queryTimeseries(QueryTimeseriesRequest request) {
        logger.debug("Sending QueryTimeseries for {} measurements", request.getMeasurementUUIDsCount());
        channelLock.readLock().lock();
        try {
            return blockingStub.queryTimeseries(request);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public Assets getAssets() {
        logger.debug("Sending GetAssets");
        channelLock.readLock().lock();
        try {
            return blockingStub.getAssets(GetAssetsRequest.newBuilder().build());
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public AssetProperties getAssetProperties(GetAssetPropertiesRequest request) {
        logger.debug("Sending GetAssetProperties for {} assets", request.getAssetUUIDsCount());
        channelLock.readLock().lock();
        try {
            return blockingStub.getAssetProperties(request);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public Collectors getCollectors() {
        logger.debug("Sending GetCollectors");
        channelLock.readLock().lock();
        try {
            return blockingStub.getCollectors(GetCollectorsRequest.newBuilder().build());
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public Collector registerCollector(RegisterCollectorSchema schema) {
        logger.debug("Sending RegisterCollector");
        channelLock.readLock().lock();
        try {
            return blockingStub.registerCollector(schema);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public HealthUpdateReply updateHealth(HealthUpdates updates) {
        logger.debug("Sending UpdateHealth");
        channelLock.readLock().lock();
        try {
            return blockingStub.updateHealth(updates);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public void updateCollectorState(Struct state) {
        logger.debug("Sending UpdateCollectorState");
        channelLock.readLock().lock();
        try {
            blockingStub.updateCollectorState(state);
        } finally {
            channelLock.readLock().unlock();
        }
    }

    public CreateMeasurementsReply createMeasurements(CreateMeasurementsRequest request) {
        logger.debug("Sending CreateMeasurements with {} measurements", request.getMeasurementsCount());
        channelLock.readLock().lock();
        try {
            CreateMeasurementsReply reply = blockingStub
                    .withDeadlineAfter(WRITE_DEADLINE_SECONDS, TimeUnit.SECONDS)
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
        logger.info("Reconfiguring gRPC client: {}:{}", host, port);
        channelLock.writeLock().lock();
        try {
            shutdownChannel();
            buildChannel(host, port, collectorUUID, token, useTls, skipTlsVerification, customCaCert);
            logger.info("gRPC client reconfigured ({}), target={}:{}, collectorUUID={}",
                    tlsLabel(useTls, skipTlsVerification), host, port, collectorUUID);
        } finally {
            channelLock.writeLock().unlock();
        }
    }

    private void buildChannel(String host, int port, String collectorUUID, String token,
                              boolean useTls, boolean skipTlsVerification, String customCaCert) {
        if (useTls) {
            try {
                var sslBuilder = GrpcSslContexts.forClient();
                if (skipTlsVerification) {
                    sslBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    // Trust the system/public CAs AND the bundled Factry CA, plus any custom CA
                    // the user supplied per-profile. This lets publicly-signed (e.g. Let's Encrypt),
                    // Factry-signed, and internal/enterprise-CA or self-signed certificates all
                    // verify without having to disable verification entirely.
                    try (InputStream caCert = getClass().getClassLoader().getResourceAsStream(CA_CERT_RESOURCE)) {
                        if (caCert == null) {
                            logger.warn("Bundled CA certificate '{}' not found; using system CAs only", CA_CERT_RESOURCE);
                        }
                        boolean hasCustomCa = customCaCert != null && !customCaCert.isBlank();
                        logger.info("TLS verification using system CAs plus bundled Factry CA{}",
                                hasCustomCa ? " plus a custom CA certificate" : "");
                        sslBuilder.trustManager(buildCombinedTrustManagerFactory(caCert, customCaCert));
                    } catch (Exception e) {
                        logger.warn("Failed to build combined trust store ({}), using the system default trust store",
                                e.getMessage());
                    }
                }
                this.channel = NettyChannelBuilder.forAddress(host, port)
                        .sslContext(sslBuilder.build())
                        .build();
            } catch (SSLException e) {
                throw new RuntimeException("Failed to create TLS-enabled gRPC channel", e);
            }
        } else {
            this.channel = NettyChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
        }

        Metadata headers = new Metadata();
        headers.put(COLLECTOR_UUID_KEY, collectorUUID);
        headers.put(AUTHORIZATION_KEY, "Bearer " + token);

        this.blockingStub = HistorianGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    /**
     * Build a TrustManagerFactory that trusts the JVM's default (system/public) CAs plus the
     * bundled Factry CA. Convenience overload with no user-supplied custom CA.
     */
    static TrustManagerFactory buildCombinedTrustManagerFactory(InputStream extraCaCerts) throws Exception {
        return buildCombinedTrustManagerFactory(extraCaCerts, null);
    }

    /**
     * Build a TrustManagerFactory that trusts the JVM's default (system/public) CAs, the given
     * bundled CA certificate(s), and any user-supplied custom CA certificate(s) in PEM form.
     * This lets the module verify servers using a publicly-signed certificate (e.g. Let's
     * Encrypt), a Factry-signed one, or one issued by a customer's internal/enterprise CA (or a
     * self-signed certificate pasted directly), without having to turn off verification.
     *
     * <p>Trust is purely additive: the custom CA is added alongside, never in place of, the
     * system and bundled CAs. If {@code extraCaCerts} is null only the system CAs and any custom
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

        // 2. Extra (bundled Factry) CA certificate(s).
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

    private static String tlsLabel(boolean useTls, boolean skipTlsVerification) {
        if (!useTls) return "plaintext";
        return skipTlsVerification ? "TLS (insecure)" : "TLS";
    }

    public void shutdown() {
        channelLock.writeLock().lock();
        try {
            shutdownChannel();
        } finally {
            channelLock.writeLock().unlock();
        }
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

    public boolean isConnected() {
        return connected;
    }

    public void markConnected() {
        connected = true;
    }

    /**
     * Test connectivity by making a lightweight gRPC call.
     *
     * @return true if the server responds
     */
    public boolean testConnection() {
        channelLock.readLock().lock();
        try {
            blockingStub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getMeasurements(MeasurementRequest.newBuilder().build());
            connected = true;
            return true;
        } catch (Exception e) {
            connected = false;
            logger.debug("Connection test failed: {}", e.getMessage());
            return false;
        } finally {
            channelLock.readLock().unlock();
        }
    }
}
