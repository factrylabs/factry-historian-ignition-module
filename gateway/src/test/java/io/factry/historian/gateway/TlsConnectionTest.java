package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.gson.JsonObject;
import io.factry.historian.gateway.IgnitionHistoryQuery.Dataset;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Factry's gRPC port always uses TLS (#55). A new profile connects with TLS without any TLS field;
 * when the token says Factry serves its built-in certificate, that certificate is checked against
 * Factry's CA as historian.factry.local, whatever host the gateway connects to; and a failed check
 * says which setting to change.
 */
class TlsConnectionTest {

    private static final long START = 1_700_000_000_000L;
    private static final String LOOPBACK = InetAddress.getLoopbackAddress().getHostAddress();

    private final List<AutoCloseable> open = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : open) {
            c.close();
        }
    }

    @Test
    void newProfileWithOnlyATokenConnectsWithTls() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);
        String uuid = factry.addMeasurement("default/a", "number");
        factry.addPoint(uuid, START, 1.0);
        JsonObject config = new JsonObject();
        config.add("collectorToken", FakeSecrets.embedded(jwt(factry.port(), false)).toJsonTree());
        config.addProperty("customCaCert", TestTls.caPem());

        FactryHistoryProvider provider = new FactryHistoryProvider(new FakeSecrets().gatewayContext(), "Factry",
                new FactryHistorianExtensionPoint().decode(config));
        open.add(provider::onShutdown);

        Dataset ds = IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1, null,
                "default/a");
        assertNull(ds.error);
        assertEquals(1, ds.rows.size());
    }

    @Test
    void factrysBuiltInCertificateIsCheckedAsHistorianFactryLocal() throws Exception {
        FakeFactryServer factry = server(TestTls.FACTRY_BUILT_IN);

        Status check = client(factry, true, null).checkConnection();

        assertTrue(check.isOk(), check.toString());
    }

    @Test
    void withoutTheClaimTheCertificateMustNameTheHost() throws Exception {
        FakeFactryServer factry = server(TestTls.FACTRY_BUILT_IN);

        Status check = client(factry, false, TestTls.caPem()).checkConnection();

        assertEquals(Status.Code.UNAVAILABLE, check.getCode());
        assertTrue(FactryGrpcClient.tlsHint(check).contains("does not name the host"), FactryGrpcClient.tlsHint(check));
        assertTrue(FactryGrpcClient.tlsHint(check).contains("clear it"), FactryGrpcClient.tlsHint(check));
    }

    @Test
    void certificateFromAnUnknownCaSaysToPasteTheCa() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);

        Status check = client(factry, false, null).checkConnection();

        assertEquals(Status.Code.UNAVAILABLE, check.getCode());
        assertTrue(WriteHealth.checkFailure(check).details(check).contains("Custom CA Certificate"),
                WriteHealth.checkFailure(check).details(check));
    }

    /** Factry behind a proxy with a certificate of its own: the token still says built-in. */
    @Test
    void customCaWinsOverTheClaim() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);

        Status check = client(factry, true, TestTls.caPem()).checkConnection();

        assertTrue(check.isOk(), check.toString());
    }

    @Test
    void serverWithoutTlsSaysToCheckHostAndPort() throws Exception {
        FakeFactryServer plain = server(null);

        Status check = client(plain, false, TestTls.caPem()).checkConnection();

        assertEquals(Status.Code.UNAVAILABLE, check.getCode());
        assertTrue(FactryGrpcClient.tlsHint(check).contains("does not answer with TLS"), FactryGrpcClient.tlsHint(check));
    }

    /**
     * Netty's NotSslRecordException names the channel pipeline and dumps the reply in hex: over a
     * thousand characters in front of the hint, unreadable in the status tooltip.
     */
    @Test
    void notTlsStatusIsShortWithTheHint() throws Exception {
        FakeFactryServer plain = server(null);
        Status check = client(plain, false, TestTls.caPem()).checkConnection();

        String details = WriteHealth.checkFailure(check).details(check);

        assertTrue(details.contains("UNAVAILABLE"), details);
        assertTrue(details.contains("not an SSL/TLS record"), details);
        assertTrue(details.contains("The server does not answer with TLS"), details);
        assertFalse(details.contains("Channel Pipeline") || details.contains("SslHandler"), details);
        assertTrue(details.length() < 500, details.length() + ": " + details);
    }

    /** Any other long cause is cut, so no Netty text can blow up the status either. */
    @Test
    void longCauseIsCut() {
        Status status = Status.UNAVAILABLE.withDescription("io exception")
                .withCause(new RuntimeException("x".repeat(5_000)));

        assertTrue(WriteHealth.describe(status).length() <= 250, WriteHealth.describe(status));
    }

    /**
     * A proxy with a certificate from a CA the system trusts (Let's Encrypt, an enterprise CA) in
     * front of a Factry that still serves its built-in certificate: the token carries the claim, and
     * the normal check must still pass. Before, only Factry's CA was trusted then.
     */
    @Test
    void proxyWithACertificateTheSystemTrustsStillPassesWithTheClaim() throws Exception {
        FakeFactryServer proxy = server(TestTls.LOOPBACK);
        TestTls.withTestCaAsSystemCa(() -> {
            Status check = clientWithRealFactryCa(proxy, true).checkConnection();
            assertTrue(check.isOk(), check.toString());
        });
    }

    /** That normal check still compares the host: a system-trusted certificate for another name fails. */
    @Test
    void withTheClaimASystemTrustedCertificateMustStillNameTheHost() throws Exception {
        FakeFactryServer other = server(TestTls.FACTRY_BUILT_IN);
        TestTls.withTestCaAsSystemCa(() -> {
            Status check = clientWithRealFactryCa(other, true).checkConnection();
            assertEquals(Status.Code.UNAVAILABLE, check.getCode());
            assertTrue(FactryGrpcClient.tlsHint(check).contains("does not name the host"), FactryGrpcClient.tlsHint(check));
        });
    }

    /**
     * Signed by Factry's CA but not naming historian.factry.local: no host can fix that, since the
     * Factry check accepts only that name. The hint must not send the user to Host Override.
     */
    @Test
    void factrySignedCertificateForAnotherNameIsNotTheBuiltInOne() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);

        Status check = client(factry, true, null).checkConnection();

        assertEquals(Status.Code.UNAVAILABLE, check.getCode());
        String hint = FactryGrpcClient.tlsHint(check);
        assertTrue(hint.contains("not Factry's built-in certificate"), hint);
        assertFalse(hint.contains("Set Host Override"), hint);
    }

    /** The two-argument check knows no host, so it must never pass a certificate. */
    @Test
    void checkWithoutAHostIsRefused() throws Exception {
        FactryOrSystemTrustManager tm = new FactryOrSystemTrustManager(TestTls.ca());
        java.security.cert.X509Certificate[] chain = {(java.security.cert.X509Certificate)
                java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(
                        TlsConnectionTest.class.getClassLoader().getResourceAsStream("tls/test-factry-builtin.crt"))};

        assertThrows(java.security.cert.CertificateException.class, () -> tm.checkServerTrusted(chain, "ECDHE_ECDSA"));
    }

    /** "PKIX path validation failed ... validity check failed" is no unknown CA. */
    @Test
    void expiredCertificateSaysItExpired() throws Exception {
        FakeFactryServer factry = server("test-server-expired");

        Status check = client(factry, false, TestTls.caPem()).checkConnection();

        assertEquals(Status.Code.UNAVAILABLE, check.getCode());
        String hint = FactryGrpcClient.tlsHint(check);
        assertTrue(hint.contains("expired"), hint);
        assertFalse(hint.contains("not signed by a CA"), hint);
    }

    private FactryGrpcClient clientWithRealFactryCa(FakeFactryServer server, boolean factryCertificate) {
        FactryGrpcClient client = new FactryGrpcClient(LOOPBACK, server.port(), "collector", "token", true, false,
                null, factryCertificate, FactryGrpcClient.bundledFactryCa());
        open.add(0, client::shutdown);
        return client;
    }

    private FakeFactryServer server(String certificate) throws Exception {
        FakeFactryServer server = new FakeFactryServer(certificate);
        open.add(server);
        return server;
    }

    /** A client to 127.0.0.1 with the test CA standing in for Factry's. */
    private FactryGrpcClient client(FakeFactryServer server, boolean factryCertificate, String customCa) {
        FactryGrpcClient client = new FactryGrpcClient(LOOPBACK, server.port(), "collector", "token", true, false,
                customCa, factryCertificate, TestTls.ca());
        open.add(0, client::shutdown);
        return client;
    }

    private static String jwt(int port, boolean factryCertificate) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(("{\"uuid\":\"collector\",\"name\":\"Ignition\","
                + "\"aud\":\"http://127.0.0.1\",\"grpc-port\":\"" + port + "\","
                + "\"use-self-signed-certificate\":" + factryCertificate + "}").getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".sig";
    }
}
