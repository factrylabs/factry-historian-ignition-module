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
 * TLS by default (#55). A new profile connects with TLS without any TLS field; when the token says
 * Factry serves its built-in certificate, the module uses TLS even with Plain Text on, and that
 * certificate is checked against Factry's CA as historian.factry.local, whatever host the gateway
 * connects to. Only Plain Text on with a token without that claim connects in plain text, for a
 * Factry whose gRPC TLS is turned off. A failed check says which setting to change.
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

    /** Factry with its gRPC TLS turned off (USE_SELF_SIGNED_CERTIFICATE=false): its tokens carry no claim. */
    @Test
    void plainTextConnectsToAFactryWithoutTls() throws Exception {
        FakeFactryServer factry = server(null);
        String uuid = factry.addMeasurement("default/a", "number");
        factry.addPoint(uuid, START, 1.0);

        FactryHistoryProvider provider = provider(factry, true, false, null);

        Dataset ds = IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1, null,
                "default/a");
        assertNull(ds.error);
        assertEquals(1, ds.rows.size());
    }

    /**
     * Profiles saved with the old "Use TLS" off (now Plain Text on) against a Factry with its built-in
     * certificate: the token's claim wins, as it does for Factry's own collectors. A proxy whose
     * certificate the system trusts, so nothing but the claim turns TLS on.
     */
    @Test
    void plainTextStillUsesTlsWhenTheTokenSaysBuiltInCertificate() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);
        String uuid = factry.addMeasurement("default/a", "number");
        factry.addPoint(uuid, START, 1.0);

        TestTls.withTestCaAsSystemCa(() -> {
            FactryHistoryProvider provider = provider(factry, true, true, null);
            Dataset ds = IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1,
                    null, "default/a");
            assertNull(ds.error);
            assertEquals(1, ds.rows.size());
        });
    }

    /**
     * A profile from before #42 as it is stored: "useTls": false (its old default) becomes Plain
     * Text on when decoded, and the token's claim still makes it use TLS against a default Factry.
     */
    @Test
    void profileFromBefore42WithUseTlsOffStillUsesTlsWithTheClaim() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);
        String uuid = factry.addMeasurement("default/a", "number");
        factry.addPoint(uuid, START, 1.0);
        JsonObject config = new JsonObject();
        config.add("collectorToken", FakeSecrets.embedded(jwt(factry.port(), true)).toJsonTree());
        config.addProperty("useTls", false);

        TestTls.withTestCaAsSystemCa(() -> {
            FactryHistorianSettings settings = new FactryHistorianExtensionPoint().decode(config);
            assertTrue(settings.isPlainText());
            FactryHistoryProvider provider = new FactryHistoryProvider(new FakeSecrets().gatewayContext(), "Factry",
                    settings);
            open.add(provider::onShutdown);
            Dataset ds = IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1,
                    null, "default/a");
            assertNull(ds.error);
            assertEquals(1, ds.rows.size());
        });
    }

    /** The same old profile against a Factry with its gRPC TLS off and no claim: plain text, as before #42. */
    @Test
    void profileFromBefore42WithUseTlsOffConnectsToAFactryWithoutTls() throws Exception {
        FakeFactryServer factry = server(null);
        String uuid = factry.addMeasurement("default/a", "number");
        factry.addPoint(uuid, START, 1.0);
        JsonObject config = new JsonObject();
        config.add("collectorToken", FakeSecrets.embedded(jwt(factry.port(), false)).toJsonTree());
        config.addProperty("useTls", false);

        FactryHistoryProvider provider = new FactryHistoryProvider(new FakeSecrets().gatewayContext(), "Factry",
                new FactryHistorianExtensionPoint().decode(config));
        open.add(provider::onShutdown);

        Dataset ds = IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1, null,
                "default/a");
        assertNull(ds.error);
        assertEquals(1, ds.rows.size());
    }

    /** A Custom CA only makes sense with TLS, so it means TLS with Plain Text on. */
    @Test
    void aCustomCaMeansTlsWithPlainTextOn() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);
        String uuid = factry.addMeasurement("default/a", "number");
        factry.addPoint(uuid, START, 1.0);

        FactryHistoryProvider provider = provider(factry, true, false, TestTls.caPem());

        Dataset ds = IgnitionHistoryQuery.wide(provider.getQueryEngine().orElseThrow(), START, START + 1_000, -1, null,
                "default/a");
        assertNull(ds.error);
        assertEquals(1, ds.rows.size());
    }

    @Test
    void plainTextAgainstAFactryWithTlsSaysToTurnPlainTextOff() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);
        FactryGrpcClient client = new FactryGrpcClient(LOOPBACK, factry.port(), "collector", "token", false, false,
                null, false, TestTls.ca());
        open.add(0, client::shutdown);

        Status check = client.checkConnection();

        assertEquals(Status.Code.UNAVAILABLE, check.getCode());
        String hint = FactryGrpcClient.tlsHint(check, false, false);
        assertTrue(hint.contains("Plain Text (No TLS) is on") && hint.contains("turn it off"), check + " / " + hint);
        // The same answer to a TLS client is a dropped connection, not a TLS problem.
        assertFalse(FactryGrpcClient.tlsHint(check, true, false).contains("Plain Text"),
                FactryGrpcClient.tlsHint(check, true, false));
    }

    /** The profile's status carries that hint: the provider tells the status how it connects. */
    @Test
    void statusOfAPlainTextProfileAgainstTlsSaysToTurnPlainTextOff() throws Exception {
        FakeFactryServer factry = server(TestTls.LOOPBACK);
        FactryHistoryProvider provider = provider(factry, true, false, null);

        provider.checkConnection();

        String details = provider.statusOnceStarted().getErrorDetails();
        assertNotNull(details);
        assertTrue(details.contains("Plain Text (No TLS) is on"), details);
    }

    /**
     * A profile from before #42 with "Use TLS" off and Skip TLS Verification on, against a Factry
     * with its gRPC TLS off: Skip now means TLS, so the hint must name it, not only Plain Text.
     */
    @Test
    void tlsForcedBySkipTlsVerificationAgainstAPlainServerNamesIt() throws Exception {
        FakeFactryServer plain = server(null);
        FactryGrpcClient client = new FactryGrpcClient(LOOPBACK, plain.port(), "collector", "token", true, true,
                null, false, TestTls.ca());
        open.add(0, client::shutdown);

        Status check = client.checkConnection();

        String hint = FactryGrpcClient.tlsHint(check, true, false);
        assertTrue(hint.contains("turn off Skip TLS Verification and clear Custom CA Certificate"), hint);
    }

    /** Factry's gRPC TLS turned off after the token was made: the old token's claim still forces TLS. */
    @Test
    void tlsForcedByTheClaimAgainstAPlainServerSaysToMakeANewToken() throws Exception {
        FakeFactryServer plain = server(null);

        Status check = client(plain, true, null).checkConnection();

        String hint = FactryGrpcClient.tlsHint(check, true, true);
        assertTrue(hint.contains("make a new collector token"), hint);
        assertFalse(hint.contains("turn on Plain Text"), hint);
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
        assertTrue(FactryGrpcClient.tlsHint(check).contains("turn on Plain Text (No TLS)"), FactryGrpcClient.tlsHint(check));
        assertFalse(FactryGrpcClient.tlsHint(check).contains("new collector token"), FactryGrpcClient.tlsHint(check));
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

    /** A provider for this server, through the stored settings. */
    private FactryHistoryProvider provider(FakeFactryServer factry, boolean plainText, boolean factryCertificate,
                                           String customCa) throws Exception {
        FactryHistoryProvider provider = new FactryHistoryProvider(new FakeSecrets().gatewayContext(), "Factry",
                decode(factry, plainText, factryCertificate, customCa));
        open.add(provider::onShutdown);
        return provider;
    }

    private static FactryHistorianSettings decode(FakeFactryServer factry, boolean plainText, boolean factryCertificate,
                                                  String customCa) throws Exception {
        JsonObject config = new JsonObject();
        config.add("collectorToken", FakeSecrets.embedded(jwt(factry.port(), factryCertificate)).toJsonTree());
        if (customCa != null) {
            config.addProperty("customCaCert", customCa);
        }
        config.addProperty("plainText", plainText);
        return new FactryHistorianExtensionPoint().decode(config);
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
