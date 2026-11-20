package io.factry.historian.gateway;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The REST call for the default database checks Factry's certificate exactly as the gRPC channel
 * does, through the JDK's HttpClient: its own host name check must neither refuse Factry's
 * built-in certificate (named historian.factry.local, reached by IP) nor be switched off. A
 * refused certificate means no request, so the token is never sent.
 */
class RestTlsTest {

    private static final String LOOPBACK = InetAddress.getLoopbackAddress().getHostAddress();

    private HttpsServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void factrysBuiltInCertificatePassesWithTheClaimAtAnyHost() throws Exception {
        assertEquals("db-a", ask(serve(TestTls.jdkServer(TestTls.FACTRY_BUILT_IN)), true, false, null));
    }

    @Test
    void withoutTheClaimTheBuiltInCertificateIsRefused() throws Exception {
        assertRefused(serve(TestTls.jdkServer(TestTls.FACTRY_BUILT_IN)), false, false, null);
    }

    /** Signed by Factry's CA, but not the built-in name: the Factry check accepts only that name. */
    @Test
    void factrySignedCertificateForAnotherNameIsRefused() throws Exception {
        assertRefused(serve(TestTls.jdkServer(TestTls.LOOPBACK)), true, false, null);
    }

    @Test
    void aProxyWithACertificateTheSystemTrustsPassesWithTheClaim() throws Exception {
        int port = serve(TestTls.jdkServer(TestTls.LOOPBACK));
        TestTls.withTestCaAsSystemCa(() -> assertEquals("db-a", askWithRealFactryCa(port)));
    }

    @Test
    void aSelfSignedCertificateNamingHistorianFactryLocalIsRefusedWithTheClaim() throws Exception {
        assertRefused(serve(selfSigned("other")), true, false, null);
    }

    @Test
    void aCustomCaIsTrustedAndTheHostIsChecked() throws Exception {
        assertEquals("db-a", ask(serve(TestTls.jdkServer(TestTls.LOOPBACK)), true, false, TestTls.caPem()));
        requests.clear();
        stop();
        assertRefused(serve(TestTls.jdkServer(TestTls.FACTRY_BUILT_IN)), false, false, TestTls.caPem());
    }

    @Test
    void skipTlsVerificationAcceptsAnyCertificate() throws Exception {
        assertEquals("db-a", ask(serve(selfSigned("other")), false, true, null));
    }

    /**
     * The REST call goes to the gRPC host the profile uses (Host Override over the token's host),
     * on the token's rest-port: Port Override is the gRPC port and does not apply to it.
     */
    @Test
    void restCallUsesHostOverrideAndTheTokensRestPort() throws Exception {
        int restPort = serve(TestTls.jdkServer(TestTls.LOOPBACK));
        try (FakeFactryServer factry = new FakeFactryServer(TestTls.LOOPBACK)) {
            factry.database = "db-a";
            Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
            String token = enc.encodeToString("{\"alg\":\"HS256\"}".getBytes(StandardCharsets.UTF_8)) + "."
                    + enc.encodeToString(("{\"uuid\":\"collector\",\"name\":\"Ignition\",\"organization-uuid\":\"org-1\","
                    + "\"aud\":\"http://nowhere.invalid\",\"grpc-port\":\"1\",\"rest-port\":\"" + restPort + "\"}")
                    .getBytes(StandardCharsets.UTF_8)) + ".sig";
            com.inductiveautomation.ignition.common.gson.JsonObject config = new com.inductiveautomation.ignition.common.gson.JsonObject();
            config.add("collectorToken", FakeSecrets.embedded(token).toJsonTree());
            config.addProperty("customCaCert", TestTls.caPem());
            config.addProperty("hostOverride", LOOPBACK);
            config.addProperty("portOverride", factry.port());
            FactryHistorianExtensionPoint ep = new FactryHistorianExtensionPoint();
            FactryHistoryProvider provider = new FactryHistoryProvider(new FakeSecrets().gatewayContext(), "Factry",
                    ep.decode(config));
            try {
                // A save that changes something: one that changes nothing applies nothing.
                FactryHistorianSettings saved = ep.decode(config);
                saved.setDelimiter("/");
                assertTrue(provider.handleSettingsChange(saved));
                assertEquals(List.of("/api/collectors/collector"), requests.stream().distinct().toList());
            } finally {
                provider.onShutdown();
            }
        }
    }

    private void assertRefused(int port, boolean claim, boolean skip, String customCa) {
        assertThrows(Exception.class, () -> ask(port, claim, skip, customCa));
        assertEquals(List.of(), requests, "no request, so the token was not sent");
    }

    private String ask(int port, boolean claim, boolean skip, String customCa) throws Exception {
        FactryGrpcClient client = new FactryGrpcClient(LOOPBACK, 1, "collector-a", token(port), true, skip,
                customCa, claim, TestTls.ca());
        try {
            return client.getDefaultDatabaseUUID();
        } finally {
            client.shutdown();
        }
    }

    private String askWithRealFactryCa(int port) throws Exception {
        FactryGrpcClient client = new FactryGrpcClient(LOOPBACK, 1, "collector-a", token(port), true, false,
                null, true, FactryGrpcClient.bundledFactryCa());
        try {
            return client.getDefaultDatabaseUUID();
        } finally {
            client.shutdown();
        }
    }

    private int serve(SSLContext tls) throws Exception {
        server = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestURI().toString());
            byte[] answer = "{\"DefaultDatabaseUUID\":\"db-a\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, answer.length);
            exchange.getResponseBody().write(answer);
            exchange.close();
        });
        server.start();
        return server.getAddress().getPort();
    }

    /** The self-signed key stores of DefaultDatabaseTest; "other" names only historian.factry.local. */
    private static SSLContext selfSigned(String name) throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream in = RestTlsTest.class.getClassLoader().getResourceAsStream("rest-tls/" + name + ".p12")) {
            keys.load(in, "changeit".toCharArray());
        }
        javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(
                javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keys, "changeit".toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, null);
        return context;
    }

    private static String token(int restPort) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"HS256\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(("{\"uuid\":\"collector-a\",\"organization-uuid\":\"org-1\","
                + "\"grpc-port\":\"1\",\"rest-port\":\"" + restPort + "\"}").getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".sig";
    }
}
