package io.factry.historian.gateway;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The collector's default time-series database, which tells the module its own Site. Factry's gRPC
 * API does not give it; its REST API does, at {@code GET /api/collectors/<uuid>}, to the collector
 * token itself (checked live on 8.2.3), on the port of the token's {@code rest-port} claim. HTTPS
 * when the profile uses TLS, with the gRPC channel's certificate checks; bounded in time.
 */
class DefaultDatabaseTest {

    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private volatile int status = 200;
    private volatile String body = "{\"Name\":\"ignition-site-a\",\"DefaultDatabaseUUID\":\"db-a\"}";

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    /** Answers as Factry does, over plain HTTP, or over HTTPS with the key store {@code tls}. */
    private int serve(String tls) throws Exception {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        if (tls == null) {
            server = HttpServer.create(address, 0);
        } else {
            HttpsServer https = HttpsServer.create(address, 0);
            https.setHttpsConfigurator(new HttpsConfigurator(serverContext(tls)));
            server = https;
        }
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI()
                    + " auth=" + exchange.getRequestHeaders().getFirst("Authorization")
                    + " org=" + exchange.getRequestHeaders().getFirst("x-organization-uuid"));
            byte[] answer = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, answer.length);
            exchange.getResponseBody().write(answer);
            exchange.close();
        });
        server.start();
        return server.getAddress().getPort();
    }

    private static SSLContext serverContext(String name) throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream in = resource("rest-tls/" + name + ".p12")) {
            keys.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keys, "changeit".toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, null);
        return context;
    }

    private static InputStream resource(String path) {
        return DefaultDatabaseTest.class.getClassLoader().getResourceAsStream(path);
    }

    private static String certificate(String name) throws Exception {
        try (InputStream in = resource("rest-tls/" + name + ".crt")) {
            return new String(in.readAllBytes(), StandardCharsets.US_ASCII);
        }
    }

    private static String token(int restPort) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"HS256\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(("{\"uuid\":\"collector-a\",\"organization-uuid\":\"org-1\","
                + "\"grpc-port\":\"1\",\"rest-port\":\"" + restPort + "\"}").getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".sig";
    }

    private static String ask(String host, int port, boolean tls, boolean skip, String customCa) throws Exception {
        FactryGrpcClient client = new FactryGrpcClient(host, 1, "collector-a", token(port), tls, skip, customCa);
        try {
            return client.getDefaultDatabaseUUID();
        } finally {
            client.shutdown();
        }
    }

    @Test
    void askedOfFactrysRestApiWithTheCollectorToken() throws Exception {
        int port = serve(null);

        assertEquals("db-a", ask("127.0.0.1", port, false, false, null));
        assertEquals(List.of("GET /api/collectors/collector-a auth=Bearer " + token(port) + " org=org-1"), requests);
    }

    @Test
    void noneWhenFactryAnswersWithoutOne() throws Exception {
        body = "{\"Name\":\"ignition-site-a\",\"DefaultDatabaseUUID\":null}";
        assertNull(ask("127.0.0.1", serve(null), false, false, null));
    }

    @Test
    void aRefusalIsNotAnAnswer() throws Exception {
        status = 403;
        int port = serve(null);
        assertThrows(Exception.class, () -> ask("127.0.0.1", port, false, false, null));
    }

    /** Headers arrive, the body never does. */
    @Test
    void aStalledBodyIsBoundedByTheTimeout() throws Exception {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 100);
            exchange.getResponseBody().write("{\"Default".getBytes(StandardCharsets.UTF_8));
            exchange.getResponseBody().flush();
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException ignored) {
                // the test is over
            }
            exchange.close();
        });
        server.start();
        int port = server.getAddress().getPort();

        long start = System.nanoTime();
        assertThrows(Exception.class, () -> ask("127.0.0.1", port, false, false, null));
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertTrue(ms < 5_000, "took " + ms + " ms");
    }

    /** TCP accepted by the kernel, nothing ever answered. */
    @Test
    void aSilentPortIsBounded() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            long start = System.nanoTime();
            assertThrows(Exception.class, () -> ask("127.0.0.1", socket.getLocalPort(), false, false, null));
            long ms = (System.nanoTime() - start) / 1_000_000;
            assertTrue(ms < 5_000, "took " + ms + " ms");
        }
    }

    @Test
    void withTlsTheTokenIsNeverSentOverPlainHttp() throws Exception {
        int port = serve(null);

        assertThrows(Exception.class, () -> ask("127.0.0.1", port, true, false, null));
        assertEquals(List.of(), requests);
    }

    @Test
    void skipTlsVerificationAcceptsAnyCertificateAndName() throws Exception {
        assertEquals("db-a", ask("127.0.0.1", serve("other"), true, true, null));
    }

    @Test
    void anUntrustedCertificateIsRefused() throws Exception {
        int port = serve("localhost");

        assertThrows(Exception.class, () -> ask("localhost", port, true, false, null));
        assertEquals(List.of(), requests);
    }

    @Test
    void aCustomCaIsTrusted() throws Exception {
        assertEquals("db-a", ask("localhost", serve("localhost"), true, false, certificate("localhost")));
    }

    @Test
    void aTrustedCertificateForAnotherNameIsRefused() throws Exception {
        int port = serve("other");

        assertThrows(Exception.class, () -> ask("127.0.0.1", port, true, false, certificate("other")));
        assertEquals(List.of(), requests);
    }
}
