package io.factry.historian.gateway;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Test certificates in {@code src/test/resources/tls}, all signed by one test CA (valid to 2126):
 * {@value #LOOPBACK} names localhost and 127.0.0.1; {@value #FACTRY_BUILT_IN} names only
 * historian.factry.local and historian.factry.dev, like Factry's built-in certificate, with the
 * test CA standing in for Factry's.
 */
final class TestTls {

    static final String LOOPBACK = "test-server-loopback";
    static final String FACTRY_BUILT_IN = "test-factry-builtin";

    private TestTls() {
    }

    static SslContext server(String name) throws IOException {
        try (InputStream cert = open(name + ".crt"); InputStream key = open(name + ".key")) {
            return GrpcSslContexts.forServer(cert, key).build();
        }
    }

    /** The JDK's server context for {@code name}, for an HTTPS stand-in of Factry's REST API. */
    static javax.net.ssl.SSLContext jdkServer(String name) throws Exception {
        String keyPem;
        java.security.cert.Certificate cert;
        try (InputStream key = open(name + ".key"); InputStream crt = open(name + ".crt")) {
            keyPem = new String(key.readAllBytes(), StandardCharsets.US_ASCII);
            cert = java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(crt);
        }
        byte[] der = java.util.Base64.getMimeDecoder().decode(keyPem.replaceAll("-----[A-Z ]+-----", ""));
        java.security.PrivateKey key = java.security.KeyFactory.getInstance("EC")
                .generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(der));
        java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("server", key, "changeit".toCharArray(), new java.security.cert.Certificate[]{cert});
        javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(
                javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, "changeit".toCharArray());
        javax.net.ssl.SSLContext context = javax.net.ssl.SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, null);
        return context;
    }

    static String caPem() {
        try (InputStream in = open("test-ca.crt")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<X509Certificate> ca() {
        return FactryGrpcClient.parseCaCertificates(caPem());
    }

    private static InputStream open(String file) {
        InputStream in = TestTls.class.getClassLoader().getResourceAsStream("tls/" + file);
        if (in == null) {
            throw new IllegalStateException("missing test resource tls/" + file);
        }
        return in;
    }

    interface Body {
        void run() throws Exception;
    }

    /** Runs {@code body} with the test CA as the JVM's only system CA (javax.net.ssl.trustStore). */
    static void withTestCaAsSystemCa(Body body) throws Exception {
        java.nio.file.Path store = java.nio.file.Files.createTempFile("test-ca", ".p12");
        java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setCertificateEntry("test-ca", TestTls.ca().get(0));
        try (var out = java.nio.file.Files.newOutputStream(store)) {
            ks.store(out, "changeit".toCharArray());
        }
        String[] keys = {"javax.net.ssl.trustStore", "javax.net.ssl.trustStoreType", "javax.net.ssl.trustStorePassword"};
        String[] old = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            old[i] = System.getProperty(keys[i]);
        }
        System.setProperty(keys[0], store.toString());
        System.setProperty(keys[1], "PKCS12");
        System.setProperty(keys[2], "changeit");
        try {
            body.run();
        } finally {
            for (int i = 0; i < keys.length; i++) {
                if (old[i] == null) {
                    System.clearProperty(keys[i]);
                } else {
                    System.setProperty(keys[i], old[i]);
                }
            }
            java.nio.file.Files.deleteIfExists(store);
        }
    }
}
