package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies the combined TLS trust store trusts the system CAs, the bundled Factry CA, and any custom CA. */
class FactryGrpcClientTlsTest {

    private static final String CA_RESOURCE = "factry-historian-ca.crt";
    /** A self-signed cert (CN=localhost) standing in for an internal-CA-signed or self-signed server cert. */
    private static final String SELF_SIGNED_RESOURCE = "test-selfsigned-localhost.crt";

    private static X509Certificate[] issuers(TrustManagerFactory tmf) {
        for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                return ((X509TrustManager) tm).getAcceptedIssuers();
            }
        }
        return new X509Certificate[0];
    }

    private static X509TrustManager x509TrustManager(TrustManagerFactory tmf) {
        for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                return (X509TrustManager) tm;
            }
        }
        throw new IllegalStateException("no X509TrustManager");
    }

    private static InputStream caStream() {
        return FactryGrpcClientTlsTest.class.getClassLoader().getResourceAsStream(CA_RESOURCE);
    }

    private static String selfSignedPem() throws Exception {
        try (InputStream in = FactryGrpcClientTlsTest.class.getClassLoader()
                .getResourceAsStream(SELF_SIGNED_RESOURCE)) {
            assertNotNull(in, "self-signed test cert resource should be present");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static X509Certificate selfSignedCert() throws Exception {
        try (InputStream in = FactryGrpcClientTlsTest.class.getClassLoader()
                .getResourceAsStream(SELF_SIGNED_RESOURCE)) {
            return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
        }
    }

    @Test
    void systemOnly_trustsPublicCAs() throws Exception {
        X509Certificate[] issuers = issuers(FactryGrpcClient.buildCombinedTrustManagerFactory(null));
        assertTrue(issuers.length > 0, "Should trust the JVM's built-in system/public CAs");
    }

    @Test
    void combined_trustsSystemCAsAndBundledFactryCA() throws Exception {
        // The bundled Factry CA (a private CA) should not already be a system CA.
        X509Certificate factryCa;
        try (InputStream ca = caStream()) {
            assertNotNull(ca, "bundled Factry CA cert resource should be present");
            factryCa = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(ca);
        }

        X509Certificate[] systemOnly = issuers(FactryGrpcClient.buildCombinedTrustManagerFactory(null));
        assertFalse(contains(systemOnly, factryCa),
                "The private Factry CA should not already be in the system trust store");

        // The combined store must trust the Factry CA AND keep all the system CAs.
        X509Certificate[] combined;
        try (InputStream ca = caStream()) {
            combined = issuers(FactryGrpcClient.buildCombinedTrustManagerFactory(ca));
        }
        assertTrue(contains(combined, factryCa), "Combined store must trust the bundled Factry CA");
        assertTrue(combined.length >= systemOnly.length + 1,
                "Combined store must keep the system CAs and add the Factry CA");
    }

    // --- parseCaCertificates ---

    @Test
    void parseCaCertificates_blankReturnsEmpty() {
        assertTrue(FactryGrpcClient.parseCaCertificates(null).isEmpty());
        assertTrue(FactryGrpcClient.parseCaCertificates("").isEmpty());
        assertTrue(FactryGrpcClient.parseCaCertificates("   \n  ").isEmpty());
    }

    @Test
    void parseCaCertificates_validPemReturnsCert() throws Exception {
        List<X509Certificate> certs = FactryGrpcClient.parseCaCertificates(selfSignedPem());
        assertEquals(1, certs.size());
        assertEquals(selfSignedCert(), certs.get(0));
    }

    @Test
    void parseCaCertificates_garbageThrowsClearError() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> FactryGrpcClient.parseCaCertificates("not a certificate"));
        assertTrue(ex.getMessage().toLowerCase().contains("custom ca certificate"),
                "error should point the user at the custom CA field: " + ex.getMessage());
    }

    // --- buildCombinedTrustManagerFactory with a custom CA ---

    @Test
    void combined_addsCustomCaAlongsideSystemAndBundled() throws Exception {
        X509Certificate custom = selfSignedCert();

        X509Certificate[] withoutCustom;
        try (InputStream ca = caStream()) {
            withoutCustom = issuers(FactryGrpcClient.buildCombinedTrustManagerFactory(ca, null));
        }
        assertFalse(contains(withoutCustom, custom), "custom cert should not be trusted unless supplied");

        X509Certificate[] withCustom;
        try (InputStream ca = caStream()) {
            withCustom = issuers(FactryGrpcClient.buildCombinedTrustManagerFactory(ca, selfSignedPem()));
        }
        assertTrue(contains(withCustom, custom), "combined store must trust the supplied custom CA");
        assertEquals(withoutCustom.length + 1, withCustom.length,
                "custom CA must be added, not replace the system + bundled CAs");
    }

    /**
     * The behaviour the customer needs: a server presenting a certificate that does not chain to
     * any public or Factry CA (internal/enterprise CA or self-signed) is rejected by default, but
     * accepted once its CA is supplied — without disabling verification. Exercises the real trust
     * decision via {@link X509TrustManager#checkServerTrusted}.
     */
    @Test
    void customCa_makesOtherwiseUntrustedServerTrusted() throws Exception {
        X509Certificate[] chain = { selfSignedCert() };

        X509TrustManager without;
        try (InputStream ca = caStream()) {
            without = x509TrustManager(FactryGrpcClient.buildCombinedTrustManagerFactory(ca, null));
        }
        assertThrows(CertificateException.class, () -> without.checkServerTrusted(chain, "RSA"),
                "without the custom CA the self-signed server cert must not verify");

        X509TrustManager with;
        try (InputStream ca = caStream()) {
            with = x509TrustManager(FactryGrpcClient.buildCombinedTrustManagerFactory(ca, selfSignedPem()));
        }
        assertDoesNotThrow(() -> with.checkServerTrusted(chain, "RSA"),
                "with the custom CA supplied the same server cert must verify");
    }

    private static boolean contains(X509Certificate[] certs, X509Certificate target) {
        return Arrays.stream(certs).anyMatch(c -> c.equals(target));
    }
}
