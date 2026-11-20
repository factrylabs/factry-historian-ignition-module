package io.factry.historian.gateway;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

/**
 * The check when the token says Factry serves its built-in certificate. The certificate the
 * gateway sees passes when either holds:
 * <ul>
 *   <li>the normal check: it chains to a system CA and names the host the client connects to
 *   (a proxy with a Let's Encrypt or enterprise certificate in front of Factry, which still
 *   serves the built-in one behind it);</li>
 *   <li>it chains to Factry's CA and names {@value FactryGrpcClient#FACTRY_CERTIFICATE_NAME}, the
 *   only name in Factry's built-in certificate, whatever host the client connects to.</li>
 * </ul>
 * An {@link X509ExtendedTrustManager}, so the TLS engine leaves the host name check to it: the
 * normal check gets the engine and checks the host as usual; the Factry check compares the name
 * itself.
 */
final class FactryOrSystemTrustManager extends X509ExtendedTrustManager {

    private final X509ExtendedTrustManager system;
    private final X509ExtendedTrustManager factry;
    private final List<X509Certificate> factryCa;

    FactryOrSystemTrustManager(List<X509Certificate> factryCa) throws Exception {
        this.system = extended(null);
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        for (int i = 0; i < factryCa.size(); i++) {
            ks.setCertificateEntry("factry-ca-" + i, factryCa.get(i));
        }
        this.factry = extended(ks);
        this.factryCa = factryCa;
    }

    private static X509ExtendedTrustManager extended(KeyStore ks) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509ExtendedTrustManager extended) {
                return extended;
            }
        }
        throw new IllegalStateException("no X509ExtendedTrustManager");
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        try {
            system.checkServerTrusted(chain, authType, engine);
        } catch (CertificateException e) {
            checkFactry(chain, authType, e);
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        try {
            system.checkServerTrusted(chain, authType, socket);
        } catch (CertificateException e) {
            checkFactry(chain, authType, e);
        }
    }

    /** Knows no host, so it cannot make the normal check; Netty and the JDK always pass the engine. */
    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        throw new CertificateException("Server certificate check without the host is not supported");
    }

    /** The Factry check; when it fails too, the error of the check that applies to this chain. */
    private void checkFactry(X509Certificate[] chain, String authType, CertificateException systemFailure)
            throws CertificateException {
        if (!issuedByFactry(chain)) {
            throw systemFailure;
        }
        // Without the engine: chain and validity only, the name is compared below.
        factry.checkServerTrusted(chain, authType);
        if (!names(chain[0], FactryGrpcClient.FACTRY_CERTIFICATE_NAME)) {
            throw new CertificateException("No subject alternative DNS name matching "
                    + FactryGrpcClient.FACTRY_CERTIFICATE_NAME + " found in the certificate signed by Factry's CA");
        }
    }

    private boolean issuedByFactry(X509Certificate[] chain) {
        X509Certificate top = chain[chain.length - 1];
        return factryCa.stream().anyMatch(ca -> ca.getSubjectX500Principal().equals(top.getIssuerX500Principal())
                || ca.equals(top));
    }

    private static boolean names(X509Certificate cert, String name) throws CertificateParsingException {
        Collection<List<?>> names = cert.getSubjectAlternativeNames();
        if (names == null) {
            return false;
        }
        for (List<?> entry : names) {
            // 2 = dNSName
            if (Integer.valueOf(2).equals(entry.get(0)) && name.equalsIgnoreCase(String.valueOf(entry.get(1)))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        throw new CertificateException("client certificates are not checked here");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        throw new CertificateException("client certificates are not checked here");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        throw new CertificateException("client certificates are not checked here");
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        X509Certificate[] systemIssuers = system.getAcceptedIssuers();
        X509Certificate[] all = java.util.Arrays.copyOf(systemIssuers, systemIssuers.length + factryCa.size());
        for (int i = 0; i < factryCa.size(); i++) {
            all[systemIssuers.length + i] = factryCa.get(i);
        }
        return all;
    }
}
