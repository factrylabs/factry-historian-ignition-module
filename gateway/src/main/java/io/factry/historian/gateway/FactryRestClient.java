package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.gson.JsonElement;
import com.inductiveautomation.ignition.common.gson.JsonObject;
import com.inductiveautomation.ignition.common.gson.JsonParser;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The one thing the module asks of Factry's REST API: the collector's default time-series
 * database, which Factry's gRPC {@code Collector} does not carry. Factry v8.2.3 answers
 * {@code GET /api/collectors/<uuid>} to the collector token itself, with the organisation from
 * the token in {@code x-organization-uuid}. The REST port is the token's {@code rest-port} claim,
 * on the gRPC host. HTTPS when the profile uses TLS, with the gRPC channel's own trust decision
 * ({@link FactryGrpcClient#trustManagerFactory}); never plain HTTP then, so the token is not sent
 * in the clear. One HTTP client per channel settings, and one deadline over the whole call, body
 * included: a REST port that does not answer costs a full cache reload {@link #TIMEOUT}.
 */
final class FactryRestClient {

    static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final HttpClient http;
    private final HttpRequest request;

    private FactryRestClient(HttpClient http, HttpRequest request) {
        this.http = http;
        this.request = request;
    }

    /**
     * A client for the collector of {@code token}, or null when the token does not say where the
     * REST API is (no {@code rest-port}, organisation or collector claim).
     */
    static FactryRestClient forToken(String host, String token, boolean useTls, TrustManagerFactory trust)
            throws Exception {
        JsonObject claims = JwtTokenParser.parsePayload(token).orElse(null);
        Integer port = claims != null ? JwtTokenParser.getRestPort(claims).orElse(null) : null;
        String organization = claims != null ? JwtTokenParser.getOrganizationUUID(claims).orElse(null) : null;
        String collector = claims != null ? JwtTokenParser.getCollectorUUID(claims).orElse(null) : null;
        if (port == null || organization == null || collector == null) {
            return null;
        }
        String uriHost = host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host;
        URI uri = URI.create((useTls ? "https" : "http") + "://" + uriHost + ":" + port + "/api/collectors/" + collector);

        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(TIMEOUT);
        if (useTls) {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trust != null ? trust.getTrustManagers() : null, null);
            builder.sslContext(context);
        }
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(TIMEOUT)
                .header("Authorization", "Bearer " + token)
                .header("x-organization-uuid", organization)
                .header("Accept", "application/json")
                .GET().build();
        return new FactryRestClient(builder.build(), request);
    }

    /**
     * The default database UUID, or null when Factry answers without one. Throws when Factry
     * could not be asked or did not answer within {@link #TIMEOUT}.
     */
    String defaultDatabaseUUID() throws Exception {
        CompletableFuture<HttpResponse<String>> call = http.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> response;
        try {
            // HttpRequest.timeout covers the headers only; this also bounds a body that stalls.
            response = call.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException e) {
            call.cancel(true);
            throw e;
        }
        if (response.statusCode() != 200) {
            throw new IOException("HTTP " + response.statusCode() + " from " + request.uri().getPath());
        }
        JsonElement database = JsonParser.parseString(response.body()).getAsJsonObject().get("DefaultDatabaseUUID");
        return database == null || database.isJsonNull() || database.getAsString().isBlank()
                ? null : database.getAsString();
    }
}
