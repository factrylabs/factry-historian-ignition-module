package io.factry.historian.gateway;

import com.inductiveautomation.ignition.gateway.model.ProfileStatus;
import io.factry.historian.proto.Collector;
import io.factry.historian.proto.HistorianGrpc;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A failed connection check must say why (#32): Factry tells a refused token, a deleted collector
 * and an unreachable server apart, and the operator needs to know which one to fix. The status is
 * what the provider's getStatus() returns once started.
 */
class ConnectionCheckReasonTest {

    private Server server;
    private FactryGrpcClient client;
    private volatile Status getCollectorStatus = Status.OK;
    private final WriteHealth health = new WriteHealth();

    private class Factry extends HistorianGrpc.HistorianImplBase {
        @Override
        public void getCollector(com.google.protobuf.Empty request, StreamObserver<Collector> observer) {
            if (getCollectorStatus.isOk()) {
                observer.onNext(Collector.newBuilder().setUuid("collector-uuid").setStatus("Active").build());
                observer.onCompleted();
            } else {
                observer.onError(getCollectorStatus.asRuntimeException());
            }
        }
    }

    @BeforeEach
    void start() throws Exception {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .addService(new Factry()).build().start();
        client = new FactryGrpcClient(InetAddress.getLoopbackAddress().getHostAddress(), server.getPort(),
                "collector-uuid", "token", false, false, null);
    }

    @AfterEach
    void stop() throws Exception {
        client.shutdown();
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    /** Runs the check as the provider's scheduler does, and returns what the gateway log got. */
    private String check() {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true));
        try {
            health.connectionChecked(client.checkConnection());
        } finally {
            System.setErr(original);
        }
        return captured.toString();
    }

    @Test
    void aRefusedTokenSaysToReplaceIt() {
        getCollectorStatus = Status.UNAUTHENTICATED
                .withDescription("invalid auth token: can't parse token: signature is invalid");

        check();

        ProfileStatus status = health.status();
        assertEquals(ProfileStatus.State.Errored, status.getState());
        assertEquals("Factry refused the collector token", status.getMessage().toString());
        assertEquals("Factry refused the collector token (UNAUTHENTICATED: invalid auth token: can't parse"
                + " token: signature is invalid). Replace the token in the profile settings. Store and forward"
                + " keeps the data until it can be stored.", status.getErrorDetails());
    }

    @Test
    void aDeletedCollectorSaysSo() {
        getCollectorStatus = Status.NOT_FOUND.withDescription("collector is deleted");

        check();

        ProfileStatus status = health.status();
        assertEquals("Collector deleted in Factry", status.getMessage().toString());
        assertTrue(status.getErrorDetails().startsWith("The collector of this token was deleted in Factry"
                + " (NOT_FOUND: collector is deleted). Create a collector in Factry"), status.getErrorDetails());
    }

    @Test
    void anUnreachableFactrySaysSoWithTheCause() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            closedPort = socket.getLocalPort();
        }
        client.shutdown();
        client = new FactryGrpcClient(InetAddress.getLoopbackAddress().getHostAddress(), closedPort,
                "collector-uuid", "token", false, false, null);

        check();

        ProfileStatus status = health.status();
        assertEquals("Factry Historian unreachable", status.getMessage().toString());
        assertTrue(status.getErrorDetails().startsWith("Cannot reach Factry Historian (UNAVAILABLE"), status.getErrorDetails());
        assertTrue(status.getErrorDetails().contains("Connection refused"), "the cause: " + status.getErrorDetails());
    }

    @Test
    void theReasonIsLoggedOnceWhenItChanges() {
        getCollectorStatus = Status.UNAUTHENTICATED.withDescription("invalid auth token");
        String first = check();
        String second = check();
        getCollectorStatus = Status.NOT_FOUND.withDescription("collector is deleted");
        String third = check();
        getCollectorStatus = Status.OK;
        String fourth = check();

        assertTrue(first.contains("WARN") && first.contains("Factry refused the collector token"), first);
        assertEquals("", second, "same reason, no new line");
        assertTrue(third.contains("WARN") && third.contains("deleted in Factry"), third);
        assertTrue(fourth.contains("INFO") && fourth.contains("answers the connection check again"), fourth);
        assertSame(ProfileStatus.RUNNING, health.status());
    }
}
