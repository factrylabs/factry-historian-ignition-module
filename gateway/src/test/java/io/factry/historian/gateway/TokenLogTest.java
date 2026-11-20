package io.factry.historian.gateway;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Netty's HTTP/2 frame logger writes every request header at DEBUG, the collector token included.
 * An admin who turns on DEBUG for gRPC or for everything while chasing a problem must not get the
 * token in the gateway log (and in every support bundle made from it).
 */
class TokenLogTest {

    private static final String FRAME_LOGGER = "io.grpc.netty.shaded.io.grpc.netty.NettyClientHandler";
    private static final String SERVER_FRAME_LOGGER = "io.grpc.netty.shaded.io.grpc.netty.NettyServerHandler";
    /** Not a real token; the signature part is what must never show up. */
    private static final String SIGNATURE = "fakeSignatureThatMustStayOutOfTheLog";
    private static final String TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0." + SIGNATURE;

    private final LoggerContext logback = (LoggerContext) LoggerFactory.getILoggerFactory();
    private final Logger frameLogger = logback.getLogger(FRAME_LOGGER);
    private final Logger serverFrameLogger = logback.getLogger(SERVER_FRAME_LOGGER);
    private Level frameLevelBefore;
    private FakeFactryServer factry;

    @BeforeEach
    void freshGateway() throws Exception {
        // As on a gateway where nobody set this logger: a client made by an earlier test must not
        // decide the outcome.
        frameLevelBefore = frameLogger.getLevel();
        frameLogger.setLevel(null);
        // The fake Factry's own frame logger also prints the header it receives; the gateway runs
        // no gRPC server, so only the client side counts.
        serverFrameLogger.setLevel(Level.INFO);
        factry = new FakeFactryServer();
    }

    @AfterEach
    void restore() throws Exception {
        factry.close();
        frameLogger.setLevel(frameLevelBefore);
        serverFrameLogger.setLevel(null);
    }

    @ParameterizedTest
    @ValueSource(strings = {"io.grpc", Logger.ROOT_LOGGER_NAME})
    void broadDebugLevelDoesNotLogTheToken(String loggerName) {
        String log;
        try (LogCapture capture = new LogCapture(loggerName, Level.DEBUG)) {
            getCollectors();
            log = capture.text();
            // The rest of gRPC's Netty still logs at DEBUG.
            assertTrue(logback.getLogger("io.grpc.netty.shaded.io.netty").isDebugEnabled());
        }
        assertFalse(log.contains(SIGNATURE), "the token is in the log with DEBUG on " + loggerName);
    }

    @Test
    void debugSetOnTheFrameLoggerItselfIsKept() {
        try (LogCapture capture = new LogCapture(FRAME_LOGGER, Level.DEBUG)) {
            getCollectors();
            assertEquals(Level.DEBUG, frameLogger.getLevel());
            assertTrue(capture.text().contains("OUTBOUND HEADERS"), "an admin who asks for frames gets them");
        }
    }

    @Test
    void levelClearedByALogbackReloadIsSetAgainBeforeTheNextCall() {
        FactryGrpcClient client = new FactryGrpcClient(InetAddress.getLoopbackAddress().getHostAddress(),
                factry.port(), "collector", TOKEN, false, false, null);
        try {
            client.getCollectors();
            frameLogger.setLevel(null); // what a reload does to a level set in code
            String log;
            try (LogCapture capture = new LogCapture("io.grpc", Level.DEBUG)) {
                client.getCollectors();
                log = capture.text();
            }
            assertFalse(log.contains(SIGNATURE), "the token is in the log after a logback reload");
        } finally {
            client.shutdown();
        }
    }

    private void getCollectors() {
        FactryGrpcClient client = new FactryGrpcClient(InetAddress.getLoopbackAddress().getHostAddress(),
                factry.port(), "collector", TOKEN, false, false, null);
        try {
            client.getCollectors();
        } finally {
            client.shutdown();
        }
    }
}
