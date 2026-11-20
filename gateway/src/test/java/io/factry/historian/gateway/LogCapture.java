package io.factry.historian.gateway;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Collects what reaches one logger while it is open, the way the gateway's logback would write
 * it. The logger's own appenders (the console) are detached meanwhile, so nothing captured is
 * printed, and its level, appenders and additivity are put back on close.
 */
final class LogCapture implements AutoCloseable {

    private final Logger logger;
    private final Level originalLevel;
    private final boolean originalAdditive;
    private final List<Appender<ILoggingEvent>> originalAppenders = new ArrayList<>();
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    /** Captures {@code loggerName} (or {@link Logger#ROOT_LOGGER_NAME}); a null level keeps its level. */
    LogCapture(String loggerName, Level level) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        logger = context.getLogger(loggerName);
        originalLevel = logger.getLevel();
        originalAdditive = logger.isAdditive();
        for (Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders(); it.hasNext(); ) {
            originalAppenders.add(it.next());
        }
        originalAppenders.forEach(logger::detachAppender);
        appender.setContext(context);
        appender.start();
        logger.addAppender(appender);
        logger.setAdditive(false);
        if (level != null) {
            logger.setLevel(level);
        }
    }

    List<ILoggingEvent> events() {
        return List.copyOf(appender.list);
    }

    /** Every captured message, one per line. */
    String text() {
        return events().stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.joining("\n"));
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        appender.stop();
        originalAppenders.forEach(logger::addAppender);
        logger.setAdditive(originalAdditive);
        logger.setLevel(originalLevel);
    }
}
