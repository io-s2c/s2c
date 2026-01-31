package io.s2c.logging;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.event.Level;
import org.slf4j.spi.LoggingEventBuilder;

public class StructuredLogger {

  private final LoggingContext loggingContext;

  private final Logger logger;

  public StructuredLogger(Logger logger, LoggingContext loggingContext) {
    this.logger = logger;
    this.loggingContext = loggingContext;
  }

  public UncaughtExceptionHandler uncaughtExceptionLogger() {
    return (Thread thread, Throwable e) -> e.printStackTrace();
  }

  public LoggingEventBuilder info() {
    return log(Level.INFO);
  }

  public LoggingEventBuilder error() {
    return log(Level.ERROR);
  }

  public LoggingEventBuilder warn() {
    return log(Level.WARN);
  }

  public LoggingEventBuilder debug() {
    return log(Level.DEBUG);
  }

  public LoggingEventBuilder trace() {
    return log(Level.TRACE);
  }

  private LoggingEventBuilder log(Level level) {
    LoggingEventBuilder builder = logger.atLevel(level);
    if (!logger.isEnabledForLevel(level)) {
      return builder; // NOPLoggingEventBuilder
    }
    return loggingContext.enrich(builder);
  }
}
