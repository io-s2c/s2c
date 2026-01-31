package io.s2c;

import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.concurrency.TaskExecutor;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.error.ObjectCorruptedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.LogEntriesBatch;
import io.s2c.s3.ObjectReader;
import io.s2c.s3.ObjectWriter;
import io.s2c.util.KeysResolver;
import io.s2c.util.LRUCache;

public class S2CLog implements AutoCloseable {

  private final Logger logger = LoggerFactory.getLogger(S2CLog.class);

  final StructuredLogger log;

  private final LRUCache<String, LogEntriesBatch> lruCache;
  private final KeysResolver keysResolver;
  private final ObjectReader objectReader;
  private final ObjectWriter objectWriter;
  private final TaskExecutor taskExecutor;
  private final S2COptions s2cOptions;

  private Timer appendLatency;
  private Timer truncateLatency;

  private Counter concurrentStateModificationErrors;
  private Counter logReadCounts;
  private Counter cacheMissCount;

  public S2CLog(Function<StructuredLogger, ObjectReader> objectReaderFactory,
      Function<StructuredLogger, ObjectWriter> objectWriterFactory,
      ContextProvider contextProvider,
      S2COptions s2cOptions,

      MeterRegistry meterRegistry) {

    this.keysResolver = new KeysResolver(contextProvider.s2cGroupId());
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.lruCache = new LRUCache<>(s2cOptions.logLruCacheSize(), this::getDurableBatch);
    this.taskExecutor = new TaskExecutor(contextProvider.ownerName(S2CLog.class),
        log.uncaughtExceptionLogger(), meterRegistry);
    this.objectReader = objectReaderFactory.apply(log);
    this.objectWriter = objectWriterFactory.apply(log);
    this.s2cOptions = s2cOptions;
    appendLatency = Timer.builder("s2c.log.append.latency").register(meterRegistry);
    truncateLatency = Timer.builder("s2c.log.truncate.latency").register(meterRegistry);

    concurrentStateModificationErrors = Counter.builder("s2c.log.append")
        .tag("outcome", "concurrent.state.modification")
        .tag("s2cGroupId", contextProvider.s2cGroupId())
        .register(meterRegistry);

    logReadCounts = Counter.builder("s2c.log.read.count").register(meterRegistry);

    cacheMissCount = Counter.builder("s2c.log.cache.miss").register(meterRegistry);

  }

  public void append(LogEntriesBatch logEntriesBatch, long index, LeaderState leaderState)
      throws ConcurrentStateModificationException, InterruptedException {
    String entryKey = entryKeyAt(index);
    Timer.Sample sample = Timer.start();

    Optional<String> etag = objectWriter.writeIfNoneMatch(entryKey,
        logEntriesBatch.toByteString());

    sample.stop(appendLatency);

    if (etag.isPresent()) {
      lruCache.put(entryKey, logEntriesBatch);
    } else {
      concurrentStateModificationErrors.increment();
      throw new ConcurrentStateModificationException(leaderState);
    }

  }

  public void truncate(long start, long end) throws InterruptedException {

    Timer.Sample sample = Timer.start();
    while (start <= end) {
      String key = keysResolver.logEntryKey(start);
      objectWriter.delete(key);
      start++;
    }
    sample.stop(truncateLatency);
  }

  public String entryKeyAt(long index) {
    return keysResolver.logEntryKey(index);
  }

  public Optional<LogEntriesBatch> getBatchAt(String key) {
    logReadCounts.increment();
    if (s2cOptions.logLruCacheSize() <= 0) {
      return Optional.ofNullable(getDurableBatch(key));
    }
    return Optional.ofNullable(lruCache.get(key));
  }

  private LogEntriesBatch getDurableBatch(String key) {
    LogEntriesBatch result = null;
    try {
      cacheMissCount.increment();
      var resultOptional = this.objectReader.read(key, LogEntriesBatch.parser());
      if (resultOptional.isPresent()) {
        result = resultOptional.get().object();
      }
    } catch (ObjectCorruptedException e) {
      throw new IllegalStateException("Object couldn't be read", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug().setCause(e).log("Interrupted");
    }
    return result;
  }

  public LogReplayer replay(Long from, Long to) {
    LogReplayer replayer = new LogReplayer(from, to, this::getBatch);
    taskExecutor.start("log-replayer", replayer);
    return replayer;
  }

  private LogEntriesBatch getBatch(Long index) {
    return getBatchAt(entryKeyAt(index)).orElse(null);
  }

  public void close() {
    taskExecutor.close();
  }
}