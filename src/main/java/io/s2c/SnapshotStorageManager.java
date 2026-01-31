package io.s2c;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.error.ObjectCorruptedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.StateSnapshot;
import io.s2c.s3.ObjectReader;
import io.s2c.s3.ObjectWriter;
import io.s2c.s3.ReadObjectResult;
import io.s2c.util.KeysResolver;

public class SnapshotStorageManager {

  private final Logger logger = LoggerFactory.getLogger(SnapshotStorageManager.class);

  private final StructuredLogger log;

  private final KeysResolver keysResolver;
  private final ObjectReader objectReader;
  private final ObjectWriter objectWriter;
  private volatile String snapshotETag;
  private final AtomicLong currentSnapshotApplyIndex = new AtomicLong();
  private final ReentrantLock snapshotLock = new ReentrantLock();
  private Timer uploadLatency;
  private Timer downloadLatency;
  private final ContextProvider contextProvider;

  public SnapshotStorageManager(Function<StructuredLogger, ObjectReader> objectReaderFactory,
      Function<StructuredLogger, ObjectWriter> objectWriterFactory,
      ContextProvider contextProvider,
      MeterRegistry meterRegistry) {
    this.keysResolver = new KeysResolver(contextProvider.s2cGroupId());
    this.log = new StructuredLogger(logger, contextProvider.loggingContext());
    this.objectReader = objectReaderFactory.apply(log);
    this.objectWriter = objectWriterFactory.apply(log);
    this.uploadLatency = Timer.builder("snapshot.upload.latency").register(meterRegistry);
    this.downloadLatency = Timer.builder("snapshot.download.latency").register(meterRegistry);
    this.contextProvider = contextProvider;
  }

  public Optional<StateSnapshot> download() throws InterruptedException {
    snapshotLock.lock();
    try {
      Timer.Sample sample = Timer.start();
      Optional<ReadObjectResult<StateSnapshot>> resultOptional = objectReader
          .read(keysResolver.stateSnapshotKey(), StateSnapshot.parser());
      sample.stop(downloadLatency);
      if (resultOptional.isPresent()) {
        snapshotETag = resultOptional.get().eTag();

        currentSnapshotApplyIndex.set(resultOptional.get().object().getEndApplyIndex());
        return Optional.of(resultOptional.get().object());
      } else {
        return Optional.empty();
      }
    } catch (ObjectCorruptedException e) {
      throw new IllegalStateException("Snapshot couldn't be read", e);
    } finally {
      snapshotLock.unlock();
    }
  }

  public void upload(StateSnapshot snapshot, LeaderState leaderState)
      throws ConcurrentStateModificationException, InterruptedException {
    Optional<String> newEtag;
    snapshotLock.lock();
    try {
      Timer.Sample sample = Timer.start();
      if (snapshotETag != null) {
        newEtag = objectWriter.writeIfMatch(keysResolver.stateSnapshotKey(),
            snapshot.toByteString(), snapshotETag);
      } else {
        newEtag = objectWriter.writeIfNoneMatch(keysResolver.stateSnapshotKey(),
            snapshot.toByteString());
      }
      sample.stop(uploadLatency);
      if (newEtag.isPresent()) {

        snapshotETag = newEtag.get();

        currentSnapshotApplyIndex.set(snapshot.getEndApplyIndex());

      } else {
        throw new ConcurrentStateModificationException(leaderState);
      }
    } finally {
      snapshotLock.unlock();
    }

  }

  public long currentSnapshotApplyIndex() {
    return currentSnapshotApplyIndex.get();
  }

}