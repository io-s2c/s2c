package io.s2c;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.LogReplayer.ReplayerBrokenException;
import io.s2c.configs.S2COptions;
import io.s2c.configs.S2CRetryOptions;
import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.LogEntriesBatch;
import io.s2c.model.state.LogEntry;
import io.s2c.model.state.NodeIdentity;
import io.s2c.s3.ObjectReader;
import io.s2c.s3.ObjectWriter;
import io.s2c.s3.S3Facade;

class S2CLogTest {

  private S2CLog s2cLog;
  private S3Facade s3Facade;
  private ContextProvider contextProvider;
  private MeterRegistry meterRegistry;
  private final String bucket = "test-bucket";

  @AfterEach
  void tearDown() {
    if (s2cLog != null) {
      s2cLog.close();
    }
  }

  @BeforeEach
  void setUp() {
    s3Facade = S3Facade.createNewInMemory(bucket);
    contextProvider = new ContextProvider("test-group",
        NodeIdentity.newBuilder().setAddress("localhost").setPort(7777).build(), false);
    meterRegistry = new SimpleMeterRegistry();

    Function<StructuredLogger, ObjectReader> objectReaderFactory = log -> new ObjectReader(s3Facade,
        bucket, log, new S2CRetryOptions());
    Function<StructuredLogger, ObjectWriter> objectWriterFactory = log -> new ObjectWriter(s3Facade,
        bucket, log, new S2CRetryOptions());

    s2cLog = new S2CLog(objectReaderFactory, objectWriterFactory, contextProvider, new S2COptions().logLruCacheSize(0),
        meterRegistry);
  }

  @Test
  void testAppendMultipleEntries()
      throws ConcurrentStateModificationException, InterruptedException {
    LeaderState leaderState = createLeaderState(0, 0);

    for (int i = 1; i <= 5; i++) {
      LogEntriesBatch batch = createBatch(i);
      s2cLog.append(batch, i, leaderState);
      Optional<LogEntriesBatch> retrieved = s2cLog.getBatchAt(s2cLog.entryKeyAt(i));
      assertTrue(retrieved.isPresent());
      assertEquals(retrieved.get(), batch);
    }
  }

  @Test
  void testAppendConcurrentStateModification()
      throws ConcurrentStateModificationException, InterruptedException {
    LogEntriesBatch batch = createBatch(1);
    LeaderState leaderState = createLeaderState(0, 0);

    // First append succeeds
    s2cLog.append(batch, 1, leaderState);

    // Second append to same index should fail
    assertThrows(ConcurrentStateModificationException.class, () -> {
      s2cLog.append(batch, 1, leaderState);
    });
  }

  @Test
  void testTruncate() throws ConcurrentStateModificationException, InterruptedException {
    LeaderState leaderState = createLeaderState(0, 0);

    // Append entries 1-10
    for (int i = 1; i <= 10; i++) {
      LogEntriesBatch batch = createBatch(i);
      s2cLog.append(batch, i, leaderState);
    }

    // Truncate entries 1-5
    s2cLog.truncate(1, 5);

    // Entries 1-5 should be gone
    for (int i = 1; i <= 5; i++) {
      Optional<LogEntriesBatch> retrieved = s2cLog.getBatchAt(s2cLog.entryKeyAt(i));
      assertFalse(retrieved.isPresent());
    }

    // Entries 6-10 should still exist
    for (int i = 6; i <= 10; i++) {
      Optional<LogEntriesBatch> retrieved = s2cLog.getBatchAt(s2cLog.entryKeyAt(i));
      assertTrue(retrieved.isPresent());
    }
  }

  @Test
  void testTruncateSingleEntry() throws ConcurrentStateModificationException, InterruptedException {
    LeaderState leaderState = createLeaderState(0, 0);

    LogEntriesBatch batch = createBatch(1);
    s2cLog.append(batch, 1, leaderState);

    s2cLog.truncate(1, 1);

    Optional<LogEntriesBatch> retrieved = s2cLog.getBatchAt(s2cLog.entryKeyAt(1));
    assertFalse(retrieved.isPresent());
  }

  @Test
  void testGetBatchAtNonExistent() {
    String key = s2cLog.entryKeyAt(999);
    Optional<LogEntriesBatch> retrieved = s2cLog.getBatchAt(key);
    assertFalse(retrieved.isPresent());
  }

  @Test
  void testCacheMiss() throws ConcurrentStateModificationException, InterruptedException {
    LeaderState leaderState = createLeaderState(0, 0);

    LogEntriesBatch batch = createBatch(1);
    s2cLog.append(batch, 1, leaderState);

    // Clear cache by creating new S2CLog instance
    Function<StructuredLogger, ObjectReader> objectReaderFactory = log -> new ObjectReader(s3Facade,
        bucket, log, new S2CRetryOptions());
    Function<StructuredLogger, ObjectWriter> objectWriterFactory = log -> new ObjectWriter(s3Facade,
        bucket, log, new S2CRetryOptions());

    // Should still be able to retrieve from S3
    try (S2CLog newLog = new S2CLog(objectReaderFactory, objectWriterFactory, contextProvider,
        new S2COptions(), meterRegistry)) {
      Optional<LogEntriesBatch> retrieved = newLog.getBatchAt(newLog.entryKeyAt(1));
      assertTrue(retrieved.isPresent());
    }
  }

  @Test
  void testReplay() throws ConcurrentStateModificationException, InterruptedException, ReplayerBrokenException {
    LeaderState leaderState = createLeaderState(0, 0);

    // Append entries 1-5
    for (int i = 1; i <= 5; i++) {
      LogEntriesBatch batch = createBatch(i);
      s2cLog.append(batch, i, leaderState);
    }

    LogReplayer replayer = s2cLog.replay(1L, 5L);

    var next = replayer.next();

    int i = 0;
    while (next != null) {
      next = replayer.next();
      i++;
    }

    assertEquals(5, i);

    replayer.close();
  }

  private LogEntriesBatch createBatch(int index) {
    LogEntry entry = LogEntry.newBuilder()
        .setBody(ByteString.copyFromUtf8("data-" + index))
        .build();

    return LogEntriesBatch.newBuilder().setCommitIndex(index).addLogEntries(entry).build();
  }

  private LeaderState createLeaderState(long commitIndex, long epoch) {
    return LeaderState.newBuilder()
        .setNodeIdentity(contextProvider.nodeIdentity())
        .setCommitIndex(commitIndex)
        .setEpoch(epoch)
        .build();
  }
  
  
}
