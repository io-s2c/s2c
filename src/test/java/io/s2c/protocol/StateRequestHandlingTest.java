package io.s2c.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.ContextProvider;
import io.s2c.LeaderStateManager;
import io.s2c.OrderedLastResult;
import io.s2c.S2CLog;
import io.s2c.StateRequestHandler;
import io.s2c.concurrency.GuardedValue;
import io.s2c.configs.S2COptions;
import io.s2c.error.RequestOutOfSequenceException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.messages.StateRequest;
import io.s2c.model.messages.StateRequest.StateRequestType;
import io.s2c.model.state.LastResult;
import io.s2c.model.state.LogEntriesBatch;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.ClientRole;
import io.s2c.network.S2CClient;
import io.s2c.network.message.reader.S2CMessageReader;
import io.s2c.s3.ObjectReader;
import io.s2c.s3.ObjectWriter;
import io.s2c.s3.S3Facade;
import io.s2c.util.KeysResolver;
import io.s2c.util.LRUCache;

@Tag("protocol")
class StateRequestHandlingTest {

  private StateRequestHandler stateRequestHandler;
  private LeaderStateManager leaderStateManager;

  private NodeIdentity nodeIdentity = NodeIdentity.newBuilder()
      .setAddress("localhost")
      .setPort(7777)
      .build();

  private S2CLog s2cLog;

  private S2COptions s2cOptions = new S2COptions();
  private final String bucket = "s2cbucket";
  private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
  private final ContextProvider contextProvider = new ContextProvider("group", nodeIdentity, false);
  private final AtomicLong stateMachine = new AtomicLong();
  private final AtomicLong applyIndex = new AtomicLong();

  private final GuardedValue<LRUCache<NodeIdentity, OrderedLastResult>> guardedValuenodesLastResults = new GuardedValue<LRUCache<NodeIdentity, OrderedLastResult>>(
      new LRUCache<>(1000, n -> null));

  @BeforeEach
  void setUp() {

    S3Facade s3Facade = S3Facade.createNewInMemory(bucket);

    Function<StructuredLogger, ObjectReader> objectReaderFactory = l -> new ObjectReader(s3Facade,
        bucket, l, s2cOptions.s2cRetryOptions());
    Function<StructuredLogger, ObjectWriter> objectWriterFactory = l -> new ObjectWriter(s3Facade,
        bucket, l, s2cOptions.s2cRetryOptions());

    leaderStateManager = new LeaderStateManager(objectReaderFactory, objectWriterFactory,
        s2cOptions, contextProvider,
        () -> new S2CClient(s2cOptions, contextProvider, meterRegistry, r -> {
        }, () -> S2CMessageReader.create(s2cOptions.maxMessageSize()), ClientRole.IS_ALIVE_CHECKER),
        () -> 0L, meterRegistry);

    s2cLog = new S2CLog(objectReaderFactory, objectWriterFactory, contextProvider, new S2COptions(), meterRegistry);

    stateRequestHandler = new StateRequestHandler(contextProvider, s2cOptions.flushIntervalMs(),
        s2cOptions.batchMinCount(), leaderStateManager, l -> {
        }, b -> {
          b.batch().forEach(r -> {
            if (r.reqRes().request().getType().equals(StateRequestType.COMMAND)) {
              stateMachine.incrementAndGet();
            }

            r.reqRes().response(ByteString.EMPTY);

            LastResult newLastResult = LastResult.newBuilder()
                .setLastSeqNum(r.reqRes().request().getSequenceNumber())
                .setResult(ByteString.EMPTY)
                .build();

            guardedValuenodesLastResults.write(nodesLastResults -> {
              OrderedLastResult currentLastResult = nodesLastResults
                  .get(r.reqRes().request().getSourceNode());
              if (currentLastResult != null) {
                if (currentLastResult.lastResult().getLastSeqNum() < newLastResult
                    .getLastSeqNum()) {
                  nodesLastResults.put(r.reqRes().request().getSourceNode(),
                      new OrderedLastResult(newLastResult));
                }
              } else {
                nodesLastResults.put(r.reqRes().request().getSourceNode(),
                    new OrderedLastResult(newLastResult));
              }
              return nodesLastResults;
            });

          });
          if (b.requestsType() == StateRequestType.COMMAND) {
            applyIndex.incrementAndGet();
          }
          return applyIndex.get();
        }, s2cLog, leaderStateManager::handleConcurrentStateModificationException,
        guardedValuenodesLastResults, meterRegistry);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    if (stateRequestHandler != null) {
      stateRequestHandler.close();
    }
    if (leaderStateManager != null) {
      leaderStateManager.close();
    }
    if (s2cLog != null) {
      s2cLog.close();
    }
  }

  @Test
  void testHandling() throws InterruptedException, S2CStoppedException, ExecutionException {

    Thread t3 = new Thread(stateRequestHandler);

    stateRequestHandler.init();

    t3.start();

    long time = System.currentTimeMillis();
    AtomicLong seqNum = new AtomicLong();

    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
      Future<?> commands = executorService.submit(() -> {
        int i = 0;
        while (i < 20) {
          StateRequest stateRequest = StateRequest.newBuilder()
              .setBody(ByteString.EMPTY)
              .setType(StateRequestType.COMMAND)
              .setSequenceNumber(seqNum.incrementAndGet())
              .build();

          executorService.execute(() -> {
            assertDoesNotThrow(() -> {
              while (true) {
                try {
                  stateRequestHandler.handle(UUID.randomUUID().toString(), stateRequest);
                  break;
                } catch (RequestOutOfSequenceException e) {
                  TimeUnit.MILLISECONDS.sleep(stateRequest.getSequenceNumber());
                  continue;
                }
              }

            });

          });
          i++;
        }
      });

      Future<?> reads = executorService.submit(() -> {
        int i = 0;
        while (i < 20) {
          StateRequest stateRequest = StateRequest.newBuilder()
              .setBody(ByteString.EMPTY)
              .setType(StateRequestType.READ)
              .build();
          executorService.execute(() -> {
            assertDoesNotThrow(() -> {
              stateRequestHandler.handle(UUID.randomUUID().toString(), stateRequest);
            });
          });
          i++;
        }
      });

      reads.get();
      commands.get();

    }
    stateRequestHandler.close();
    t3.interrupt();
    long commitIndex = leaderStateManager.getLeaderState().getCommitIndex();
    
    assertEquals(meterRegistry.find("state.request.committed.batches.count").counter().count(),
        commitIndex);

    assertEquals(20, stateMachine.get());
    int i = 1;
    KeysResolver keysResolver = new KeysResolver(contextProvider.s2cGroupId());
    while (i <= commitIndex) {
      Optional<LogEntriesBatch> batch = s2cLog.getBatchAt(keysResolver.logEntryKey(i));
      assertEquals(true, batch.isPresent());
      i++;
    }
  }
}