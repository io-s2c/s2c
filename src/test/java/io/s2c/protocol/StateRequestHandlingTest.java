package io.s2c.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.ContextProvider;
import io.s2c.DedupUnit;
import io.s2c.LeaderStateManager;
import io.s2c.RSM;
import io.s2c.S2CLog;
import io.s2c.StateRequestHandler;
import io.s2c.concurrency.GuardedValue;
import io.s2c.configs.S2COptions;
import io.s2c.error.ApplicationResultUnavailableException;
import io.s2c.error.RequestOutOfSequenceException;
import io.s2c.error.S2CStoppedException;
import io.s2c.error.StateRequestException;
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

  private final String bucket = "s2cbucket";
  private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
  private final ContextProvider contextProvider = new ContextProvider("group", nodeIdentity, false);
  private final AtomicInteger stateMachine = new AtomicInteger();
  private final AtomicLong applyIndex = new AtomicLong();

  private final StringBuilder seqNumsStr = new StringBuilder();
  
  private final GuardedValue<LRUCache<NodeIdentity, DedupUnit>> guardedDedupUnits = new GuardedValue<LRUCache<NodeIdentity, DedupUnit>>(
      new LRUCache<>(1000, n -> null));

  @BeforeEach
  void setUp() {

    S3Facade s3Facade = S3Facade.createNewInMemory(bucket);

    S2COptions s2cOptions = new S2COptions();
    s2cOptions.s2cExactlyOnceOptions().outOfSeqBufferSize(20);

    Function<StructuredLogger, ObjectReader> objectReaderFactory = l -> new ObjectReader(s3Facade,
        bucket, l, s2cOptions.s2cRetryOptions());
    Function<StructuredLogger, ObjectWriter> objectWriterFactory = l -> new ObjectWriter(s3Facade,
        bucket, l, s2cOptions.s2cRetryOptions());

    leaderStateManager = new LeaderStateManager(objectReaderFactory, objectWriterFactory,
        s2cOptions, contextProvider,
        () -> new S2CClient(s2cOptions, contextProvider, meterRegistry, r -> {
        }, () -> S2CMessageReader.create(s2cOptions.maxMessageSize()), ClientRole.IS_ALIVE_CHECKER),
        () -> 0L, meterRegistry);

    s2cLog = new S2CLog(objectReaderFactory, objectWriterFactory, contextProvider, s2cOptions,
        meterRegistry);

    stateRequestHandler = new StateRequestHandler(contextProvider, s2cOptions.flushIntervalMs(),
        s2cOptions.batchMinCount(), leaderStateManager, l -> {
        }, b -> {
          b.batch().forEach(r -> {
            if (r.reqRes().request().getType() == StateRequestType.COMMAND) {
              stateMachine.incrementAndGet();
              seqNumsStr.append(stateMachine.get() + "-");
            }
            ByteString result = ByteString
                .copyFrom(ByteBuffer.allocate(Integer.SIZE).putInt(stateMachine.get()).flip());
            LastResult newLastResult = LastResult.newBuilder()
                .setLastSeqNum(r.reqRes().request().getSequenceNumber())
                .setResult(result)
                .setErrMsg(RSM.NO_ERR_MSG)
                .build();

            guardedDedupUnits.write(dedupUnits -> {
              var dedupUnit = dedupUnits.get(r.reqRes().request().getSourceNode());
              if (dedupUnit != null) {
                if (dedupUnit.lastResult().getLastSeqNum() < newLastResult.getLastSeqNum()) {
                  dedupUnits.put(r.reqRes().request().getSourceNode(),
                      new DedupUnit(newLastResult, dedupUnit.eosBuffer()));
                }
              } else {
                dedupUnits.put(r.reqRes().request().getSourceNode(),
                    new DedupUnit(newLastResult, null));
              }
              return dedupUnits;
            });
            r.reqRes().response(result);

          });
          if (b.requestsType() == StateRequestType.COMMAND) {
            applyIndex.incrementAndGet();
          }

          return applyIndex.get();
        }, s2cLog, leaderStateManager::handleConcurrentStateModificationException,
        guardedDedupUnits, s2cOptions.s2cExactlyOnceOptions(), meterRegistry);
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

    long shouldNextSeqNum = -1;

    StateRequest outsideSlidingWindowRequest = StateRequest.newBuilder()
        .setSourceNode(nodeIdentity)
        .setBody(ByteString.EMPTY)
        .setType(StateRequestType.COMMAND)
        .setSequenceNumber(21)
        .build();

    try {
      stateRequestHandler.handle(UUID.randomUUID().toString(), outsideSlidingWindowRequest);
    }
    catch (StateRequestException | InterruptedException | RequestOutOfSequenceException e) {
      if (e instanceof RequestOutOfSequenceException re) {
        shouldNextSeqNum = re.nextSeqNum();
      }
    }

    assertEquals(1, shouldNextSeqNum);

    AtomicLong seqNum = new AtomicLong();
    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      int i = 0;

      while (i < 20) {
        StateRequest.Builder stateRequestBuilder = StateRequest.newBuilder()
            .setSourceNode(nodeIdentity)
            .setBody(ByteString.EMPTY)
            .setType(StateRequestType.COMMAND);

        executorService.submit(() -> {
          assertDoesNotThrow(() -> {
            // We set the seq num only when the request is actually sent, to prevent deterministic
            // order of submission.. Also we send in reverse order
            var stateRequest = stateRequestBuilder.setSequenceNumber(20 - seqNum.getAndIncrement())
                .build();
            stateRequestHandler.handle(UUID.randomUUID().toString(), stateRequest);
          });
        });
        i++;
      }

      int j = 0;
      while (j < 20) {
        StateRequest stateRequest = StateRequest.newBuilder()
            .setBody(ByteString.EMPTY)
            .setSourceNode(nodeIdentity)
            .setType(StateRequestType.READ)
            .build();

        executorService.submit(() -> {
          assertDoesNotThrow(() -> {
            stateRequestHandler.handle(UUID.randomUUID().toString(), stateRequest);
          });

        });
        j++;
      }

    }

    StateRequest resultUnavailableRequest = StateRequest.newBuilder()
        .setSourceNode(nodeIdentity)
        .setBody(ByteString.EMPTY)
        .setType(StateRequestType.COMMAND)
        .setSequenceNumber(19)
        .build();

    assertThrows(ApplicationResultUnavailableException.class, () -> {
      stateRequestHandler.handle(UUID.randomUUID().toString(), resultUnavailableRequest);
    });


    StateRequest lastResultRequest = StateRequest.newBuilder()
        .setSourceNode(nodeIdentity)
        .setBody(ByteString.EMPTY)
        .setType(StateRequestType.COMMAND)
        .setSequenceNumber(20)
        .build();

    final long stateMachineValue = stateMachine.get();
    long applyIndexBefore = applyIndex.get();
    // This should not throw but also not cause a new application
    AtomicReference<ByteString> resultRef = new AtomicReference<>();
    assertDoesNotThrow(() -> {
      ByteString result = stateRequestHandler.handle(UUID.randomUUID().toString(),
          lastResultRequest);
      resultRef.set(result);
    });

    // Assert state machine value was not changed
    assertEquals(stateMachineValue, stateMachine.get());
    // Assert applyIndex was not incremented
    assertEquals(applyIndexBefore, applyIndex.get());
    // Assert if we retry the last request, we receive the cached result
    assertEquals(20, resultRef.get().asReadOnlyByteBuffer().getInt());
    
    stateRequestHandler.close();
    t3.interrupt();
    long commitIndex = leaderStateManager.getLeaderState().getCommitIndex();

    assertEquals(meterRegistry.find("state.request.committed.batches.count").counter().count(),
        commitIndex);

    assertEquals(20, stateMachine.get());
    int ii = 1;
    KeysResolver keysResolver = new KeysResolver(contextProvider.s2cGroupId());
    while (ii <= commitIndex) {
      Optional<LogEntriesBatch> batch = s2cLog.getBatchAt(keysResolver.logEntryKey(ii));
      assertEquals(true, batch.isPresent());
      ii++;
    }
    Optional<LogEntriesBatch> batch = s2cLog.getBatchAt(keysResolver.logEntryKey(ii));
    assertEquals(false, batch.isPresent()); // 21 should not exist
    
    StringBuilder expectedSeqNumOrdering = new StringBuilder();
    
    int seq = 1;
    while (seq <= 20) {
      expectedSeqNumOrdering.append(seq + "-");
      seq++;
    }
    assertEquals(expectedSeqNumOrdering.toString(), seqNumsStr.toString());
  }
}