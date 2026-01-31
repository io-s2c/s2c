package io.s2c.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.S2CGroupRegistry;
import io.s2c.S2CNode;
import io.s2c.TestUtil;
import io.s2c.configs.S2COptions;
import io.s2c.configs.S2CRetryOptions;
import io.s2c.error.ApplicationException;
import io.s2c.error.S2CStoppedException;
import io.s2c.model.messages.S2CMessage;
import io.s2c.model.state.NodeIdentity;
import io.s2c.model.state.StateSnapshot;
import io.s2c.network.S2CServer;
import io.s2c.network.message.reader.Failure;
import io.s2c.network.message.reader.S2CMessageReader;
import io.s2c.s3.GetObjectResult;
import io.s2c.s3.S3Error;
import io.s2c.s3.S3Facade;
import io.s2c.teststatemachines.CounterStateMachine;
import io.s2c.teststatemachines.StringAppender;
import io.s2c.util.KeysResolver;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Tag("protocol")
@Tag("chaos")
@Tag("slow")
class ConsensusTest {

  private final Logger log = LoggerFactory.getLogger(ConsensusTest.class);

  String bucket = "bucket";
  String group = "group";

  S3Facade s3Facade;

  MeterRegistry meterRegistry1 = new SimpleMeterRegistry();

  MeterRegistry meterRegistry2 = new SimpleMeterRegistry();

  NodeIdentity nodeIdentity1 = NodeIdentity.newBuilder()
      .setAddress("127.0.0.1")
      .setPort(7777)
      .build();
  NodeIdentity nodeIdentity2 = NodeIdentity.newBuilder()
      .setAddress("127.0.0.1")
      .setPort(8888)
      .build();
  S2CNode s2cNode1;
  S2CServer s2cServer1;
  CounterStateMachine counterStateMachine1;
  StringAppender appenderStateMachine1;
  S2CGroupRegistry s2cGroupRegistry1 = new S2CGroupRegistry();
  S2COptions s2cOptions = new S2COptions()
      .s2cRetryOptions(new S2CRetryOptions().baseDelayMS(100).maxDelaySeconds(1))
      .maxMissedHeartbeats(1000) // We don't want to trigger state transition while dropping
                                 // messages
      .logNodeIdentity(true)
      .snapshottingThreshold(10);
  S2CNode s2cNode2;
  S2CServer s2cServer2;
  CounterStateMachine counterStateMachine2;
  StringAppender appenderStateMachine2;
  S2CGroupRegistry s2cGroupRegistry2 = new S2CGroupRegistry();

  private enum StateMachine {
    COUNTER, APPENDER, COUNTER_AND_APPENDER
  }

  @AfterEach
  void tearDown() throws InterruptedException, UnsupportedOperationException, IOException {
    if (s2cNode1 != null) {
      shutdownNode1();
    }
    if (s2cNode2 != null) {
      shutdownNode2();
    }
    TestUtil.cleanupBucket(bucket);
  }

  @AfterAll
  static void stopContainer() {
    TestUtil.stopLocalStack();
  }

  @BeforeEach
  void setUp(TestInfo testInfo) throws UnsupportedOperationException {
    log.info("started test: {}", testInfo.getDisplayName());
    s3Facade = TestUtil.createS3Facade(bucket);
  }

  private Supplier<S2CMessageReader> newChaosS2CMessageReaderFactory(int maxMessageSize,
      Supplier<Integer> failEvery, Set<Failure> failures, Consumer<S2CMessage> messageInspector) {
    return () -> {
      S2CMessageReader s2cMessageReader = S2CMessageReader.create(maxMessageSize);
      return S2CMessageReader.wrapForTest(s2cMessageReader, failEvery, failures, messageInspector);
    };
  }

  private Supplier<S2CMessageReader> newS2CMessageReaderFactory(int maxMessageSize) {
    return () -> S2CMessageReader.create(maxMessageSize);
  }

  private void initAndStartNode1(Supplier<S2CMessageReader> messageReaderFactory,
      StateMachine stateMachine) throws IOException, InterruptedException {
    s2cServer1 = new S2CServer(nodeIdentity1, messageReaderFactory, s2cOptions, meterRegistry1);

    s2cNode1 = S2CNode.builder()
        .bucket(bucket)
        .nodeIdentity(nodeIdentity1)
        .s2cGroupId(group)
        .s2cGroupRegistry(s2cGroupRegistry1)
        .s2cMessageReaderFactory(messageReaderFactory)
        .s2cOptions(s2cOptions.snapshottingThreshold(10))
        .s2cServer(s2cServer1)
        .s3Facade(s3Facade)
        .build();
    if (stateMachine == StateMachine.COUNTER) {
      counterStateMachine1 = s2cNode1.createAndRegisterStateMachine("counterSM",
          CounterStateMachine::new);
    } else if (stateMachine == StateMachine.APPENDER) {
      appenderStateMachine1 = s2cNode1.createAndRegisterStateMachine("appenderSM",
          StringAppender::new);
    } else if (stateMachine == StateMachine.COUNTER_AND_APPENDER) {
      counterStateMachine1 = s2cNode1.createAndRegisterStateMachine("counterSM",
          CounterStateMachine::new);
      appenderStateMachine1 = s2cNode1.createAndRegisterStateMachine("appenderSM",
          StringAppender::new);
    }

    s2cServer1.start();
    s2cNode1.start();
  }

  private void initAndStartNode2(Supplier<S2CMessageReader> messageReaderFactory,
      StateMachine stateMachine) throws IOException, InterruptedException {
    s2cServer2 = new S2CServer(nodeIdentity2, messageReaderFactory, s2cOptions, meterRegistry2);

    s2cNode2 = S2CNode.builder()
        .bucket(bucket)
        .nodeIdentity(nodeIdentity2)
        .s2cGroupId(group)
        .s2cGroupRegistry(s2cGroupRegistry2)
        .s2cMessageReaderFactory(messageReaderFactory)
        .s2cOptions(s2cOptions.snapshottingThreshold(10))
        .s2cServer(s2cServer2)
        .s3Facade(s3Facade)
        .build();

    if (stateMachine == StateMachine.COUNTER) {
      counterStateMachine2 = s2cNode2.createAndRegisterStateMachine("counterSM",
          CounterStateMachine::new);
    } else if (stateMachine == StateMachine.APPENDER) {
      appenderStateMachine2 = s2cNode2.createAndRegisterStateMachine("appenderSM",
          StringAppender::new);
    } else if (stateMachine == StateMachine.COUNTER_AND_APPENDER) {
      counterStateMachine2 = s2cNode2.createAndRegisterStateMachine("counterSM",
          CounterStateMachine::new);
      appenderStateMachine2 = s2cNode2.createAndRegisterStateMachine("appenderSM",
          StringAppender::new);
    }

    s2cServer2.start();
    s2cNode2.start();
  }

  private void shutdownNode1() throws InterruptedException {
    s2cNode1.close();
    s2cServer1.close();
  }

  private void shutdownNode2() throws InterruptedException {
    s2cNode2.close();
    s2cServer2.close();
  }

  @Test
  void testCommandSingleNode() throws IOException, InterruptedException {
    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);
    assertDoesNotThrow(() -> counterStateMachine1.increment());
    int value = assertDoesNotThrow(() -> counterStateMachine1.get());
    assertEquals(1, value);
  }

  @Test
  void testConcurrentCommandsTwoNodes() throws IOException, InterruptedException {
    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      int i = 0;
      while (i < 10) {
        executorService.execute(() -> {
          assertDoesNotThrow(() -> counterStateMachine1.increment());
        });
        i++;
      }

      // Join while other node applying
      initAndStartNode2(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
          StateMachine.COUNTER);

      int j = 0;
      while (j < 5) {
        executorService.execute(() -> {
          assertDoesNotThrow(() -> counterStateMachine2.decrement());
        });
        j++;
      }

    }

    int value1 = assertDoesNotThrow(() -> counterStateMachine1.get());
    int value2 = assertDoesNotThrow(() -> counterStateMachine2.get());

    boolean node1IsLeader = assertDoesNotThrow(() -> s2cNode1.isLeader());
    boolean node2IsLeader = assertDoesNotThrow(() -> s2cNode2.isLeader());

    assertNotEquals(node1IsLeader, node2IsLeader);

    assertEquals(5, value1);
    assertEquals(5, value2);

  }

  @Test
  void testLogOrderingWithConcurrentCommandsTwoNodesTwoSMs()
      throws IOException, InterruptedException, S2CStoppedException, ApplicationException {

    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      CountDownLatch l = new CountDownLatch(1);
      AtomicInteger x = new AtomicInteger(0);

      executorService.execute(() -> {

        assertDoesNotThrow(() -> {
          initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
              StateMachine.COUNTER_AND_APPENDER);
          initAndStartNode2(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
              StateMachine.COUNTER_AND_APPENDER);
          assertTrue(s2cNode1.isLeader());
          l.countDown();
        });

      });

      executorService.execute(() -> {
        try {
          l.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        int i = 0;
        while (i < 10) {
          assertDoesNotThrow(() -> {
            appenderStateMachine1.append(x.getAndIncrement() + "");
          });
          i++;

        }
      });
      // Send commands concurrently to stress the leader
      int j = 0;
      while (j < 10) {
        executorService.execute(() -> {
          try {
            l.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          assertDoesNotThrow(() -> {
            counterStateMachine2.increment();
          });
        });

        j++;
      }

    }

    String node1valueAppender = appenderStateMachine1.value();
    int node1valueCounter = counterStateMachine1.get();

    assertEquals("0123456789", node1valueAppender);
    assertEquals(10, node1valueCounter);

  }

  @Test
  void testNodeDoesCatchUp() throws IOException, InterruptedException {
    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      // Once we have both nodes ready, we start sending requests with fake latency
      int j = 0;
      while (j < 100) {
        executorService.execute(() -> {
          assertDoesNotThrow(() -> {
            counterStateMachine1.increment();
          });
        });
        j++;
      }
    }

    shutdownNode1();

    initAndStartNode2(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    int value2 = assertDoesNotThrow(() -> counterStateMachine2.get());

    boolean node2IsLeader = assertDoesNotThrow(() -> s2cNode2.isLeader());

    assertEquals(100, value2);
    assertTrue(node2IsLeader);

  }

  @Test
  void testNodeDoesCatchUpConcurrently() throws IOException, InterruptedException {
    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      int j = 0;
      while (j < 100) {
        executorService.execute(() -> {
          assertDoesNotThrow(() -> {
            counterStateMachine1.increment();
          });
        });
        j++;
      }

      executorService.execute(() -> {
        assertDoesNotThrow(() -> initAndStartNode2(
            newS2CMessageReaderFactory(s2cOptions.maxMessageSize()), StateMachine.COUNTER));
      });
    }

    shutdownNode1();

    int value2 = assertDoesNotThrow(() -> counterStateMachine2.get());

    boolean node2IsLeader = assertDoesNotThrow(() -> s2cNode2.isLeader());

    assertEquals(100, value2);
    assertTrue(node2IsLeader);

  }

  @Test
  void testConcurrentCommandsSingleNode() throws IOException, InterruptedException {

    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
      int i = 0;
      while (i < 5) {
        executorService.execute(() -> {
          assertDoesNotThrow(() -> counterStateMachine1.increment());
        });
        i++;
      }
      int j = 0;
      while (j < 5) {
        executorService.execute(() -> {
          assertDoesNotThrow(() -> counterStateMachine1.decrement());
        });
        j++;
      }
    }

    int value = assertDoesNotThrow(() -> counterStateMachine1.get());
    assertEquals(0, value);

  }

  @Test
  void testCommandFailsWhenNodeIsShuttingDown() throws IOException, InterruptedException {

    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);
    AtomicReference<Throwable> cause = new AtomicReference<>();
    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      CountDownLatch latch = new CountDownLatch(1);

      executorService.execute(() -> {
        try {
          shutdownNode1();
          latch.countDown();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      executorService.execute(() -> {
        try {
          latch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        try {
          counterStateMachine1.increment();
        } catch (ApplicationException e) {
          cause.set(e.getCause());
        }
      });

    }

    assertNotNull(cause.get());
    assertTrue(cause.get() instanceof S2CStoppedException);
  }

  @Test
  void testCommandSuccedsWhenLeaderShuttingDownTwoNodes() throws IOException, InterruptedException {
    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    initAndStartNode2(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    boolean node1IsLeader = assertDoesNotThrow(() -> s2cNode1.isLeader());
    assertTrue(node1IsLeader);

    AtomicReference<Throwable> cause = new AtomicReference<>();
    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      CountDownLatch latch = new CountDownLatch(1);

      executorService.execute(() -> {
        try {
          latch.countDown();
          shutdownNode1();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      executorService.execute(() -> {
        try {
          latch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        try {
          // Must retry till succeeds (as a leader because now single node)
          counterStateMachine2.increment();

        } catch (ApplicationException e) {
          cause.set(e.getCause());
        }
      });

    }
    int value = assertDoesNotThrow(() -> counterStateMachine2.get());
    boolean node2IsLeader = assertDoesNotThrow(() -> s2cNode2.isLeader());
    assertTrue(node2IsLeader);
    assertNull(cause.get());
    assertEquals(1, value);

  }

  @Test
  void testCommandSuccedsWhenLeaderShuttingDownTwoNodesConcurrently()
      throws IOException, InterruptedException {

    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    initAndStartNode2(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    boolean node1IsLeader = assertDoesNotThrow(() -> s2cNode1.isLeader());
    assertTrue(node1IsLeader);

    AtomicReference<Throwable> cause = new AtomicReference<>();
    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      CountDownLatch latch = new CountDownLatch(1);

      executorService.execute(() -> {
        try {
          latch.await();
          shutdownNode1();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // All threads must keep retrying till a valid leader is found (node itself must
      // become
      // leader)
      executorService.execute(() -> {
        int i = 0;
        while (i < 10) {
          try {
            counterStateMachine2.increment();
          } catch (ApplicationException e) {
            cause.set(e.getCause());
          }
          i++;
          if (i == 2) {
            latch.countDown();
          }
        }

      });

    }
    int value = assertDoesNotThrow(() -> counterStateMachine2.get());
    boolean node2IsLeader = assertDoesNotThrow(() -> s2cNode2.isLeader());
    assertTrue(node2IsLeader);
    assertNull(cause.get());
    assertEquals(10, value);

  }

  @Test
  void testCommandExactlyOnceSuccedsWhenLeaderUnreacheable()
      throws IOException, InterruptedException {

    AtomicBoolean dropAll = new AtomicBoolean();

    CountDownLatch startedDroppingAll = new CountDownLatch(1);

    initAndStartNode1(newChaosS2CMessageReaderFactory(s2cOptions.maxMessageSize(), () -> {

      if (dropAll.get()) {
        startedDroppingAll.countDown();
        return 1;
      } else {
        return 0;
      }
    }, Set.of(Failure.DROP), m -> {
    }), StateMachine.COUNTER);

    AtomicBoolean node1IsLeader = new AtomicBoolean(assertDoesNotThrow(() -> {
      return s2cNode1.isLeader();
    }));

    assertTrue(node1IsLeader.get());

    initAndStartNode2(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    AtomicBoolean node2IsLeader = new AtomicBoolean(assertDoesNotThrow(() -> {
      return s2cNode2.isLeader();
    }));

    assertFalse(node2IsLeader.get());

    AtomicReference<Throwable> cause = new AtomicReference<>();

    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
      CountDownLatch node2BecameLeaderLatch = new CountDownLatch(1);

      // All threads must keep retrying (detect exactly-once issues) till a valid
      // leader is found
      // (node itself must become
      // leader when i=5)

      executorService.execute(() -> {

        int i = 0;
        while (i < 20) {

          if (i == 5) {

            dropAll.set(true);

            try {
              startedDroppingAll.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }

            try {
              counterStateMachine2.increment();
            } catch (ApplicationException e) {
              cause.set(e);
            }

          } else {
            assertDoesNotThrow(() -> {
              counterStateMachine2.increment();
              // Node1 started drop message, so we became leader
              if (s2cNode2.isLeader()) {
                node2BecameLeaderLatch.countDown();
              }
            });

          }

          i++;
        }
      });

      // Do some random decrements to stress

      executorService.execute(() -> {

        int i = 0;
        while (i < 3) {
          assertDoesNotThrow(() -> {
            counterStateMachine2.decrement();
          });

          i++;
        }
      });

      executorService.execute(() -> {

        int i = 0;
        while (i < 4) {
          assertDoesNotThrow(() -> {
            counterStateMachine2.decrement();
          });
          i++;
        }
      });

      executorService.execute(() -> {
        try {

          // Now node2 must be leader
          node2BecameLeaderLatch.await();

          // So that we can receive the response
          dropAll.set(false);

          node1IsLeader.set(assertDoesNotThrow(() -> s2cNode1.isLeader()));

          // Node 1 still thinks it is leader
          assertTrue(node1IsLeader.get());

          // This should fail to commit locally and send instead to node2
          counterStateMachine1.increment();

          node1IsLeader.set(assertDoesNotThrow(() -> s2cNode1.isLeader()));
          // Node 1 detected stale leadership and detected new leader
          assertFalse(node1IsLeader.get());

          node2IsLeader.set(assertDoesNotThrow(() -> s2cNode2.isLeader()));
          assertTrue(node2IsLeader.get());

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (ApplicationException e) {
          cause.set(e);

        }
      });
    }
    int value = assertDoesNotThrow(() -> counterStateMachine1.get());
    assertNull(cause.get());
    assertEquals(20 + 1 - 3 - 4, value);

  }

  @Test
  void testSnapshotting()
      throws IOException, InterruptedException, S2CStoppedException, ApplicationException {

    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    int i = 0;
    while (i < 10) {
      assertDoesNotThrow(() -> {
        counterStateMachine1.increment();
      });
      i++;
    }

    // We now know 15 commits were made, snapshot happens each 10
    // We give sometime for truncation
    TimeUnit.SECONDS.sleep(5);

    KeysResolver keysResolver = new KeysResolver(group);

    GetObjectResult result = assertDoesNotThrow(
        () -> s3Facade.getObject(keysResolver.stateSnapshotKey(), bucket));

    StateSnapshot snapshot = StateSnapshot.parseFrom(result.data());

    long snapshotApplyIndex = snapshot.getEndApplyIndex();

    assertEquals(10, snapshotApplyIndex);

    long commitIndex = s2cNode1.commitIndex();

    // Assert log truncated
    AtomicInteger j = new AtomicInteger();
    while (j.get() <= snapshotApplyIndex) {
      S3Exception ex = null;
      try {
        s3Facade.getObject(keysResolver.logEntryKey(j.get()), bucket);
      } catch (S3Exception e) {
        ex = e;
      }
      assertNotNull(ex);
      assertEquals(ex.awsErrorDetails().errorCode(), S3Error.KEY_NOT_FOUND.errorCode());
      j.incrementAndGet();
    }
    while (j.get() <= commitIndex) {
      assertDoesNotThrow(() -> s3Facade.getObject(keysResolver.logEntryKey(j.get()), bucket));
      j.incrementAndGet();
    }

    int node1value = counterStateMachine1.get();

    shutdownNode1();

    initAndStartNode2(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    // Assert snapshot is restored
    int node2value = counterStateMachine2.get();

    assertEquals(node2value, node1value);

  }

  // @Test
  // void testFaultInjectedSyncrhonizationKeepsOrdering()
  // throws IOException, InterruptedException, S2CStoppedException,
  // ApplicationException {

  // initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
  // StateMachine.APPENDER);

  // boolean node1Leader = s2cNode1.isLeader();
  // assertTrue(node1Leader);

  // initAndStartNode2(newChaosS2CMessageReaderFactory(s2cOptions.maxMessageSize(),
  // () -> 3,
  // Set.of(Failure.DROP, Failure.IO_EXCEPTION), m -> {
  // }), StateMachine.APPENDER);

  // boolean node2Ledaer = s2cNode2.isLeader();
  // assertFalse(node2Ledaer);

  // AtomicInteger i = new AtomicInteger(1);
  // while (i.get() <= 10) {
  // assertDoesNotThrow(() -> {
  // appenderStateMachine1.append(i.get() + "");
  // });
  // i.incrementAndGet();
  // }

  // String node1value = appenderStateMachine1.value();

  // AtomicReference<ByteString> snapshot = new
  // AtomicReference<>(appenderStateMachine2.snapshot());

  // AtomicReference<String> node2Value = new AtomicReference<>(
  // snapshot.get().toString(StandardCharsets.UTF_8));

  // TestUtil.sleepUntil(300, 500, () -> {

  // snapshot.set(appenderStateMachine2.snapshot());
  // node2Value.set(snapshot.get().toString(StandardCharsets.UTF_8));

  // return node2Value.get().equals(node1value);
  // });

  // assertEquals(node1value, node2Value.get());

  // node1Leader = s2cNode1.isLeader();
  // assertTrue(node1Leader);

  // node2Ledaer = s2cNode2.isLeader();
  // assertFalse(node2Ledaer);

  // }

  @Test
  void testSynchronizationOfTooFarBehindFollower()
      throws IOException, InterruptedException, ApplicationException {

    // Disable caching so that no truncated entry can be found in cache
    int logLruCacheSize = s2cOptions.logLruCacheSize();
    s2cOptions.logLruCacheSize(0);

    initAndStartNode1(newS2CMessageReaderFactory(s2cOptions.maxMessageSize()),
        StateMachine.COUNTER);

    AtomicBoolean farBehindDetected = new AtomicBoolean();
    AtomicBoolean snapshottingDone = new AtomicBoolean();
    AtomicInteger failAfterFahrBehind = new AtomicInteger(1);

    CountDownLatch failBehindDetectedLatch = new CountDownLatch(1);

    Consumer<S2CMessage> inspector = m -> {
      var synchReq = m.getSynchronizeRequest();
      if (synchReq.hasFollowerTooFarBehindError()) {
        farBehindDetected.set(true);
        failBehindDetectedLatch.countDown();
      }
    };

    // We initially fail half synch messages, then we allow all once snapshotting
    // done to detect
    // late follower, then we fail all to ensure follower can catch up alone.
    Supplier<Integer> failEverySupplier = () -> {
      if (farBehindDetected.get()) {
        // We let far behind error pass, and then we drop all
        if (failAfterFahrBehind.get() == 1) {
          failAfterFahrBehind.decrementAndGet();
          return 0;
        } else {
          return 1;
        }
      }
      if (snapshottingDone.get()) {
        return 0;
      }
      return 2;
    };

    // We never synchronize, once we know leader detected follower is far behind
    initAndStartNode2(newChaosS2CMessageReaderFactory(s2cOptions.maxMessageSize(),
        failEverySupplier, Set.of(Failure.DROP), inspector), StateMachine.COUNTER);

    int i = 0;
    while (i < 10) {
      counterStateMachine1.increment();
      i++;
    }

    // Wait till snapshotted
    TimeUnit.SECONDS.sleep(5);

    snapshottingDone.set(true);

    KeysResolver keysResolver = new KeysResolver(group);

    // We make sure log was truncated and last entry was removed
    assertThrows(S3Exception.class, () -> {
      s3Facade.getObject(keysResolver.logEntryKey(10), bucket);
    });

    // And then we know that follower is not in synch
    failBehindDetectedLatch.await();

    // Wait till follower has caught up
    TimeUnit.SECONDS.sleep(5);

    int node2Value = counterStateMachine2.snapshot().asReadOnlyByteBuffer().getInt();

    assertTrue(farBehindDetected.get());
    assertEquals(10, node2Value);

    // reset
    s2cOptions.logLruCacheSize(logLruCacheSize);

  }

}