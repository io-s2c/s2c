package io.s2c.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.s2c.ContextProvider;
import io.s2c.LeaderStateManager;
import io.s2c.configs.S2COptions;
import io.s2c.error.ConcurrentStateModificationException;
import io.s2c.error.ConnectTimeoutException;
import io.s2c.error.S2CStoppedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.FollowerInfo;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.NodeIdentity;
import io.s2c.network.S2CClient;
import io.s2c.network.error.ClientNotConnectedException;
import io.s2c.network.error.UnknownHostException;
import io.s2c.s3.ObjectReader;
import io.s2c.s3.ObjectWriter;
import io.s2c.s3.S3Facade;
import io.s2c.TestUtil;

@ExtendWith(MockitoExtension.class)
@Tag("protocol")
class LeaderFencingTest {

  private LeaderStateManager leaderStateManager;
  private LeaderStateManager leaderStateManager2;

  private NodeIdentity nodeIdentity = NodeIdentity.newBuilder()
      .setAddress("localhost")
      .setPort(7777)
      .build();

  private NodeIdentity nodeIdentity2 = NodeIdentity.newBuilder()
      .setAddress("localhost2")
      .setPort(8888)
      .build();

  private S2COptions s2cOptions = new S2COptions();
  private final String bucket = "s2cbucket";

  private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

  private final AtomicLong applyIndex = new AtomicLong();
  private final AtomicLong applyIndex2 = new AtomicLong();

  @Mock
  private S2CClient s2cClient;

  S3Facade s3Facade;

  Function<StructuredLogger, ObjectReader> objectReaderFactory = l -> new ObjectReader(s3Facade,
      bucket, l, s2cOptions.s2cRetryOptions());

  @BeforeEach
  void setUp() throws UnsupportedOperationException, IOException, InterruptedException {
    ContextProvider contextProvider = new ContextProvider("group", nodeIdentity, false);
    ContextProvider contextProvider2 = new ContextProvider("group", nodeIdentity2, false);

    s3Facade = TestUtil.createS3Facade(bucket);

    Function<StructuredLogger, ObjectWriter> objectWriterFactory = l -> new ObjectWriter(s3Facade,
        bucket, l, s2cOptions.s2cRetryOptions());

    leaderStateManager = new LeaderStateManager(objectReaderFactory, objectWriterFactory,
        s2cOptions, contextProvider, () -> s2cClient, applyIndex::get, meterRegistry);

    leaderStateManager2 = new LeaderStateManager(objectReaderFactory, objectWriterFactory,
        s2cOptions, contextProvider2, () -> s2cClient, applyIndex2::get, meterRegistry);
  }

  @Test
  void testAttemptsLeadership() throws InterruptedException, S2CStoppedException {

    LeaderState leaderState = leaderStateManager.getLeaderState();

    LeaderState expectedLeaderState = LeaderState.newBuilder()
        .setNodeIdentity(nodeIdentity)
        .setCommitIndex(0)
        .setEpoch(1) // Epoch starts at 1
        .build();

    assertEquals(expectedLeaderState, leaderState);

  }

  @AfterAll
  static void stopContainer() {
    TestUtil.stopLocalStack();
  }

  @AfterEach
  void tearDown() throws UnsupportedOperationException, IOException, InterruptedException {
    if (leaderStateManager != null) {
      leaderStateManager.close();
    }
    if (leaderStateManager2 != null) {
      leaderStateManager2.close();
    }
    TestUtil.cleanupBucket(bucket);
  }

  @Test
  void testAttemptsNewLeadershipWithNewEpoch() throws InterruptedException, S2CStoppedException {

    LeaderState leaderState1 = leaderStateManager.getLeaderState();

    leaderStateManager.checkLeaderState(leaderState1);

    LeaderState leaderState2 = leaderStateManager.getLeaderState();

    LeaderState expectedLeaderState = LeaderState.newBuilder()
        .setNodeIdentity(nodeIdentity)
        .setCommitIndex(0)
        .setEpoch(leaderState1.getEpoch() + 1)
        .build();

    assertEquals(expectedLeaderState, leaderState2);

  }

  @Test
  void testUpdateCommitIndex()
      throws InterruptedException, S2CStoppedException, ConcurrentStateModificationException {

    LeaderState leaderState1 = leaderStateManager.getLeaderState();

    leaderStateManager.updateCommitIndex(10);

    LeaderState leaderState2 = leaderStateManager.getLeaderState();

    LeaderState expectedLeaderState = LeaderState.newBuilder()
        .setNodeIdentity(nodeIdentity)
        .setCommitIndex(10)
        .setEpoch(leaderState1.getEpoch())
        .build();

    assertEquals(expectedLeaderState, leaderState2);

  }

  @Test
  void testNodesAgreeOnSameLeader() {
    AtomicReference<LeaderState> leaderState = new AtomicReference<>();
    AtomicReference<LeaderState> leaderState2 = new AtomicReference<>();

    assertDoesNotThrow(() -> leaderState.set(leaderStateManager.getLeaderState()));

    assertDoesNotThrow(() -> leaderState2.set(leaderStateManager2.getLeaderState()));

    assertEquals(leaderState.get(), leaderState2.get());
    // Only one attempt succeeded
    assertEquals(1, leaderState.get().getEpoch());
  }

  @Test
  void testUpdateIndexFailsIfStaleLeader() throws InterruptedException, S2CStoppedException {

    // Leader node
    leaderStateManager.getLeaderState();

    // Not leader
    LeaderState leaderState2 = leaderStateManager2.getLeaderState();

    assertDoesNotThrow(() -> leaderStateManager.updateCommitIndex(10));

    LeaderState leaderState = leaderStateManager.getLeaderState();

    assertEquals(1, leaderState.getEpoch());
    assertEquals(10, leaderState.getCommitIndex());

    // New leadership - we do it twice to update etag cause updateCommitIndex
    // changed the etag
    leaderState2 = leaderStateManager2.checkLeaderState(leaderState2);
    leaderState2 = leaderStateManager2.checkLeaderState(leaderState2);

    assertEquals(2, leaderState2.getEpoch());
    assertEquals(10, leaderState2.getCommitIndex());

    // Not leader anymore
    assertThrows(ConcurrentStateModificationException.class,
        () -> leaderStateManager.updateCommitIndex(11));

    // Still holds old leader, get the new one
    leaderState = leaderStateManager.checkLeaderState(leaderState);

    assertEquals(leaderState, leaderState2);
  }

  @Test
  void testHigherRankIsNotAlive() throws InterruptedException, S2CStoppedException, IOException,
      UnknownHostException, ConnectTimeoutException, ClientNotConnectedException {

    // Leader node
    AtomicReference<LeaderState> leaderState = new AtomicReference<>(
        leaderStateManager.getLeaderState());

    // Not leader
    AtomicReference<LeaderState> leaderState2 = new AtomicReference<>(
        leaderStateManager2.getLeaderState());

    assertEquals(leaderState.get(), leaderState2.get());

    NodeIdentity followerNodeIdentity = NodeIdentity.getDefaultInstance();

    // We need to wait if we attempt leadership as nodeIdentity2, 100 > 70 == true
    applyIndex2.set(70);
    FollowerInfo followerInfo = FollowerInfo.newBuilder()
        .setApplyIndex(100)
        .setNodeIdentity(followerNodeIdentity)
        .build();

    boolean added = leaderStateManager.addNewFollower(followerInfo);

    assertTrue(added);

    // Make sure followers list is durable

    assertDoesNotThrow(
        () -> leaderStateManager.updateCommitIndex(leaderState.get().getCommitIndex()));

    CountDownLatch l = new CountDownLatch(1);

    doThrow(IOException.class).when(s2cClient).connect(followerNodeIdentity);

    CompletableFuture.runAsync(() -> {

      try {

        // We first do a checkLeaderState to let both leaderStateManagers have the same
        // etags,
        // So that the second call can attempt leadership

        // This one updates the etag that was changed by calling updateCommitIndex
        leaderState2.set(leaderStateManager2.checkLeaderState(leaderState2.get()));
        // Now we have the same etag, we know leadership attempt will be made
        leaderState2.set(leaderStateManager2.checkLeaderState(leaderState2.get()));
      } catch (InterruptedException | S2CStoppedException e) {
        e.printStackTrace();
      }
      l.countDown();
    });

    // We should win with new epoch with no delay as we are leader - should cause
    // leaderStateManager2 to fail attempt.
    // We sleep to give other node some time that it starts waiting, then we become
    // leader while it
    // waits
    TimeUnit.MILLISECONDS.sleep(s2cOptions.leadershipDelay() / 2);
    leaderState.set(leaderStateManager.checkLeaderState(leaderStateManager.getLeaderState()));

    l.await();

    assertEquals(2, leaderState.get().getEpoch());

    assertTrue(leaderStateManager.isLeader(leaderState.get()));

    assertFalse(leaderStateManager2.isLeader(leaderState2.get()));

    assertEquals(leaderState.get(), leaderState2.get());

  }

  @Test
  void testAwaitLeaderChange() throws InterruptedException, S2CStoppedException {

    leaderStateManager.getLeaderState();
    AtomicInteger changesCount = new AtomicInteger();
    CountDownLatch l = new CountDownLatch(10);

    Thread t = new Thread(() -> {
      assertDoesNotThrow(() -> {
        l.await();

        // This will trigger first increment
        LeaderState leaderState = leaderStateManager.getLeaderState();
        while (changesCount.get() < 5) {
          leaderState = leaderStateManager.checkLeaderState(leaderState);
          changesCount.incrementAndGet();
        }
      });
    });

    Map<AtomicInteger, CompletableFuture<?>> awaiting = new HashMap<>();
    int i = 0;
    while (i < 10) {
      var awaiter = leaderStateManager.getNewAwaiter();
      awaiting.compute(new AtomicInteger(), (k, v) -> {
        return CompletableFuture.runAsync(() -> {
          l.countDown();
          while (true) {
            try {
              awaiter.await(ll -> true);
              k.incrementAndGet();
            } catch (InterruptedException | S2CStoppedException e) {
              break;
            }
          }
        }, Executors.newVirtualThreadPerTaskExecutor());
      });
      i++;
    }
    t.start();
    t.join();
    leaderStateManager.close();
    for (var e : awaiting.entrySet()) {
      e.getValue().join();
      // 5 valid signal, and 1 WAKE_UP causes exception
      assertEquals(5, e.getKey().get());
    }

  }

  @Test
  void testAwaitRole() throws InterruptedException, S2CStoppedException {

    var leadershipAwaiter1 = leaderStateManager.getNewAwaiter();

    var followershipAwaiter1 = leaderStateManager.getNewAwaiter();

    var leadershipAwaiter2 = leaderStateManager2.getNewAwaiter();

    var followershipAwaiter2 = leaderStateManager2.getNewAwaiter();

    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {

      executorService.submit(() -> {
        try {
          leaderStateManager.getLeaderState();
          leaderStateManager2.getLeaderState();
          leaderStateManager.close();
          leaderStateManager2.close();
        } catch (InterruptedException | S2CStoppedException e) {

        }

      });

      executorService.submit(() -> {
        try {
          LeaderState leaderState = leadershipAwaiter1.await(leaderStateManager::isLeader);
          boolean isLeader = leaderStateManager.isLeader(leaderState);
          assertTrue(isLeader);
        } catch (InterruptedException | S2CStoppedException e) {

        }
      });

      executorService.submit(() -> {
        try {
          LeaderState leaderState = followershipAwaiter1
              .await(l -> !leaderStateManager.isLeader(l));
          boolean isLeader = leaderStateManager.isLeader(leaderState);
          assertFalse(isLeader);
        } catch (InterruptedException | S2CStoppedException e) {

        }

      });

      executorService.submit(() -> {
        try {
          LeaderState leaderState = leadershipAwaiter2.await(leaderStateManager2::isLeader);
          boolean isLeader = leaderStateManager2.isLeader(leaderState);
          assertFalse(isLeader);
        } catch (InterruptedException | S2CStoppedException e) {
        }
      });

      executorService.submit(() -> {
        try {
          LeaderState leaderState = followershipAwaiter2
              .await(l -> !leaderStateManager2.isLeader(l));
          boolean isLeader = leaderStateManager2.isLeader(leaderState);
          assertTrue(isLeader);
        } catch (InterruptedException | S2CStoppedException e) {

        }

      });
    }

  }

}