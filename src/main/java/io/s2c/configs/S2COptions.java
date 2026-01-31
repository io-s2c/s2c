package io.s2c.configs;

public class S2COptions {

  private static final int MAX_MESSAGE_SIZE = 1024 * 500; // 500 KiB
  private static final int SNAPSHOTTING_THRESHOLD = 100; // Snapshot each N entries appended to log
                                                         // and applied to RSM
  private static final int FLUSH_INTERVAL_MS = 2000;
  private static final int BATCH_MIN_COUNT = 100;
  private static final int REQUEST_TIMEOUT_MS = 10_000;
  private static final int LEADER_HEARTBEAT_TIMEOUT_MS = 5000;
  private static final int MAX_MISSED_HEARTBEATS = 10;
  private static final int MAX_CONCURRENT_STATE_REQUESTS_HANDLING = 100;
  private static final int LEADERSHIP_DELAY_MS = 3000;
  private static final int LOG_LRU_CACHE_SIZE = 1000;
  private static final int MAX_BATCHES_PENDING_FOR_APPLY = 1000; // The batches sent by
                                                                 // SynchronizeRequest to follower

  // This is used as the size of the LRU cache containing the clients with their last values used
  // for exactly once.
  // 1000 means S2C can ensure exactly once semantics for 1000 unique clients (unique node
  // identities).
  // Should the cache size reaches 1000 and a new client joins, the least recently active client is
  // removed from the cache.
  // That client gets reinserted only when it re-appears.
  private static final int MAX_DEDUPLICATED_CLIENTS = 1000;

  private int snapshottingThreshold;
  private int flushIntervalMs;
  private int batchMinCount;
  private int requestTimeoutMs;
  private int leaderHeartbeatTimeoutMs;
  private int maxMessageSize;
  private int maxMissedHeartbeats;
  private int maxConcurrentStateRequestsHandling;
  private int leadershipDelay;
  private int maxDeduplicatedClients;
  private int logLruCacheSize;
  private int maxBatchesPendingForApply;
  private boolean logNodeIdentity;

  private S2CNetworkOptions s2cNetworkOptions;
  private S2CRetryOptions s2cRetryOptions;

  public S2COptions() {
    snapshottingThreshold = SNAPSHOTTING_THRESHOLD;
    flushIntervalMs = FLUSH_INTERVAL_MS;
    batchMinCount = BATCH_MIN_COUNT;
    requestTimeoutMs = REQUEST_TIMEOUT_MS;
    leaderHeartbeatTimeoutMs = LEADER_HEARTBEAT_TIMEOUT_MS;
    maxMessageSize = MAX_MESSAGE_SIZE;
    maxMissedHeartbeats = MAX_MISSED_HEARTBEATS;
    maxConcurrentStateRequestsHandling = MAX_CONCURRENT_STATE_REQUESTS_HANDLING;
    leadershipDelay = LEADERSHIP_DELAY_MS;
    maxDeduplicatedClients = MAX_DEDUPLICATED_CLIENTS;
    logLruCacheSize = LOG_LRU_CACHE_SIZE;
    maxBatchesPendingForApply = MAX_BATCHES_PENDING_FOR_APPLY;
    s2cNetworkOptions = new S2CNetworkOptions();
    s2cRetryOptions = new S2CRetryOptions();
  }

  public int snapshottingThreshold() {
    return snapshottingThreshold;
  }

  public int flushIntervalMs() {
    return flushIntervalMs;
  }

  public int batchMinCount() {
    return batchMinCount;
  }

  public int requestTimeoutMs() {
    return requestTimeoutMs;
  }

  public int leaderHeartbeatTimeoutMs() {
    return leaderHeartbeatTimeoutMs;
  }

  public int maxConcurrentStateRequestsHandling() {
    return maxConcurrentStateRequestsHandling;
  }

  public int maxDeduplicatedClients() {
    return maxDeduplicatedClients;
  }

  public int logLruCacheSize() {
    return logLruCacheSize;
  }

  public int maxMessageSize() {
    return maxMessageSize;
  }

  public int maxMissedHeartbeats() {
    return maxMissedHeartbeats;
  }

  public int leadershipDelay() {
    return leadershipDelay;
  }

  public int maxBatchesPendingForApply() {
    return maxBatchesPendingForApply;
  }

  public S2COptions snapshottingThreshold(int snapshottingThreshold) {
    this.snapshottingThreshold = snapshottingThreshold;
    return this;
  }

  public S2COptions flushIntervalMs(int flushIntervalMs) {
    this.flushIntervalMs = flushIntervalMs;
    return this;
  }

  public S2COptions batchMinCount(int batchMinCount) {
    this.batchMinCount = batchMinCount;
    return this;
  }

  public S2COptions requestTimeoutMs(int requestTimeoutMs) {
    this.requestTimeoutMs = requestTimeoutMs;
    return this;
  }

  public S2COptions leaderHeartbeatTimeoutMs(int leaderHeartbeatTimeoutMs) {
    this.leaderHeartbeatTimeoutMs = leaderHeartbeatTimeoutMs;
    return this;
  }

  public S2COptions maxMessageSize(int maxMessageSize) {
    this.maxMessageSize = maxMessageSize;
    return this;
  }

  public S2COptions leadershipDelay(int leadershipDelay) {
    this.leadershipDelay = leadershipDelay;
    return this;
  }

  public S2COptions maxMissedHeartbeats(int maxMissedHeartbeats) {
    this.maxMissedHeartbeats = maxMissedHeartbeats;
    return this;
  }

  public S2COptions maxConcurrentStateRequestsHandling(int maxConcurrentStateRequestsHandling) {
    this.maxConcurrentStateRequestsHandling = maxConcurrentStateRequestsHandling;
    return this;
  }

  public S2COptions maxDeduplicatedClients(int maxDeduplicatedClients) {
    this.maxDeduplicatedClients = maxDeduplicatedClients;
    return this;
  }

  public S2CNetworkOptions s2cNetworkOptions() {
    return s2cNetworkOptions;
  }

  public S2CRetryOptions s2cRetryOptions() {
    return s2cRetryOptions;
  }
  
  public boolean logNodeIdentity() {
    return logNodeIdentity;
  }

  public S2COptions s2cNetworkOptions(S2CNetworkOptions s2cNetworkOptions) {
    this.s2cNetworkOptions = s2cNetworkOptions;
    return this;
  }

  public S2COptions s2cRetryOptions(S2CRetryOptions s2cRetryOptions) {
    this.s2cRetryOptions = s2cRetryOptions;
    return this;
  }

  public S2COptions logLruCacheSize(int logLruCacheSize) {
    this.logLruCacheSize = logLruCacheSize;
    return this;
  }
  
  public S2COptions logNodeIdentity(boolean logNodeIdentity) {
    this.logNodeIdentity = logNodeIdentity;
    return this;
  }

  public S2COptions maxBatchesPendingForApply(int maxBatchesPendingForApply) {
    this.maxBatchesPendingForApply = maxBatchesPendingForApply;
    return this;
  }

}