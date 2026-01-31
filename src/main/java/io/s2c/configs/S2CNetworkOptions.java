package io.s2c.configs;

public class S2CNetworkOptions {

  private static final int MAX_PENDING_RESPONSES_PER_CLIENT = 1000;
  private static final int MAX_PENDING_REQUESTS_PER_CLIENT = 1000;

  private static final int MAX_PENDING_SERVER_RESPONSES = 1000;

  private static final int CONNECT_TIMEOUT_MS = 10_000;
  private static final int THROTTLE_DELAY_MS = 10_000;
  private static final int HANDSHAKE_TIMEOUT = 10_000;

  private int maxPendingReqsPerClient;
  private int maxPendingRespsPerClient;
  private int maxPendingServerResponses;
  private int connectTimeoutMs;
  private int throttleDelayMs;
  private int handshakeTimeout;
  
  public S2CNetworkOptions() {
    this.maxPendingReqsPerClient = MAX_PENDING_REQUESTS_PER_CLIENT;
    this.maxPendingRespsPerClient = MAX_PENDING_RESPONSES_PER_CLIENT;
    this.maxPendingServerResponses = MAX_PENDING_SERVER_RESPONSES;
    this.connectTimeoutMs = CONNECT_TIMEOUT_MS;
    this.throttleDelayMs = THROTTLE_DELAY_MS;
    this.handshakeTimeout = HANDSHAKE_TIMEOUT;
  }

  public S2CNetworkOptions maxPendingReqsPerClient(int maxPendingReqsPerClient) {
    this.maxPendingReqsPerClient = maxPendingReqsPerClient;
    return this;
  }

  public S2CNetworkOptions maxPendingRespsPerClient(int maxPendingRespsPerClient) {
    this.maxPendingRespsPerClient = maxPendingRespsPerClient;
    return this;
  }

  public S2CNetworkOptions maxPendingServerResponses(int maxPendingServerResponses) {
    this.maxPendingServerResponses = maxPendingServerResponses;
    return this;
  }

  public S2CNetworkOptions connectTimeoutMs(int connectTimeoutMs) {
    this.connectTimeoutMs = connectTimeoutMs;
    return this;
  }

  public S2CNetworkOptions throttleDelayMs(int throttleDelayMs) {
    this.throttleDelayMs = throttleDelayMs;
    return this;
  }

  public S2CNetworkOptions handshakeTimeout(int handshakeTimeout) {
    this.handshakeTimeout = handshakeTimeout;
    return this;
  }

  public int maxPendingClientReqs() {
    return this.maxPendingReqsPerClient;
  }

  public int maxPendingRespsPerClient() {
    return this.maxPendingRespsPerClient;
  }

  public int maxPendingServerResponses() {
    return this.maxPendingServerResponses;
  }

  public int connectTimeoutMs() {
    return connectTimeoutMs;
  }

  public int throttleDelayMs() {
    return throttleDelayMs;
  }

  public int handshakeTimeout() {
    return handshakeTimeout;
  }

}
