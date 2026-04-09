package io.s2c.error;

import io.s2c.model.state.LeaderState;

public abstract sealed class CommitException extends StateRequestHandlingException
permits ConcurrentStateModificationException, NotLeaderException {
  private static final long serialVersionUID = 8786728688464975811L;
  private final LeaderState leaderState;

  public CommitException(LeaderState leaderState) {
    this.leaderState = leaderState;
  }

  public LeaderState leaderState() { return this.leaderState; }
}
