package io.s2c.error;

import io.s2c.model.state.LeaderState;

public abstract sealed class CommitException extends StateRequestException
permits ConcurrentStateModificationException, NotLeaderException {
  private final LeaderState leaderState;

  public CommitException(LeaderState leaderState) {
    this.leaderState = leaderState;
  }

  public LeaderState leaderState() { return this.leaderState; }
}
