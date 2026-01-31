package io.s2c.error;

import io.s2c.model.state.LeaderState;

public final class NotLeaderException extends CommitException {

  public NotLeaderException(LeaderState leaderState) {
    super(leaderState);
  }

}
