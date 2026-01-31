package io.s2c.error;

import io.s2c.model.state.LeaderState;

public final class ConcurrentStateModificationException
extends CommitException {

  public ConcurrentStateModificationException(LeaderState leaderState) {
    super(leaderState);
  }

}