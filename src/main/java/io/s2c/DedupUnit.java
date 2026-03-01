package io.s2c;

import io.s2c.model.state.LastResult;

public final class DedupUnit {

  private final LastResult lastResult;
  private EOSBuffer eosBuffer;

  public DedupUnit(LastResult lastResult, EOSBuffer eosBuffer) {
    this.lastResult = lastResult;
    this.eosBuffer = eosBuffer;
  }

  public LastResult lastResult() {
    return lastResult;
  }

  public EOSBuffer eosBuffer() {
    return eosBuffer;
  }

  public void resetBuffer() {
    this.eosBuffer.close();
    // Now the node is not leader, start with a fresh eosBuffer when leader again.
    this.eosBuffer = null;
  }

}