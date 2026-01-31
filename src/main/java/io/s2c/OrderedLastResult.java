package io.s2c;

import io.s2c.model.state.LastResult;

public class OrderedLastResult {
  
  private final LastResult lastResult;
  private Long nextSeqNum;

  public OrderedLastResult(LastResult lastResult) {
    this.lastResult = lastResult;
    this.nextSeqNum = lastResult.getLastSeqNum() + 1;
  }
  
  public void nextSeqNum(long nextSeqNum) {
    this.nextSeqNum = nextSeqNum;
  }
  
  public long nextSeqNum() {
    return this.nextSeqNum;
  }
  
  public LastResult lastResult() {
    return this.lastResult;
  }
  
}
