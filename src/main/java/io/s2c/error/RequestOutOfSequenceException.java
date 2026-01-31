package io.s2c.error;

public class RequestOutOfSequenceException extends Exception {
  private final long nextSeqNum;
  
  public RequestOutOfSequenceException(long nextSeqNum) {
    this.nextSeqNum = nextSeqNum;
  }
  
  public long nextSeqNum() {
    return nextSeqNum;
  }
}
