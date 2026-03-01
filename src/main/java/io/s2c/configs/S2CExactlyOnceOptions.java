package io.s2c.configs;


public class S2CExactlyOnceOptions {
  
  private boolean enabled = true;
  private int outOfSeqBufferSize = 1000;
  private int outOfSeqBufferGCDelaySec = 30;
  
  
  public S2CExactlyOnceOptions outOfSeqBufferSize(int outOfSeqBufferSize) {
    this.outOfSeqBufferSize = outOfSeqBufferSize;
    return this;
  }
  
  public S2CExactlyOnceOptions outOfSeqBufferGCDelaySec(int outOfSeqBufferGCDelaySec) {
    this.outOfSeqBufferGCDelaySec = outOfSeqBufferGCDelaySec;
    return this;
  }
  
  public S2CExactlyOnceOptions enabled(boolean enabled) {
    this.enabled = enabled;
    return this;
  }
  
  public int outOfSeqBufferSize() {
    return this.outOfSeqBufferSize;
  }
  
  public int outOfSeqBufferGCDelaySec() {
    return this.outOfSeqBufferGCDelaySec;
  }
  
  public boolean enabled() {
    return this.enabled;
  }
  
}