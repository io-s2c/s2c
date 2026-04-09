package io.s2c.error;

public class S2CNodeStoppedException extends Exception {
  
  private static final long serialVersionUID = 4899464349298388869L;

  public S2CNodeStoppedException(Throwable e) {
    super(e);
  }
  
}
