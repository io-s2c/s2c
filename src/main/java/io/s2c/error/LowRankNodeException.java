package io.s2c.error;

public class LowRankNodeException extends Exception {

  private final int highestRank;

  public LowRankNodeException(int highestRank) {
    this.highestRank = highestRank;
  }

  public int highestRank() {
    return this.highestRank;
  }

}
