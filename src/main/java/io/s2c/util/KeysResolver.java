package io.s2c.util;

public class KeysResolver {

  private final String s2cGroupId;
  private final IdLexicographicEncoder encoder = new IdLexicographicEncoder();

  public KeysResolver(String s2cGroupId) {
    this.s2cGroupId = s2cGroupId;
  }

  public String logEntryKey(long index) {
    return "s2c/%s/log/%s.pb".formatted(s2cGroupId, encoder.encode(index));
  }

  public String leaderKey() {
    return "s2c/%s/leader.json".formatted(s2cGroupId);
  }

  public String stateSnapshotKey() {
    return "s2c/%s/state_snapshot.pb".formatted(s2cGroupId);
  }
}
