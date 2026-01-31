package io.s2c;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class S2CGroupRegistry {

  private final Set<String> s2cGroups = ConcurrentHashMap.newKeySet();

  public boolean hasGroup(String s2cGroupId) {
    return s2cGroups.contains(s2cGroupId);
  }

  public boolean registerGroup(String s2cGroupId) {
    return s2cGroups.add(s2cGroupId);
  }

}