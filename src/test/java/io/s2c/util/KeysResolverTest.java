package io.s2c.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeysResolverTest {


  void testKeys(String groupId, KeysResolver keysResolver) {
    String key = keysResolver.logEntryKey(0);
    assertEquals("s2c/%s/log/0000000000000000000.pb".formatted(groupId), key);

    key = keysResolver.logEntryKey(1);
    assertEquals("s2c/%s/log/0000000000000000001.pb".formatted(groupId), key);

    key = keysResolver.logEntryKey(12345);
    assertEquals("s2c/%s/log/0000000000000012345.pb".formatted(groupId), key);

    key = keysResolver.logEntryKey(Long.MAX_VALUE);
    assertEquals("s2c/%s/log/%s.pb".formatted(groupId, String.valueOf(Long.MAX_VALUE)), key);
    

    String leaderKey = keysResolver.leaderKey();
    assertEquals("s2c/%s/leader.json".formatted(groupId), leaderKey);


    String snapshotKey = keysResolver.stateSnapshotKey();
    assertEquals("s2c/%s/state_snapshot.pb".formatted(groupId), snapshotKey);
  }

  @Test
  void testDifferentGroupIds() {
    KeysResolver resolver1 = new KeysResolver("group1");
    
    testKeys("group1", resolver1);
    
    KeysResolver resolver2 = new KeysResolver("group2");
    
    testKeys("group2", resolver2);

  }

}
