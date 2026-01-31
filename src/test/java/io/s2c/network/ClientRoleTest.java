package io.s2c.network;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class ClientRoleTest {

  @Test
  void testFollower() {
    assertEquals(ClientRole.FOLLOWER, ClientRole.valueOf("FOLLOWER"));
  }

  @Test
  void testSynchronizer() {
    assertEquals(ClientRole.SYNCHRONIZER, ClientRole.valueOf("SYNCHRONIZER"));
  }

  @Test
  void testIsAliveChecker() {
    assertEquals(ClientRole.IS_ALIVE_CHECKER, ClientRole.valueOf("IS_ALIVE_CHECKER"));
  }

  @Test
  void testValues() {
    ClientRole[] values = ClientRole.values();
    assertEquals(3, values.length);
  }

  @Test
  void testValueOf() {
    assertNotNull(ClientRole.valueOf("FOLLOWER"));
    assertNotNull(ClientRole.valueOf("SYNCHRONIZER"));
    assertNotNull(ClientRole.valueOf("IS_ALIVE_CHECKER"));
  }
}

