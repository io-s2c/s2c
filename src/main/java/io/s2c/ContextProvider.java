package io.s2c;

import io.s2c.logging.LoggingContext;
import io.s2c.model.state.NodeIdentity;

public class ContextProvider {

  private final String s2cGroupId;
  private final NodeIdentity nodeIdentity;
  private final boolean logNodeIdentity;

  public ContextProvider(String s2cGroupId, NodeIdentity nodeIdentity, boolean logNodeIdentity) {
    this.s2cGroupId = s2cGroupId;
    this.nodeIdentity = nodeIdentity;
    this.logNodeIdentity = logNodeIdentity;
  }

  public LoggingContext loggingContext() {
    LoggingContext.Builder builder = LoggingContext.builder().add("s2cGroupId", s2cGroupId);
    if (logNodeIdentity) {
      builder.add("nodeIdentity", nodeIdentity);
    }
    return builder.build();
  }

  public String s2cGroupId() {
    return s2cGroupId;
  }

  public String ownerName(Class<?> ownerClass) {
    return "%s-%s".formatted(ownerClass.getName(), s2cGroupId);
  }

  public NodeIdentity nodeIdentity() {
    return nodeIdentity;
  }

}
