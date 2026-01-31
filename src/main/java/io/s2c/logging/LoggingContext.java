package io.s2c.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.slf4j.spi.LoggingEventBuilder;

public class LoggingContext {

  private final String[] keys;

  private final Object[] values;

  private LoggingContext(String[] keys, Object[] values) {
    this.keys = keys;
    this.values = values;
  }

  public static class Builder {

    private final Map<String, Object> contextMap = new HashMap<>();

    public Builder add(String key, Object value) {
      Objects.requireNonNull(key, "Key may not be null");
      Objects.requireNonNull(value, "Value may not be null");
      contextMap.put(key, value);
      return this;
    }

    public LoggingContext build() {
      int size = contextMap.size();
      String[] keys = new String[size];
      Object[] values = new Object[size];
      int i = 0;
      for (var e : contextMap.entrySet()) {
        keys[i] = e.getKey();
        values[i] = e.getValue();
        i++;
      }
      return new LoggingContext(keys, values);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public LoggingEventBuilder enrich(LoggingEventBuilder loggingEventBuilder) {
    for (int i = 0; i < this.keys.length; i++) {
      Object v = this.values[i];
      if (v instanceof Supplier<?> sup) {
        loggingEventBuilder.addKeyValue(this.keys[i], sup);
      } else {
        loggingEventBuilder.addKeyValue(this.keys[i], v);
      }
    }
    return loggingEventBuilder;
  }

}
