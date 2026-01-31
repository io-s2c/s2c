package io.s2c.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class LRUCache<K, V> {

  private final ConcurrentHashMap<K, Void> keysMap = new ConcurrentHashMap<>();
  private final Map<K, V> map;
  private final Function<K, V> objectsSupplier;

  public LRUCache(int capacity, Function<K, V> objectsSupplier) {
    map = initCache(capacity);
    this.objectsSupplier = objectsSupplier;
  }

  public void put(K key, V value) {
    map.put(key, value);
  }

  public V get(K key) {
    V val = map.get(key);
    if (val != null) {
      return val;
    }
    // For fine-grained locking
    keysMap.compute(key, (k, v) -> {
      V existing = map.get(key);
      if (existing == null) {
        V supplied = objectsSupplier.apply(key);
        if (supplied != null) {
          map.put(key, supplied);
        }
      }
      return null;
    });
    return map.get(key);

  }

  public void applyOnIterator(Consumer<Iterator<Entry<K, V>>> iteratorConsumer) {
    synchronized (map) {
      iteratorConsumer.accept(map.entrySet().iterator());
    }
  }

  private Map<K, V> initCache(int capacity) {
    return Collections.synchronizedMap(new LinkedHashMap<>(capacity, .75f, true) {
      @Override
      protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
        return size() > capacity;
      }
    });
  }

}