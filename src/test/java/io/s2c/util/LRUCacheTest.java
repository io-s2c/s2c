package io.s2c.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LRUCacheTest {

  private LRUCache<String, String> cache;

  @BeforeEach
  void setUp() {
    cache = new LRUCache<>(3, key -> null);
  }

  @Test
  void testPutAndGet() {
    cache.put("key1", "value1");
    assertEquals("value1", cache.get("key1"));
  }

  @Test
  void testGetNonExistentKey() {
    assertNull(cache.get("nonexistent"));
  }

  @Test
  void testLRUEviction() {
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");

    assertEquals("value1", cache.get("key1"));
    assertEquals("value2", cache.get("key2"));
    assertEquals("value3", cache.get("key3"));

    cache.put("key4", "value4");

    assertNull(cache.get("key1"));

    assertEquals("value2", cache.get("key2"));
    assertEquals("value3", cache.get("key3"));
    assertEquals("value4", cache.get("key4"));
  }

  @Test
  void testLRUOrdering() {
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");

    // Access key1 to make it most recently used
    cache.get("key1");

    // Add fourth entry, should evict key2 (least recently used)
    cache.put("key4", "value4");
    assertNull(cache.get("key2"));

    assertEquals("value1", cache.get("key1"));
    assertEquals("value3", cache.get("key3"));
    assertEquals("value4", cache.get("key4"));
  }

  @Test
  void testObjectSupplier() {

    AtomicInteger i = new AtomicInteger();

    LRUCache<String, String> cacheWithSupplier = new LRUCache<>(3, key -> {
      if (key.startsWith("supplied")) {
        return "supplied-" + i.incrementAndGet();
      }
      return null;
    });

    assertNull(cacheWithSupplier.get("normal-key"));

    assertEquals("supplied-1", cacheWithSupplier.get("supplied-x"));
    // Second call doesn't cause supplier call
    assertEquals("supplied-1", cacheWithSupplier.get("supplied-x"));

    assertEquals("supplied-2", cacheWithSupplier.get("supplied-y"));
  }

  @Test
  void testConcurrentAccess() throws InterruptedException {

    LRUCache<String, String> largeCache = new LRUCache<>(10 * 100, k -> null);

    int numThreads = 10;
    int operationsPerThread = 100;

    AtomicInteger errors = new AtomicInteger();
    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < numThreads; i++) {
        final int threadId = i;
        executorService.execute(() -> {
          for (int j = 0; j < operationsPerThread; j++) {
            String key = "key-" + threadId + "-" + j;
            String value = "value-" + threadId + "-" + j;
            largeCache.put(key, value);
            String retrieved = largeCache.get(key);
            if (!value.equals(retrieved)) {
              errors.incrementAndGet();
            }
          }
        });
      }
    }
    assertEquals(0, errors.get());
  }

  @Test
  void testApplyOnIterator() {
    cache.put("key1", "value1");
    cache.put("key2", "value2");
    cache.put("key3", "value3");

    AtomicInteger count = new AtomicInteger();
    cache.applyOnIterator(iterator -> {
      while (iterator.hasNext()) {
        Map.Entry<String, String> entry = iterator.next();
        assertNotNull(entry.getKey());
        assertNotNull(entry.getValue());
        count.incrementAndGet();
      }
    });

    assertEquals(3, count.get());
  }

  @Test
  void testUpdateExistingKey() {
    cache.put("key1", "value1");
    assertEquals("value1", cache.get("key1"));

    cache.put("key1", "value1-updated");
    assertEquals("value1-updated", cache.get("key1"));
  }

  @Test
  void testCapacityZero() {
    LRUCache<String, String> zeroCache = new LRUCache<>(0, key -> null);
    zeroCache.put("key1", "value1");
    // With capacity 0, entry should be immediately evicted
    assertNull(zeroCache.get("key1"));
  }

  @Test
  void testLargeCapacity() {
    LRUCache<String, String> largeCache = new LRUCache<>(1000, key -> null);
    for (int i = 0; i < 1000; i++) {
      largeCache.put("key" + i, "value" + i);
    }

    // All should be present
    for (int i = 0; i < 1000; i++) {
      assertEquals("value" + i, largeCache.get("key" + i));
    }

    // Adding one more should evict the first
    largeCache.put("key1000", "value1000");
    assertNull(largeCache.get("key0"));
    assertEquals("value1000", largeCache.get("key1000"));
  }

  @Test
  void testLRUOrderingUnderConcurrentGet() throws Exception {
    
    LRUCache<String, String> cache = new LRUCache<>(2, k -> null);

    cache.put("A", "A");
    cache.put("B", "B");

    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);
    try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
      executorService.execute(() -> {
        try {
          start.await();
          cache.get("A");
        }
        catch (InterruptedException ignored) {
        }
        finally {
          done.countDown();
        }
      });
      
      executorService.execute(() -> {
        try {
          start.await();
          cache.get("B");
        }
        catch (InterruptedException ignored) {
        }
        finally {
          done.countDown();
        }
      });
      // Start both gets at the same time
      start.countDown();
    }

    done.await();

    // At this point:
    // One of A or B SHOULD be most-recently-used.
    // The other SHOULD be least-recently-used.

    // Insert C → should evict the true LRU
    cache.put("C", "C");

    // A or B should have been evicted
    boolean aPresent = cache.get("A") != null;
    boolean bPresent = cache.get("B") != null;

    boolean cPresent = cache.get("C") != null;

    assertTrue(aPresent ^ bPresent);
    assertTrue(cPresent);
  }

}
