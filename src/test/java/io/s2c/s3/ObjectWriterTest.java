package io.s2c.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.s2c.configs.S2CRetryOptions;
import io.s2c.logging.LoggingContext;
import io.s2c.logging.StructuredLogger;
import software.amazon.awssdk.services.s3.model.S3Exception;

class ObjectWriterTest {

  private ObjectWriter objectWriter;
  private S3Facade s3Facade;
  private final String bucket = "test-bucket";
  private StructuredLogger logger;

  @BeforeEach
  void setUp() {
    s3Facade = S3Facade.createNewInMemory(bucket);
    logger = new StructuredLogger(LoggerFactory.getLogger(ObjectWriterTest.class),
        LoggingContext.builder().build());
    objectWriter = new ObjectWriter(s3Facade, bucket, logger, new S2CRetryOptions());
  }

  @Test
  void testWrite() throws InterruptedException, S3Exception, IOException {
    String key = "test-key";
    ByteString data = ByteString.copyFromUtf8("test data");
    
    objectWriter.write(key, data);
    
    GetObjectResult result = s3Facade.getObject(key, bucket);
    assertEquals(data, ByteString.copyFrom(result.data()));
  }

  @Test
  void testWriteIfNoneMatch() throws InterruptedException {
    String key = "test-key";
    ByteString data = ByteString.copyFromUtf8("test data");
    
    Optional<String> eTag = objectWriter.writeIfNoneMatch(key, data);
    
    assertTrue(eTag.isPresent());
    assertNotNull(eTag.get());
    
    Optional<String> eTag2 = objectWriter.writeIfNoneMatch(key, data);
    assertFalse(eTag2.isPresent());
  }

  @Test
  void testWriteIfMatch() throws InterruptedException, S3Exception, IOException {
    String key = "test-key";
    ByteString data1 = ByteString.copyFromUtf8("data1");
    ByteString data2 = ByteString.copyFromUtf8("data2");
    
    Optional<String> eTag1 = objectWriter.writeIfNoneMatch(key, data1);
    assertTrue(eTag1.isPresent());
    
    Optional<String> eTag2 = objectWriter.writeIfMatch(key, data2, eTag1.get());
    assertTrue(eTag2.isPresent());
    assertNotEquals(eTag1.get(), eTag2.get());
    
    GetObjectResult result = s3Facade.getObject(key, bucket);
    assertEquals(data2, ByteString.copyFrom(result.data()));
  }

  @Test
  void testWriteIfMatchWithWrongETag() throws InterruptedException, S3Exception, IOException {
    String key = "test-key";
    ByteString data1 = ByteString.copyFromUtf8("data1");
    ByteString data2 = ByteString.copyFromUtf8("data2");
    
    Optional<String> eTag1 = objectWriter.writeIfNoneMatch(key, data1);
    assertTrue(eTag1.isPresent());
    
    Optional<String> eTag2 = objectWriter.writeIfMatch(key, data2, "wrong-etag");
    assertFalse(eTag2.isPresent());
    
    GetObjectResult result = s3Facade.getObject(key, bucket);
    assertEquals(data1, ByteString.copyFrom(result.data()));
  }

  @Test
  void testDelete() throws InterruptedException {
    String key = "test-key";
    ByteString data = ByteString.copyFromUtf8("test data");
    
    objectWriter.write(key, data);
    
    objectWriter.delete(key);
    
    // Should not exist anymore
    assertThrows(Exception.class, () -> {
      s3Facade.getObject(key, bucket);
    });
  }
}

