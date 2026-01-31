package io.s2c.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.s2c.configs.S2CRetryOptions;
import io.s2c.error.ObjectCorruptedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.model.state.LeaderState;
import io.s2c.model.state.LogEntriesBatch;
import io.s2c.model.state.NodeIdentity;

class ObjectReaderTest {

  private ObjectReader objectReader;
  private S3Facade s3Facade;
  private final String bucket = "test-bucket";
  private StructuredLogger logger;

  @BeforeEach
  void setUp() {
    s3Facade = S3Facade.createNewInMemory(bucket);
    logger = new StructuredLogger(LoggerFactory.getLogger(ObjectReaderTest.class),
        io.s2c.logging.LoggingContext.builder().build());
    objectReader = new ObjectReader(s3Facade, bucket, logger, new S2CRetryOptions());
  }

  @Test
  void testReadExistingObject() throws InterruptedException, ObjectCorruptedException {
    String key = "test-key";
    LogEntriesBatch batch = LogEntriesBatch.newBuilder().build();
    ByteString data = batch.toByteString();
    
    s3Facade.putObject(key, bucket, data);
    
    Optional<ReadObjectResult<LogEntriesBatch>> result = 
        objectReader.read(key, LogEntriesBatch.parser());
    
    assertTrue(result.isPresent());
    assertEquals(batch, result.get().object());
    assertNotNull(result.get().eTag());
  }

  @Test
  void testReadNonExistentObject() throws InterruptedException, ObjectCorruptedException {
    Optional<ReadObjectResult<LogEntriesBatch>> result = 
        objectReader.read("nonexistent-key", LogEntriesBatch.parser());
    
    assertFalse(result.isPresent());
  }

  @Test
  void testReadJson() throws InterruptedException, ObjectCorruptedException, InvalidProtocolBufferException {
    
    String key = "test-json-key";
    
    LeaderState leaderState = LeaderState.newBuilder()
        .setNodeIdentity(NodeIdentity.newBuilder()
            .setAddress("localhost")
            .setPort(7777)
            .build())
        .setCommitIndex(100)
        .setEpoch(1)
        .build();
    
    String json = JsonFormat.printer().print(leaderState);
    s3Facade.putObject(key, bucket, ByteString.copyFromUtf8(json));
    
    Optional<ReadObjectResult<LeaderState>> result = 
        objectReader.readJson(key, LeaderState.newBuilder());
    
    assertTrue(result.isPresent());
    assertEquals(leaderState.getCommitIndex(), result.get().object().getCommitIndex());
    assertEquals(leaderState.getEpoch(), result.get().object().getEpoch());
  }


  @Test
  void testReadWithCorruptedData() {
    String key = "corrupted-key";
    s3Facade.putObject(key, bucket, ByteString.copyFromUtf8("corrupted data"));
    
    assertThrows(ObjectCorruptedException.class, () -> {
      objectReader.read(key, LogEntriesBatch.parser());
    });
  }
}

