package io.s2c.s3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.util.JsonFormat;

import io.s2c.configs.S2CRetryOptions;
import io.s2c.error.ObjectCorruptedException;
import io.s2c.logging.StructuredLogger;
import io.s2c.util.BackoffCounter;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class ObjectReader implements AutoCloseable {

  private final StructuredLogger log;

  private final S3Facade s3Facade;
  private final S3ErrorEvaluator s3ErrorEvaluator = new S3ErrorEvaluator();
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final String bucket;
  private final S2CRetryOptions s2cRetryOptions;
  private volatile boolean running = true;

  public ObjectReader(S3Facade s3Facade,
      String bucket,
      StructuredLogger log,
      S2CRetryOptions s2cRetryOptions) {
    this.s3Facade = s3Facade;
    this.bucket = bucket;
    this.log = log;
    this.s2cRetryOptions = s2cRetryOptions;
  }

  static class ProtobufParser<T extends Message> {
    @SuppressWarnings("unchecked")
    public T parseFrom(byte[] data, T.Builder builder, Parser<T> parser) throws IOException {
      if (parser != null) {
        return parser.parseFrom(data);
      } else if (builder != null) {
        String jsonStr = new String(data, StandardCharsets.UTF_8);
        JsonFormat.parser().merge(jsonStr, builder);
        return (T) builder.build();
      }
      throw new IllegalArgumentException("Either parser or builder must not be null");
    }
  }

  public <T extends Message> Optional<ReadObjectResult<T>> read(String key, Parser<T> parser)
      throws InterruptedException, ObjectCorruptedException {
    Objects.requireNonNull(parser);
    return read(key, parser, null);
  }

  public <T extends Message> Optional<ReadObjectResult<T>> readJson(String key, T.Builder builder)
      throws InterruptedException, ObjectCorruptedException {
    Objects.requireNonNull(builder);
    return read(key, null, builder);
  }

  private <T extends Message> Optional<ReadObjectResult<T>> read(String key, Parser<T> parser,
      T.Builder builder) throws InterruptedException, ObjectCorruptedException {
    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cRetryOptions)
        .unlimited()
        .build();
    T object = null;
    readWriteLock.readLock().lock();
    try {
      while (running) {
        try {
          GetObjectResult res = s3Facade.getObject(key, bucket);
          ProtobufParser<T> protobufParser = new ProtobufParser<>();
          object = protobufParser.parseFrom(res.data(), builder, parser);
          return Optional.of(new ReadObjectResult<>(object, res.eTag()));
        }
        catch (S3Exception e) {
          if (s3ErrorEvaluator.hasS3Error(e, S3Error.KEY_NOT_FOUND)) {
            log.debug().addKeyValue("key", key).log("No object found with key");
            return Optional.empty();
          }
          log.debug().setCause(e).log("Error while reading object");
          s3ErrorEvaluator.verifyTransient(e);
          backoffCounter.awaitNextAttempt();

        }
        catch (IOException e) {
          log.debug().setCause(e).log("Error while reading object");
          if (e instanceof InvalidProtocolBufferException ex) {
            throw new ObjectCorruptedException(ex);
          }
          backoffCounter.awaitNextAttempt();
        }
      }
    }
    finally {
      readWriteLock.readLock().unlock();
    }
    return Optional.empty();
  }

  @Override
  public void close() throws InterruptedException {
    running = false;
    try {
      // Lock for blocking close
      readWriteLock.writeLock().lockInterruptibly();
    }
    finally {
      readWriteLock.writeLock().unlock();
    }
  }

}
