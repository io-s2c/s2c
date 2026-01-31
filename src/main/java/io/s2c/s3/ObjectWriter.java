package io.s2c.s3;

import java.util.Optional;

import com.google.protobuf.ByteString;

import io.s2c.configs.S2CRetryOptions;
import io.s2c.logging.StructuredLogger;
import io.s2c.util.BackoffCounter;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class ObjectWriter {

  private final S3Facade s3Facade;
  private final String bucket;
  private final StructuredLogger log;
  private final S2CRetryOptions s2cRetryOptions;
  private final S3ErrorEvaluator s3ErrorEvaluator = new S3ErrorEvaluator();

  public ObjectWriter(S3Facade s3Facade,
      String bucket,
      StructuredLogger log,
      S2CRetryOptions s2cRetryOptions) {
    this.s3Facade = s3Facade;
    this.bucket = bucket;
    this.log = log;
    this.s2cRetryOptions = s2cRetryOptions;
  }

  public void write(String key, ByteString data) throws InterruptedException {

    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cRetryOptions)
        .unlimited()
        .build();

    while (backoffCounter.canAttempt()) {
      try {
        s3Facade.putObject(key, bucket, data);
        return;
      }
      catch (S3Exception e) {
        log.debug().setCause(e).log("Error while writing");
        s3ErrorEvaluator.verifyTransient(e);
      }
      backoffCounter.enrich(log.debug()).log("Retrying");
      backoffCounter.awaitNextAttempt();
    }
  }

  public Optional<String> writeIfMatch(String key, ByteString data, String eTag)
      throws InterruptedException {

    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cRetryOptions)
        .unlimited()
        .build();

    while (backoffCounter.canAttempt()) {
      try {
        return Optional.of(s3Facade.putIfMatch(key, bucket, data, eTag));
      }
      catch (S3Exception e) {
        log.debug().setCause(e).log("Error while writing");
        if (s3ErrorEvaluator.hasS3Error(e, S3Error.PRECONDITION_FAILED)) {
          break;
        }
        s3ErrorEvaluator.verifyTransient(e);
      }
      backoffCounter.enrich(log.debug()).log("Retrying");
      backoffCounter.awaitNextAttempt();
    }
    return Optional.empty();

  }

  public Optional<String> writeIfNoneMatch(String key, ByteString byteBuffer)
      throws InterruptedException {

    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cRetryOptions)
        .unlimited()
        .build();

    while (backoffCounter.canAttempt()) {
      try {
        return Optional.of(s3Facade.putIfNoneMatch(key, bucket, byteBuffer));
      }
      catch (S3Exception e) {
        log.debug().setCause(e).log("Error while writing");
        if (s3ErrorEvaluator.hasS3Error(e, S3Error.PRECONDITION_FAILED)) {
          break;
        }
        s3ErrorEvaluator.verifyTransient(e);
      }
      backoffCounter.enrich(log.debug()).log("Retrying");
      backoffCounter.awaitNextAttempt();
    }
    return Optional.empty();
  }

  public void delete(String key) throws InterruptedException {

    BackoffCounter backoffCounter = BackoffCounter.withRetryOptions(s2cRetryOptions)
        .unlimited()
        .build();

    while (backoffCounter.canAttempt()) {
      try {
        s3Facade.deleteObject(key, bucket);
        return;
      }
      catch (S3Exception e) {
        log.debug().setCause(e).log("Error while deleting");
        s3ErrorEvaluator.verifyTransient(e);
      }
      backoffCounter.enrich(log.debug()).log("Retrying");
      backoffCounter.awaitNextAttempt();
    }
  }

}