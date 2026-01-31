package io.s2c.error;

import software.amazon.awssdk.services.s3.model.S3Exception;

public class NonTransientS3Exception extends RuntimeException {
  public NonTransientS3Exception(S3Exception e) {
    super(e);
  }
}
