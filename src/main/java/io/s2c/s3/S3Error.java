package io.s2c.s3;

/**
 * A result can either be success, or a transient or non-transient error. Non-transient errors will
 * not be considered for retries. Errors must be checked before they considered fatal (e.g.
 * AccessDenied is fatal, but PreconditionFailed while not transient, might not be considered
 * fatal).
 */

public enum S3Error {

  // NOTE: We only set errorCode for error types that we manually throw from InMemoryS3FacadeImpl

  BUCKET_NOT_FOUND(false, "NoSuchBucket"), KEY_NOT_FOUND(false, "NoSuchKey"),
  BUCKET_ALREADY_EXISTS(false, "BucketAlreadyExists"),
  PRECONDITION_FAILED(false, "PreconditionFailed"), REDIRECT(true, ""), // Redirect,
                                                                        // TemporaryRedirect

  ACCESS_DENIED(false, ""), // AccessDenied
  INTERNAL_ERROR(true, ""), // InternalError
  KEY_TOO_LONG(false, ""), // KeyTooLongError
  INVALID_BUCKET_STATE(true, ""), // InvalidBucketState
  NO_SUCH_UPLOAD(false, ""), // NoSuchUpload, for multi part upload
  CONFLICT(true, ""), // ConditionalRequestConflict, OperationAborted,
  REQUEST_TIMEOUT(true, ""), // RequestTimeout
  REQUEST_TIME_TOO_SKEWED(true, ""), // RequestTimeTooSkewed
  SERVICE_UNAVAILABLE(true, ""), // ServiceUnavailable
  SLOW_DOWN(true, ""), // SlowDown, 503 SlowDown
  UNKNOWN_ERROR(false, "");

  S3Error() {
    this(false, "");
  }

  S3Error(boolean isTransient, String errorCode) {
    this.isTransient = isTransient;
    this.errorCode = errorCode;
  }

  private final boolean isTransient;
  private final String errorCode;

  public boolean isTransientError() {
    return this.isTransient;
  }

  public String errorCode() {
    return this.errorCode;
  }

}
