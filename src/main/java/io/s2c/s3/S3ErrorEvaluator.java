package io.s2c.s3;

import io.s2c.error.NonTransientS3Exception;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3ErrorEvaluator {

  public S3Error getS3Error(S3Exception e) {
    if (e.awsErrorDetails() == null || e.awsErrorDetails().errorCode() == null) {
      return S3Error.UNKNOWN_ERROR;
    }
    return switch (e.awsErrorDetails().errorCode()) {
    case "NoSuchBucket" -> S3Error.BUCKET_NOT_FOUND;
    case "NoSuchKey" -> S3Error.KEY_NOT_FOUND;
    case "PreconditionFailed" -> S3Error.PRECONDITION_FAILED;
    case "Redirect", "TemporaryRedirect" -> S3Error.REDIRECT;
    case "AccessDenied" -> S3Error.ACCESS_DENIED;
    case "InternalError" -> S3Error.INTERNAL_ERROR;
    case "KeyTooLongError" -> S3Error.KEY_TOO_LONG;
    case "InvalidBucketState" -> S3Error.INVALID_BUCKET_STATE;
    case "NoSuchUpload" -> S3Error.NO_SUCH_UPLOAD;
    case "ConditionalRequestConflict", "OperationAborted" -> S3Error.CONFLICT;
    case "RequestTimeout" -> S3Error.REQUEST_TIMEOUT;
    case "RequestTimeTooSkewed" -> S3Error.REQUEST_TIME_TOO_SKEWED;
    case "ServiceUnavailable" -> S3Error.SERVICE_UNAVAILABLE;
    case "SlowDown", "503 SlowDown" -> S3Error.SLOW_DOWN;
    default -> S3Error.UNKNOWN_ERROR;
    };
  }

  public boolean hasS3Error(S3Exception e, S3Error s3Error) {
    var s3ErrorOptional = getS3Error(e);
    return s3ErrorOptional == s3Error;
  }

  public void verifyTransient(S3Exception e) {
    if (!getS3Error(e).isTransientError()) {
      throw new NonTransientS3Exception(e);
    }
  }
  
}