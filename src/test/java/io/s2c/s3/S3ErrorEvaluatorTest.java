package io.s2c.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.s2c.error.NonTransientS3Exception;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3ErrorEvaluatorTest {

  private S3ErrorEvaluator evaluator;

  @BeforeEach
  void setUp() {
    evaluator = new S3ErrorEvaluator();
  }

  @Test
  void testGetS3ErrorNoSuchBucket() {
    S3Exception exception = createS3Exception("NoSuchBucket");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.BUCKET_NOT_FOUND, error);
  }

  @Test
  void testGetS3ErrorNoSuchKey() {
    S3Exception exception = createS3Exception("NoSuchKey");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.KEY_NOT_FOUND, error);
  }

  @Test
  void testGetS3ErrorPreconditionFailed() {
    S3Exception exception = createS3Exception("PreconditionFailed");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.PRECONDITION_FAILED, error);
  }

  @Test
  void testGetS3ErrorRedirect() {
    S3Exception exception = createS3Exception("Redirect");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.REDIRECT, error);
  }

  @Test
  void testGetS3ErrorTemporaryRedirect() {
    S3Exception exception = createS3Exception("TemporaryRedirect");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.REDIRECT, error);
  }

  @Test
  void testGetS3ErrorAccessDenied() {
    S3Exception exception = createS3Exception("AccessDenied");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.ACCESS_DENIED, error);
  }

  @Test
  void testGetS3ErrorInternalError() {
    S3Exception exception = createS3Exception("InternalError");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.INTERNAL_ERROR, error);
  }

  @Test
  void testGetS3ErrorSlowDown() {
    S3Exception exception = createS3Exception("SlowDown");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.SLOW_DOWN, error);
  }

  @Test
  void testGetS3Error503SlowDown() {
    S3Exception exception = createS3Exception("503 SlowDown");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.SLOW_DOWN, error);
  }

  @Test
  void testGetS3ErrorServiceUnavailable() {
    S3Exception exception = createS3Exception("ServiceUnavailable");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.SERVICE_UNAVAILABLE, error);
  }

  @Test
  void testGetS3ErrorRequestTimeout() {
    S3Exception exception = createS3Exception("RequestTimeout");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.REQUEST_TIMEOUT, error);
  }

  @Test
  void testGetS3ErrorUnknown() {
    S3Exception exception = createS3Exception("UnknownErrorCode");
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.UNKNOWN_ERROR, error);
  }

  @Test
  void testGetS3ErrorNullErrorDetails() {
    S3Exception exception = (S3Exception) S3Exception.builder().build();
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.UNKNOWN_ERROR, error);
  }

  @Test
  void testGetS3ErrorNullErrorCode() {
    S3Exception exception = (S3Exception) S3Exception.builder()
        .awsErrorDetails(AwsErrorDetails.builder().build())
        .build();
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(S3Error.UNKNOWN_ERROR, error);
  }

  @Test
  void testHasS3Error() {
    S3Exception exception = createS3Exception("NoSuchKey");
    assertTrue(evaluator.hasS3Error(exception, S3Error.KEY_NOT_FOUND));
    assertFalse(evaluator.hasS3Error(exception, S3Error.BUCKET_NOT_FOUND));
  }

  @Test
  void testVerifyTransientWithTransientError() {
    S3Exception exception = createS3Exception("SlowDown");
    assertDoesNotThrow(() -> evaluator.verifyTransient(exception));
  }

  @Test
  void testVerifyTransientWithNonTransientError() {
    S3Exception exception = createS3Exception("NoSuchBucket");
    assertThrows(NonTransientS3Exception.class, () -> {
      evaluator.verifyTransient(exception);
    });
  }

  @Test
  void testAllErrorCodes() {
    // Test all known error codes
    testErrorCode("NoSuchBucket", S3Error.BUCKET_NOT_FOUND);
    testErrorCode("NoSuchKey", S3Error.KEY_NOT_FOUND);
    testErrorCode("PreconditionFailed", S3Error.PRECONDITION_FAILED);
    testErrorCode("Redirect", S3Error.REDIRECT);
    testErrorCode("TemporaryRedirect", S3Error.REDIRECT);
    testErrorCode("AccessDenied", S3Error.ACCESS_DENIED);
    testErrorCode("InternalError", S3Error.INTERNAL_ERROR);
    testErrorCode("KeyTooLongError", S3Error.KEY_TOO_LONG);
    testErrorCode("InvalidBucketState", S3Error.INVALID_BUCKET_STATE);
    testErrorCode("NoSuchUpload", S3Error.NO_SUCH_UPLOAD);
    testErrorCode("ConditionalRequestConflict", S3Error.CONFLICT);
    testErrorCode("OperationAborted", S3Error.CONFLICT);
    testErrorCode("RequestTimeout", S3Error.REQUEST_TIMEOUT);
    testErrorCode("RequestTimeTooSkewed", S3Error.REQUEST_TIME_TOO_SKEWED);
    testErrorCode("ServiceUnavailable", S3Error.SERVICE_UNAVAILABLE);
    testErrorCode("SlowDown", S3Error.SLOW_DOWN);
    testErrorCode("503 SlowDown", S3Error.SLOW_DOWN);
  }

  private void testErrorCode(String errorCode, S3Error expectedError) {
    S3Exception exception = createS3Exception(errorCode);
    S3Error error = evaluator.getS3Error(exception);
    assertEquals(expectedError, error);
  }

  private S3Exception createS3Exception(String errorCode) {
    return (S3Exception) S3Exception.builder()
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode(errorCode)
            .build())
        .build();
  }
}

