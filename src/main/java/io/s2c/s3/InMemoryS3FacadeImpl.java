package io.s2c.s3;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class InMemoryS3FacadeImpl implements S3Facade {

  private final ConcurrentHashMap<String, ConcurrentHashMap<String, InMemoryS3Object>> store = new ConcurrentHashMap<>();

  InMemoryS3FacadeImpl(String... buckets) {
    for (String bucket : buckets) {
      store.put(bucket, new ConcurrentHashMap<>());
    }
  }

  private static record InMemoryS3Object(ByteString data, String eTag) {
    public static InMemoryS3Object create(ByteString data) {
      return new InMemoryS3Object(data, UUID.randomUUID().toString());
    }
  }

  @Override
  public void putObject(String key, String bucket, ByteString data) throws S3Exception {
    store.get(bucket).compute(key, (k, v) -> InMemoryS3Object.create(data));
  }

  @Override
  public GetObjectResult getObject(String key, String bucket) throws IOException {

    var obj = store.get(bucket).compute(key, (k, v) -> {
      if (v == null) {
        throw S3Exception.builder()
            .awsErrorDetails(
                AwsErrorDetails.builder().errorCode(S3Error.KEY_NOT_FOUND.errorCode()).build())
            .build();
      }
      return v;
    });

    return new GetObjectResult(obj.data().toByteArray(), obj.eTag());

  }

  @Override
  public String putIfMatch(String key, String bucket, ByteString data, String eTag) {
    var obj = store.get(bucket).computeIfPresent(key, (k, v) -> {
      if (v.eTag().equals(eTag)) {
        return InMemoryS3Object.create(data);
      } else {
        throw S3Exception.builder()
            .awsErrorDetails(AwsErrorDetails.builder()
                .errorCode(S3Error.PRECONDITION_FAILED.errorCode())
                .build())
            .build();
      }
    });
    return obj.eTag();

  }

  @Override
  public String putIfNoneMatch(String key, String bucket, ByteString data) {
    var obj = store.get(bucket).compute(key, (k, v) -> {
      if (v == null)
        return InMemoryS3Object.create(data);
      throw S3Exception.builder()
          .awsErrorDetails(
              AwsErrorDetails.builder().errorCode(S3Error.PRECONDITION_FAILED.errorCode()).build())
          .build();
    });
    return obj.eTag();
  }

  @Override
  public void deleteObject(String key, String bucket) {
    store.get(bucket).remove(key);
  }

}
