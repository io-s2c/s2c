package io.s2c.s3;

import java.io.IOException;

import com.google.protobuf.ByteString;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3FacadeImpl implements S3Facade {
    
  private final S3Client s3Client;

  public S3FacadeImpl(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public void putObject(String key, String bucket, ByteString data) {
    s3Client.putObject(b -> b.key(key).bucket(bucket), RequestBody.fromBytes(data.toByteArray()));
  }

  @Override
  public GetObjectResult getObject(String key, String bucket) throws IOException {
    ResponseInputStream<GetObjectResponse> res = null;
    try {
      res = s3Client.getObject(b -> b.key(key).bucket(bucket));
      return new GetObjectResult(res.readAllBytes(), res.response().eTag());
    }
    finally {
      if (res != null) {
        res.release();
      }
    }
  }

  @Override
  public String putIfNoneMatch(String key, String bucket, ByteString data) {
    
    var res = s3Client.putObject(b -> b.key(key).bucket(bucket).ifNoneMatch("*"),
        RequestBody.fromBytes(data.toByteArray()));
    return res.eTag();
  }

  @Override
  public String putIfMatch(String key, String bucket, ByteString data, String eTag) {
    var res = s3Client.putObject(
        b -> b.key(key).bucket(bucket).overrideConfiguration(b2 -> b2.putHeader("If-Match", eTag)),
        RequestBody.fromBytes(data.toByteArray()));
    return res.eTag();
  }

  @Override
  public void deleteObject(String key, String bucket) {
    s3Client.deleteObject(b -> b.key(key).bucket(bucket));
  }

}
