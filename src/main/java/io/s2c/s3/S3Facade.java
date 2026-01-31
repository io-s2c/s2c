package io.s2c.s3;

import java.io.IOException;

import com.google.protobuf.ByteString;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

public interface S3Facade {

  void putObject(String key, String bucket, ByteString data) throws S3Exception;
  
  GetObjectResult getObject(String key, String bucket) throws S3Exception, IOException;

  String putIfNoneMatch(String key, String bucket, ByteString data) throws S3Exception;

  String putIfMatch(String key, String bucket, ByteString data, String eTag)
      throws S3Exception;

  void deleteObject(String key, String bucket);
  
  static S3Facade createNew(S3Client s3Client) {
    return new S3FacadeImpl(s3Client);
  }
  
  static S3Facade createNewInMemory(String... buckets) {
    return new InMemoryS3FacadeImpl(buckets);
  }
}