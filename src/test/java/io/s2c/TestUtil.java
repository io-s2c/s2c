package io.s2c;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.s2c.s3.S3Facade;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class TestUtil {

  private static final String S3_BACKEND_PROPERTY = "s3backend";

  private static LocalStackContainer localStack;
  private static S3Client s3Client;

  enum S3Backend {
    IN_MEMORY, LOCALSTACK
  }

  public static S3Facade createS3Facade(String bucket) {
    if (getS3Backend()  == S3Backend.LOCALSTACK) {
      ensureLocalStackStarted();
      s3Client.createBucket(b -> b.bucket(bucket));
      return S3Facade.createNew(s3Client);
    }
    return S3Facade.createNewInMemory(bucket);
  }

  public static S3Backend getS3Backend() {
    // Localstack is default
    String propertyValue = System.getProperty(S3_BACKEND_PROPERTY);
    if (propertyValue == null) {
      return S3Backend.IN_MEMORY;
    }
    if ("localstack".equalsIgnoreCase(propertyValue)) {
      return S3Backend.LOCALSTACK;
    } else if ("inmemory".equalsIgnoreCase(propertyValue)) {
      return S3Backend.IN_MEMORY;
    } else {
      throw new IllegalArgumentException("Illegal S3 backend: %s".formatted(propertyValue));
    }
  }

  public static void cleanupBucket(String bucket)
      throws UnsupportedOperationException, IOException, InterruptedException {
    if (getS3Backend() == S3Backend.LOCALSTACK && localStack != null) {
      localStack.execInContainer("awslocal", "s3", "rb", "s3://" + bucket, "--force");
    }
  }

  public static void stopLocalStack() {
    if (localStack != null) {
      localStack.stop();
      localStack.close();
      localStack = null;
      s3Client = null;
    }
  }

  private static synchronized void ensureLocalStackStarted() {
    if (localStack == null) {
      localStack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
          .withServices("s3");
      localStack.start();
      s3Client = S3Client.builder()
          .endpointOverride(localStack.getEndpoint())
          .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
          .region(Region.of(localStack.getRegion()))
          .build();
    }
  }

  public static <T> void sleepUntil(int maxTotalWaitSec, int stepMs, Supplier<Boolean> until)
      throws InterruptedException {

    long maxTotalWait = TimeUnit.SECONDS.toNanos(maxTotalWaitSec);

    long elapsedTime = 0;

    while (elapsedTime < maxTotalWait) {

      TimeUnit.MILLISECONDS.sleep(stepMs);

      if (until.get() == true)
        break;

      elapsedTime += TimeUnit.MILLISECONDS.toNanos(stepMs);

    }
  }
}
