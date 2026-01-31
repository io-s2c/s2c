package io.s2c.s3;

public record GetObjectResult(byte[] data, String eTag) {
}
