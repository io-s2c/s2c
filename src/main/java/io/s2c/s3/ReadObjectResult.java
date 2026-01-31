package io.s2c.s3;

public record ReadObjectResult<T>(T object, String eTag) {

}
