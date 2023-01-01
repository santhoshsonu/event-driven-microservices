package com.microservices.eventdriven.twitter.to.kafka.service.exception;

public class TwitterToKafkaServiceException extends RuntimeException {

  public TwitterToKafkaServiceException(String message) {
    super(message);
  }

  public TwitterToKafkaServiceException(String message, Throwable cause) {
    super(message, cause);
  }
}