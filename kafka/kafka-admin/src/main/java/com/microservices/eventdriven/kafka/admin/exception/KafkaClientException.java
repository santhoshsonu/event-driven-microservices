package com.microservices.eventdriven.kafka.admin.exception;

public class KafkaClientException extends RuntimeException {

  public KafkaClientException(String msg) {
    super(msg);
  }

  public KafkaClientException(String msg, Throwable t) {
    super(msg, t);
  }
}