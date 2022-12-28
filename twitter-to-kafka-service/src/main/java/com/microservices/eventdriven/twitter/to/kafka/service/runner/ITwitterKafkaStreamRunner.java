package com.microservices.eventdriven.twitter.to.kafka.service.runner;

public interface ITwitterKafkaStreamRunner {

  void start();

  void shutdown();
}
