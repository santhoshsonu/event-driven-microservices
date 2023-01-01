package com.microservices.eventdriven.twitter.to.kafka.service.runner;

import com.microservices.eventdriven.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;

public interface IStreamRunner {

  void start() throws TwitterToKafkaServiceException;
}
