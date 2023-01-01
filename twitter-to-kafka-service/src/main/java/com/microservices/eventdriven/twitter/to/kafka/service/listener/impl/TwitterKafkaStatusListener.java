package com.microservices.eventdriven.twitter.to.kafka.service.listener.impl;

import com.microservices.eventdriven.twitter.to.kafka.service.listener.ITwitterKafkaStatusListener;
import com.microservices.eventdriven.twitter.to.kafka.service.model.Tweet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TwitterKafkaStatusListener implements ITwitterKafkaStatusListener {

  @Override
  public void onStatus(Tweet data) {
    log.info("Tweet: {}", data);
  }
}
