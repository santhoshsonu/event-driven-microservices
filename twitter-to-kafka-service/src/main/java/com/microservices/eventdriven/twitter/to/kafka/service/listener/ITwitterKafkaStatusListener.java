package com.microservices.eventdriven.twitter.to.kafka.service.listener;

import com.microservices.eventdriven.twitter.to.kafka.service.model.Tweet;

public interface ITwitterKafkaStatusListener {

  void onStatus(Tweet data);

}
