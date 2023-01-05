package com.microservices.eventdriven.twitter.to.kafka.service.init.impl;

import com.microservices.eventdriven.kafka.admin.client.KafkaAdminClient;
import com.microservices.eventdriven.twitter.to.kafka.service.init.StreamInitializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaStreamInitializer implements StreamInitializer {

  private final KafkaAdminClient kafkaAdminClient;

  public KafkaStreamInitializer(KafkaAdminClient kafkaAdminClient) {
    this.kafkaAdminClient = kafkaAdminClient;
  }

  @Override
  public void init() {
    kafkaAdminClient.createTopics();
    kafkaAdminClient.checkSchemaRegistry();
  }
}
