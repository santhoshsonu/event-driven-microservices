package com.microservices.eventdriven.kafka.to.elastic.service.consumer.impl;

import com.microservices.eventdriven.config.KafkaConsumerConfigData;
import com.microservices.eventdriven.kafka.admin.client.KafkaAdminClient;
import com.microservices.eventdriven.kafka.avro.model.TwitterAvroModel;
import com.microservices.eventdriven.kafka.to.elastic.service.consumer.KafkaConsumer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TwitterKafkaConsumer implements KafkaConsumer<Long, TwitterAvroModel> {

  private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  private final KafkaAdminClient kafkaAdminClient;
  private final KafkaConsumerConfigData kafkaConsumerConfigData;

  public TwitterKafkaConsumer(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
      KafkaAdminClient kafkaAdminClient, KafkaConsumerConfigData kafkaConsumerConfigData) {
    this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    this.kafkaAdminClient = kafkaAdminClient;
    this.kafkaConsumerConfigData = kafkaConsumerConfigData;
  }

  @Override
  @KafkaListener(id = "twitterTopicListener", topics = "${kafka-config.topic-name}")
  public void receive(@Payload List<TwitterAvroModel> messages,
      @Header(KafkaHeaders.RECEIVED_KEY) List<Integer> keys,
      @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
      @Header(KafkaHeaders.OFFSET) List<Long> offsets) {
    log.info("Received {} messages with keys {}, partitions {} and offsets {},"
            + "sending to elastic: Thread ID: {}", messages.size(), keys, partitions, offsets,
        Thread.currentThread().threadId());
  }
}