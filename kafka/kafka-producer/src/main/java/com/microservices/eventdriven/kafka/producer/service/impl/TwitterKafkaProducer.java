package com.microservices.eventdriven.kafka.producer.service.impl;

import static java.util.Objects.nonNull;

import com.microservices.eventdriven.kafka.avro.model.TwitterAvroModel;
import com.microservices.eventdriven.kafka.producer.service.KafkaProducer;
import jakarta.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TwitterKafkaProducer implements
    KafkaProducer<Long, TwitterAvroModel> {

  private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

  public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @Override
  public void send(String topicName, Long key, TwitterAvroModel message) {
    log.info("Sending message: {} to topic: {}", message, topicName);

    CompletableFuture<SendResult<Long, TwitterAvroModel>> completableFuture = kafkaTemplate.send(
        topicName, key, message);
    completableFuture.whenCompleteAsync(
        ((result, throwable) -> callback(topicName, message, result, throwable)));
  }

  @PreDestroy
  private void close() {
    if (nonNull(kafkaTemplate)) {
      log.info("Shutting down Kafka Producer...");
      kafkaTemplate.destroy();
    }
  }

  private void callback(String topicName, TwitterAvroModel message,
      SendResult<Long, TwitterAvroModel> result, Throwable throwable) {
    if (nonNull(throwable)) {
      log.error("Error while sending message: {} to topic: {}", message, topicName, throwable);
    } else {
      RecordMetadata metadata = result.getRecordMetadata();
      log.debug(
          "Message sent successfully. Topic: {} Partition: {} Offset:{} timestamp: {}, at Time: {}",
          metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(),
          LocalDateTime.now());
    }
  }
}
