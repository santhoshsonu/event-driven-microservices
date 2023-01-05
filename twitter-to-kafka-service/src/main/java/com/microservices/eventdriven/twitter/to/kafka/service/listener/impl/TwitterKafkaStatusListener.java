package com.microservices.eventdriven.twitter.to.kafka.service.listener.impl;

import com.microservices.eventdriven.config.KafkaConfigData;
import com.microservices.eventdriven.kafka.avro.model.TwitterAvroModel;
import com.microservices.eventdriven.kafka.producer.service.KafkaProducer;
import com.microservices.eventdriven.twitter.to.kafka.service.listener.ITwitterKafkaStatusListener;
import com.microservices.eventdriven.twitter.to.kafka.service.mapper.TweetToTwitterAvroRecordMapper;
import com.microservices.eventdriven.twitter.to.kafka.service.model.Tweet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TwitterKafkaStatusListener implements ITwitterKafkaStatusListener {

  private final KafkaConfigData kafkaConfigData;
  private final TweetToTwitterAvroRecordMapper mapper;

  private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

  public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData,
      TweetToTwitterAvroRecordMapper mapper, KafkaProducer<Long, TwitterAvroModel> kafkaProducer) {
    this.kafkaConfigData = kafkaConfigData;
    this.mapper = mapper;
    this.kafkaProducer = kafkaProducer;
  }

  @Override
  public void onStatus(Tweet data) {
    log.info("Received Tweet: {} sending to kafka topic: {}", data, kafkaConfigData.getTopicName());
    TwitterAvroModel tweetAvroRecord = mapper.toTwitterAvroRecord(data);
    kafkaProducer.send(kafkaConfigData.getTopicName(), tweetAvroRecord.getUserId(),
        tweetAvroRecord);
  }
}
