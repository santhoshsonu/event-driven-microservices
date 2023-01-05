package com.microservices.eventdriven.twitter.to.kafka.service.mapper;

import com.microservices.eventdriven.kafka.avro.model.TwitterAvroModel;
import com.microservices.eventdriven.twitter.to.kafka.service.model.Tweet;
import org.springframework.stereotype.Component;

@Component
public class TweetToTwitterAvroRecordMapper {

  public TwitterAvroModel toTwitterAvroRecord(Tweet tweet) {
    return TwitterAvroModel.newBuilder()
        .setId(tweet.getId())
        .setText(tweet.getText())
        .setUserId(tweet.getUser().getId())
        .setCreatedAt(tweet.getCreatedAt().toEpochSecond())
        .build();
  }
}
