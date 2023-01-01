package com.microservices.eventdriven.twitter.to.kafka.service.runner.impl;

import static java.util.Objects.nonNull;

import com.microservices.eventdriven.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.eventdriven.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.eventdriven.twitter.to.kafka.service.runner.IStreamRunner;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "twitter-to-kafka-service", name = "enable-mock-tweets", havingValue = "false", matchIfMissing = true)
@Slf4j
public class TwitterKafkaStreamRunner implements IStreamRunner {

  private final TwitterKafkaStreamHelper streamHelper;

  private final TwitterToKafkaServiceConfigData configData;

  public TwitterKafkaStreamRunner(TwitterKafkaStreamHelper streamHelper,
      TwitterToKafkaServiceConfigData configData) {
    this.streamHelper = streamHelper;
    this.configData = configData;
  }

  @Override
  public void start() throws TwitterToKafkaServiceException {
    final String bearerToken = configData.getTwitterBearerToken();
    if (nonNull(bearerToken) && !bearerToken.isBlank()) {
      streamHelper.setupRules(bearerToken, getRules());
      streamHelper.connectStream(bearerToken);
    } else {
      final String message = "There was a problem getting your bearer token." +
          "Please make sure you set the TWITTER_BEARER_TOKEN environment variable.";
      log.error(message);
      throw new TwitterToKafkaServiceException(message);
    }
  }

  private Map<String, String> getRules() {
    List<String> keywords = configData.getTwitterKeywords();
    return keywords.stream()
        .collect(Collectors.toMap(Function.identity(), keyword -> "keyword: " + keyword));
  }
}
