package com.microservices.eventdriven.twitter.to.kafka.service.runner.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microservices.eventdriven.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.eventdriven.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.eventdriven.twitter.to.kafka.service.listener.ITwitterKafkaStatusListener;
import com.microservices.eventdriven.twitter.to.kafka.service.model.Tweet;
import com.microservices.eventdriven.twitter.to.kafka.service.runner.IStreamRunner;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "twitter-to-kafka-service", name = "enable-mock-tweets", havingValue = "true")
@Slf4j
public class MockTwitterKafkaStreamRunner implements IStreamRunner {

  private static final Random RANDOM = new Random();
  private static final String[] WORDS = new String[]{
      "Lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
      "Integer", "non", "ante", "vel", "tellus", "sollicitudin", "semper", "in", "mauris"
  };
  private static final String TWEET_RAW_JSON_FORMAT = "{" +
      "\"created_at\": \"{0}\"," +
      "\"id\": \"{1}\"," +
      "\"text\": \"{2}\"," +
      "\"author_id\": \"{3}\"" +
      "}";
  private final TwitterToKafkaServiceConfigData configData;
  private final ITwitterKafkaStatusListener statusListener;

  public MockTwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData configData,
      ITwitterKafkaStatusListener statusListener) {
    this.configData = configData;
    this.statusListener = statusListener;
  }

  @Override
  public void start() {
    String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
    log.info("Starting mock filtering twitter streams for keywords: {}",
        Arrays.stream(keywords).toList());
    Integer minLength = configData.getMockTweetMinLength();
    Integer maxLength = configData.getMockTweetMaxLength();
    Long sleepMs = configData.getMockTweetSleepMs();
    simulateTwitterStream(keywords, minLength, maxLength, sleepMs);
  }

  private void sleep(long sleepTimeMs) {
    try {
      Thread.sleep(sleepTimeMs);
    } catch (InterruptedException e) {
      throw new TwitterToKafkaServiceException("Error while sleeping for waiting new tweet", e);
    }
  }

  private void simulateTwitterStream(String[] keywords, int minLength, int maxLength,
      long sleepTimeMs) {
    Executors.newSingleThreadExecutor().submit(() -> {
      try {
        while (true) {
          final String formattedTweet = getFormattedTweet(keywords, minLength, maxLength);
          statusListener.onStatus(Tweet.fromJson(formattedTweet));
          sleep(sleepTimeMs);
        }
      } catch (JsonProcessingException e) {
        log.error("Error parsing json data to Tweet object", e);
      }
    });
  }

  private String getFormattedTweet(String[] keywords, Integer minLength, Integer maxLength) {
    String[] params = new String[]{
        ZonedDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME),
        String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        getRandomTweetContent(keywords, minLength, maxLength),
        String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
    };
    return formatTweetAsJsonWithParams(params);
  }

  private String getRandomTweetContent(String[] keywords, Integer minLength, Integer maxLength) {
    StringBuilder tweetContent = new StringBuilder();
    final int tweetLength = RANDOM.nextInt(maxLength - minLength + 1) + minLength;
    return constructRandomTweet(keywords, tweetContent, tweetLength);
  }

  private String formatTweetAsJsonWithParams(String[] params) {
    StringBuilder tweet = new StringBuilder(TWEET_RAW_JSON_FORMAT);
    final int length = params.length;
    for (int i = 0; i < length; i++) {
      final int index = tweet.indexOf("{" + i + "}");
      tweet.replace(index, index + 3, params[i]);
    }
    return tweet.toString();
  }

  private String constructRandomTweet(String[] keywords, StringBuilder tweetContent,
      int tweetLength) {
    int wordsLength = WORDS.length;
    for (int i = 0; i < tweetLength; i++) {
      tweetContent.append(WORDS[RANDOM.nextInt(wordsLength)]).append(" ");
      if (i == tweetLength / 2) {
        tweetContent.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
      }
    }
    return tweetContent.toString().trim();
  }
}
