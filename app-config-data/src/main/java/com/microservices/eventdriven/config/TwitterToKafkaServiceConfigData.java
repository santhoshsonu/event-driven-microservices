package com.microservices.eventdriven.config;

import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties("twitter-to-kafka-service")
public class TwitterToKafkaServiceConfigData {

  private List<String> twitterKeywords;
  private String twitterBearerToken;
  private String twitterBaseUrl;
  private String twitterRulesBaseUrl;
  private Integer mockTweetMinLength;
  private Integer mockTweetMaxLength;
  private Long mockTweetSleepMs;
}
