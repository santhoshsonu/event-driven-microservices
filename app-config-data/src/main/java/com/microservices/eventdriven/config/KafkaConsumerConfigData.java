package com.microservices.eventdriven.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka-consumer-config")
public class KafkaConsumerConfigData {

  private String keyDeserializer;
  private String valueDeserializer;
  private String consumerGroupId;
  private String autoOffsetReset;
  private String specificAvroReaderKey;
  private boolean specificAvroReader;
  private boolean batchListener;
  private boolean autoStartup;
  private Integer concurrencyLevel;
  private Integer sessionTimeoutMs;
  private Integer heartbeatIntervalMs;
  private Integer maxPollIntervalMs;
  private Integer maxPollRecords;
  private Integer maxPartitionFetchBytesDefault;
  private Integer maxPartitionFetchBytesBoostFactor;
  private long pollTimeoutMs;
}