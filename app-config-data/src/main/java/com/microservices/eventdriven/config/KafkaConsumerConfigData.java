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
  private int concurrencyLevel;
  private long sessionTimeoutMs;
  private long heartbeatIntervalMs;
  private long maxPollIntervalMs;
  private int maxPollRecords;
  private long maxPartitionFetchBytesDefault;
  private int maxPartitionFetchBytesBoostFactor;
  private long pollTimeoutMs;
}