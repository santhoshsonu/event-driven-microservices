package com.microservices.eventdriven.kafka.admin.config;

import com.microservices.eventdriven.config.KafkaConfigData;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

@EnableRetry
@Configuration
public class KafkaAdminConfig {

  private final KafkaConfigData kafkaConfigData;

  public KafkaAdminConfig(KafkaConfigData kafkaConfigData) {
    this.kafkaConfigData = kafkaConfigData;
  }

  @Bean
  public Admin admin() {
    return Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        kafkaConfigData.getBootstrapServers()));
  }
}
