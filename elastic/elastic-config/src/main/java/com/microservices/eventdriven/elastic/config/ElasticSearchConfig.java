package com.microservices.eventdriven.elastic.config;

import com.microservices.eventdriven.config.ElasticConfigData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchConfiguration;

@Configuration
public class ElasticSearchConfig extends ReactiveElasticsearchConfiguration {

  private final ElasticConfigData elasticConfigData;

  public ElasticSearchConfig(ElasticConfigData elasticConfigData) {
    this.elasticConfigData = elasticConfigData;
  }

  @Override
  @Bean
  public ClientConfiguration clientConfiguration() {
    return ClientConfiguration.builder().connectedTo(elasticConfigData.getConnectionUrl())
        .withConnectTimeout(elasticConfigData.getConnectionTimeoutMs())
        .withSocketTimeout(elasticConfigData.getSocketTimeoutMs()).build();
  }
}