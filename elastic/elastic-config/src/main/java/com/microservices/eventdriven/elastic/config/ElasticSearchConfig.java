package com.microservices.eventdriven.elastic.config;

import com.microservices.eventdriven.config.ElasticConfigData;
import lombok.NonNull;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

@Configuration
@EnableElasticsearchRepositories(
    basePackages = "com.microservices.eventdriven.elastic.index.client.repository")
public class ElasticSearchConfig extends ElasticsearchConfiguration {

  private final ElasticConfigData elasticConfigData;

  public ElasticSearchConfig(ElasticConfigData elasticConfigData) {
    this.elasticConfigData = elasticConfigData;
  }

  @Override
  @NonNull
  public ClientConfiguration clientConfiguration() {
    return ClientConfiguration.builder()
        .connectedTo(elasticConfigData.getConnectionUrl())
        .withConnectTimeout(elasticConfigData.getConnectionTimeoutMs())
        .withSocketTimeout(elasticConfigData.getSocketTimeoutMs())
        .build();
  }
}
