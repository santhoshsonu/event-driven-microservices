package com.microservices.eventdriven.elastic.index.client.service.impl;

import com.microservices.eventdriven.config.ElasticConfigData;
import com.microservices.eventdriven.elastic.index.client.service.ElasticIndexClient;
import com.microservices.eventdriven.elastic.index.client.util.ElasticIndexUtil;
import com.microservices.eventdriven.elastic.model.index.impl.TwitterIndexModel;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexedObjectInformation;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@ConditionalOnProperty(
    name = "elastic-config.is-repository",
    havingValue = "false",
    matchIfMissing = false)
public class TwitterElasticIndexClient implements ElasticIndexClient<TwitterIndexModel> {

  private final ElasticConfigData elasticConfigData;
  private final ElasticsearchOperations operations;
  private final ElasticIndexUtil<TwitterIndexModel> elasticIndexUtil;

  @Autowired
  public TwitterElasticIndexClient(
      ElasticConfigData elasticConfigData,
      ElasticIndexUtil<TwitterIndexModel> elasticIndexUtil,
      ElasticsearchOperations operations) {
    this.elasticConfigData = elasticConfigData;
    this.elasticIndexUtil = elasticIndexUtil;
    this.operations = operations;
  }

  @Override
  public List<String> save(List<TwitterIndexModel> documents) {
    List<IndexQuery> indexQueries = elasticIndexUtil.getIndexQueries(documents);
    List<String> documentIds =
        operations
            .bulkIndex(indexQueries, IndexCoordinates.of(elasticConfigData.getIndexName()))
            .stream()
            .map(IndexedObjectInformation::getId)
            .filter(Objects::nonNull)
            .toList();
    log.info(
        "Documents indexed successfully with type: {} and ids: {}",
        TwitterIndexModel.class,
        documentIds);
    return documentIds;
  }
}
