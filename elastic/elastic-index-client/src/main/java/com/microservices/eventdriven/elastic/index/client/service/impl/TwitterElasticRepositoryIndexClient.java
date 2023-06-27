package com.microservices.eventdriven.elastic.index.client.service.impl;

import com.microservices.eventdriven.elastic.index.client.repository.TwitterElasticsearchIndexRepository;
import com.microservices.eventdriven.elastic.index.client.service.ElasticIndexClient;
import com.microservices.eventdriven.elastic.model.index.impl.TwitterIndexModel;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@ConditionalOnProperty(
    name = "elastic-config.is-repository",
    havingValue = "true",
    matchIfMissing = true)
public class TwitterElasticRepositoryIndexClient implements ElasticIndexClient<TwitterIndexModel> {
  private final TwitterElasticsearchIndexRepository repository;

  public TwitterElasticRepositoryIndexClient(TwitterElasticsearchIndexRepository repository) {
    this.repository = repository;
  }

  @Override
  public List<String> save(List<TwitterIndexModel> documents) {
    List<TwitterIndexModel> twitterIndexModels =
        (List<TwitterIndexModel>) repository.saveAll(documents);
    List<String> documentIds = twitterIndexModels.stream().map(TwitterIndexModel::getId).toList();
    log.info(
        "Documents indexed successfully with type: {} and ids: {}",
        TwitterIndexModel.class,
        documentIds);
    return documentIds;
  }
}
