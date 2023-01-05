package com.microservices.eventdriven.kafka.admin.client;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import com.microservices.eventdriven.config.KafkaConfigData;
import com.microservices.eventdriven.config.RetryConfigData;
import com.microservices.eventdriven.kafka.admin.exception.KafkaClientException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Slf4j
@Component
public class KafkaAdminClient {

  private final KafkaConfigData kafkaConfigData;
  private final RetryConfigData retryConfigData;
  private final Admin adminClient;
  private final RetryTemplate retryTemplate;
  private final WebClient webClient;

  public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData,
      Admin adminClient, RetryTemplate retryTemplate, WebClient webClient) {
    this.kafkaConfigData = kafkaConfigData;
    this.retryConfigData = retryConfigData;
    this.adminClient = adminClient;
    this.retryTemplate = retryTemplate;
    this.webClient = webClient;
  }

  public void createTopics() {
    CreateTopicsResult createTopicsResult;
    try {
      createTopicsResult = retryTemplate.execute(this::doCreateTopics);
      log.info("Create Topic Result: {}", createTopicsResult.values().values());
    } catch (Throwable e) {
      throw new KafkaClientException("Exceeded max retry attempts to create kafka topic(s)", e);
    }
    checkTopicsCreated();
  }

  public void checkTopicsCreated() {
    Collection<TopicListing> topics = getTopics();
    int retryCount = 1;
    Integer maxAttempts = retryConfigData.getMaxAttempts();
    int multiplier = retryConfigData.getMultiplier().intValue();
    Long sleepTimeMs = retryConfigData.getSleepTimeMs();
    for (String topicName : kafkaConfigData.getTopicNamesToCreate()) {
      while (!isTopicCreated(topics, topicName)) {
        checkMaxRetry(retryCount++, maxAttempts);
        sleep(sleepTimeMs);
        sleepTimeMs *= multiplier;
        topics = getTopics();
      }
    }
  }

  public void checkSchemaRegistry() {
    int retryCount = 1;
    Integer maxAttempts = retryConfigData.getMaxAttempts();
    int multiplier = retryConfigData.getMultiplier().intValue();
    Long sleepTimeMs = retryConfigData.getSleepTimeMs();
    while (!getSchemaRegistryStatus().is2xxSuccessful()) {
      checkMaxRetry(retryCount++, maxAttempts);
      sleep(sleepTimeMs);
      sleepTimeMs *= multiplier;
    }
  }

  private HttpStatus getSchemaRegistryStatus() {
    try {
      return webClient.get().uri(kafkaConfigData.getSchemaRegistryUrl())
          .retrieve().toBodilessEntity()
          .map(clientResponse -> HttpStatus.valueOf(clientResponse.getStatusCode().value()))
          .block();
    } catch (Exception e) {
      return HttpStatus.SERVICE_UNAVAILABLE;
    }
  }

  private void sleep(Long sleepTimeMs) {
    try {
      Thread.sleep(sleepTimeMs);
    } catch (InterruptedException e) {
      throw new KafkaClientException("Error while sleeping for waiting new created topic(s)", e);
    }
  }

  private void checkMaxRetry(int retryCount, Integer maxAttempts) {
    if (retryCount > maxAttempts) {
      throw new KafkaClientException("Exceeded max retry attempts to read kafka topic(s)");
    }
  }

  private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
    if (isNull(topics)) {
      return false;
    }
    return topics.stream().anyMatch(topicListing -> topicListing.name().equals(topicName));
  }

  private Collection<TopicListing> getTopics() {
    Collection<TopicListing> topics;
    try {
      topics = retryTemplate.execute(this::doGetTopics);
    } catch (Throwable e) {
      throw new KafkaClientException("Exceeded max retry attempts to read kafka topic(s)", e);
    }
    return topics;
  }

  private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
    List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
    log.info("Attempt: {} :: Creating {} topic(s)", retryContext.getRetryCount(),
        topicNames.size());
    List<NewTopic> kafkaTopics = topicNames.stream().map(
        topic -> new NewTopic(topic.trim(), kafkaConfigData.getNumOfPartitions(),
            kafkaConfigData.getReplicationFactor())).toList();
    return adminClient.createTopics(kafkaTopics);
  }

  private Collection<TopicListing> doGetTopics(RetryContext retryContext)
      throws ExecutionException, InterruptedException {
    log.info("Attempt: {} :: Reading kafka topic(s) {}", retryContext.getRetryCount(),
        kafkaConfigData.getTopicNamesToCreate());
    Collection<TopicListing> topics = adminClient.listTopics().listings().get();
    if (nonNull(topics)) {
      log.debug("List of kafka topic(s): ");
      topics.forEach(t -> log.debug("Topic Name: {}", t.name()));
    }
    return topics;
  }
}
