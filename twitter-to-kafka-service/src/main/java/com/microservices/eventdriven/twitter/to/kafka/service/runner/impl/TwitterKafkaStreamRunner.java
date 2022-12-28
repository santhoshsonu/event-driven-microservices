package com.microservices.eventdriven.twitter.to.kafka.service.runner.impl;

import static java.util.Objects.nonNull;

import com.microservices.eventdriven.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.eventdriven.twitter.to.kafka.service.listener.StreamingTweetHandler;
import com.microservices.eventdriven.twitter.to.kafka.service.listener.TweetsStreamListenersExecutor;
import com.microservices.eventdriven.twitter.to.kafka.service.listener.impl.StreamingTweetHandlerImpl;
import com.microservices.eventdriven.twitter.to.kafka.service.runner.ITwitterKafkaStreamRunner;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.AddOrDeleteRulesRequest;
import com.twitter.clientlib.model.AddOrDeleteRulesResponse;
import com.twitter.clientlib.model.AddRulesRequest;
import com.twitter.clientlib.model.DeleteRulesRequest;
import com.twitter.clientlib.model.DeleteRulesRequestDelete;
import com.twitter.clientlib.model.Rule;
import com.twitter.clientlib.model.RuleNoId;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TwitterKafkaStreamRunner implements ITwitterKafkaStreamRunner {

  private final TwitterToKafkaServiceConfigData configData;
  private TwitterApi twitterInstance;
  private TweetsStreamListenersExecutor tsle;

  public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData configData) {
    this.configData = configData;
  }

  @Override
  public void start() {
    twitterInstance = new TwitterApi(
        new TwitterCredentialsBearer(configData.getTwitterBearerToken()));
    StreamingTweetHandler streamingTweetHandler = new StreamingTweetHandlerImpl(twitterInstance);

    try {
      // Important: Add rules to filter twitter stream before executing
      addFilters();
      tsle = new TweetsStreamListenersExecutor();
      tsle.stream().streamingHandler(streamingTweetHandler).executeListeners();
    } catch (ApiException e) {
      log.error("Status code: {} Reason: {} Response headers: {}", e.getCode(), e.getResponseBody(),
          e.getResponseHeaders());
    }
  }

  private void addFilters() throws ApiException {
    List<Rule> rules = twitterInstance.tweets().getRules().execute().getData();
    // Delete Existing rules
    if (nonNull(rules) && rules.size() > 0) {
      JSONObject deleteRulesObject = new JSONObject().put(
          DeleteRulesRequest.SERIALIZED_NAME_DELETE,
          new JSONObject().put(DeleteRulesRequestDelete.SERIALIZED_NAME_IDS,
              rules.stream().map(Rule::getId).toList()));

      try {
        twitterInstance.tweets().addOrDeleteRules(
                new AddOrDeleteRulesRequest(
                    DeleteRulesRequest.fromJson(deleteRulesObject.toString())))
            .execute();
        log.info("Successfully deleted existing rules");
      } catch (IOException ex) {
        log.error("Error creating delete rules request.", ex);
        throw new ApiException("Error deleting existing rules");
      }
    }

    // Create new rules
    List<RuleNoId> newRuleList = configData.getTwitterKeywords().stream().map(keyword -> {
      RuleNoId rule = new RuleNoId();
      rule.setValue(keyword);
      rule.setTag(keyword);
      return rule;
    }).toList();

    AddOrDeleteRulesResponse addOrDeleteRulesResponse = twitterInstance.tweets()
        .addOrDeleteRules(new AddOrDeleteRulesRequest(new AddRulesRequest().add(newRuleList)))
        .execute();

    if (nonNull(addOrDeleteRulesResponse.getErrors())
        && addOrDeleteRulesResponse.getErrors().size() > 0) {
      addOrDeleteRulesResponse.getErrors().forEach(problem -> log.error(problem.toString()));
      throw new ApiException("Failed to create filtering rules.");
    }
    log.info("Successfully created new rules: {}",
        newRuleList.stream().map(RuleNoId::getValue).toList());
  }

  @Override
  @PreDestroy
  public void shutdown() {
    if (nonNull(tsle)) {
      tsle.shutdown();
    }
  }
}
