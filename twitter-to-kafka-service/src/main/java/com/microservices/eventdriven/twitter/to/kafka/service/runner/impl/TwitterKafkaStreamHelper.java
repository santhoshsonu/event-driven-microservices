package com.microservices.eventdriven.twitter.to.kafka.service.runner.impl;

import static java.util.Objects.nonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microservices.eventdriven.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.eventdriven.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.eventdriven.twitter.to.kafka.service.listener.ITwitterKafkaStatusListener;
import com.microservices.eventdriven.twitter.to.kafka.service.model.Tweet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "twitter-to-kafka-service", name = "enable-mock-tweets", havingValue = "false", matchIfMissing = true)
@Slf4j
public class TwitterKafkaStreamHelper {

  private final TwitterToKafkaServiceConfigData configData;
  private final ITwitterKafkaStatusListener statusListener;

  private final HttpClient httpClient;

  public TwitterKafkaStreamHelper(TwitterToKafkaServiceConfigData configData,
      ITwitterKafkaStatusListener statusListener) {
    this.configData = configData;
    this.statusListener = statusListener;
    this.httpClient = HttpClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
        .build();
  }

  /*
   * This method calls the filtered stream endpoint and streams Tweets from it
   * */
  void connectStream(String bearerToken) throws TwitterToKafkaServiceException {
    URIBuilder uriBuilder;
    HttpGet httpGet;
    HttpResponse response;

    try {
      uriBuilder = new URIBuilder(configData.getTwitterBaseUrl());
      httpGet = new HttpGet(uriBuilder.build());
      httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
    } catch (URISyntaxException e) {
      throw new TwitterToKafkaServiceException("Failed to build twitter api.", e);
    }

    try {
      response = httpClient.execute(httpGet);
      HttpEntity entity = response.getEntity();
      if (nonNull(entity)) {
        BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
        String line = reader.readLine();
        while (line != null) {
          line = reader.readLine();
          if (!line.isEmpty()) {
            statusListener.onStatus(getFormattedTweet(line));
          }
        }
      }
    } catch (IOException e) {
      throw new TwitterToKafkaServiceException("Error while filtering twitter stream", e);
    }
  }

  /*
   * Helper method to setup rules before streaming data
   * */
  void setupRules(String bearerToken, Map<String, String> rules) {
    log.info("Setting up filtering rules for twitter...");
    try {
      List<String> existingRules = getRules(bearerToken);
      if (existingRules.size() > 0) {
        deleteRules(bearerToken, existingRules);
      }
      createRules(bearerToken, rules);
    } catch (URISyntaxException | IOException e) {
      throw new TwitterToKafkaServiceException("Error while setting up filtering rules", e);
    }
  }

  /*
   * Helper method to get existing rules
   * */
  private List<String> getRules(String bearerToken) throws URISyntaxException, IOException {
    List<String> rules = new ArrayList<>();
    URIBuilder uriBuilder = new URIBuilder(configData.getTwitterRulesBaseUrl());

    HttpGet httpGet = new HttpGet(uriBuilder.build());
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
    httpGet.setHeader("content-type", "application/json");
    HttpResponse response = httpClient.execute(httpGet);
    final int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode != 200) {
      throw new IOException(String.format(
          "Twitter Api Exception while fetching existing rules. Status code: %s Reason: %s",
          statusCode,
          response.getStatusLine().getReasonPhrase()));
    }
    HttpEntity entity = response.getEntity();
    if (nonNull(entity)) {
      JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
      if (json.length() > 1 && json.has("data")) {
        JSONArray array = (JSONArray) json.get("data");
        array.forEach(obj -> {
          JSONObject jsonObject = (JSONObject) obj;
          rules.add(jsonObject.getString("id"));
        });
      }
    }
    return rules;
  }


  /*
   * Helper method to delete rules
   * */
  private void deleteRules(String bearerToken, List<String> existingRules)
      throws URISyntaxException, IOException {
    log.info("Deleting existing filtering rules for twitter...");
    URIBuilder uriBuilder = new URIBuilder(configData.getTwitterRulesBaseUrl());

    HttpPost httpPost = new HttpPost(uriBuilder.build());
    httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
    httpPost.setHeader("content-type", "application/json");
    StringEntity body = new StringEntity(
        getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
    httpPost.setEntity(body);
    HttpResponse response = httpClient.execute(httpPost);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new IOException(String.format(
          "Twitter Api Exception while deleting existing rules. Status code: %s Reason: %s",
          response.getStatusLine().getStatusCode(),
          response.getStatusLine().getReasonPhrase()));
    }
    HttpEntity entity = response.getEntity();
    if (nonNull(entity)) {
      log.debug("DELETE rules Api Response: {}", EntityUtils.toString(entity, "UTF-8"));
    }
  }

  /*
   * Helper method to create rules for filtering
   * */
  private void createRules(String bearerToken, Map<String, String> rules)
      throws URISyntaxException, IOException {
    log.info("Creating new filtering rules for twitter...");
    URIBuilder uriBuilder = new URIBuilder(configData.getTwitterRulesBaseUrl());

    HttpPost httpPost = new HttpPost(uriBuilder.build());
    httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
    httpPost.setHeader("content-type", "application/json");
    StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules));
    httpPost.setEntity(body);
    HttpResponse response = httpClient.execute(httpPost);
    if (response.getStatusLine().getStatusCode() != 201) {
      throw new IOException(String.format(
          "Twitter Api exception while creating new rules. Status code: %s Reason: %s",
          response.getStatusLine().getStatusCode(),
          response.getStatusLine().getReasonPhrase()));
    }
    HttpEntity entity = response.getEntity();
    if (nonNull(entity)) {
      log.debug("CREATE rules Api Response: {}", EntityUtils.toString(entity, "UTF-8"));
    }
  }

  private String getFormattedString(String string, List<String> ids) {
    String sb;
    if (ids.size() == 1) {
      return String.format(string, "\"" + ids.get(0) + "\"");
    } else {
      sb = ids.stream().map(id -> "\"" + id + "\",").collect(Collectors.joining());
      return String.format(string, sb.substring(0, sb.length() - 1));
    }
  }

  private String getFormattedString(String string, Map<String, String> rules) {
    StringBuilder sb = new StringBuilder();
    if (rules.size() == 1) {
      String key = rules.keySet().iterator().next();
      return String.format(string,
          "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");
    } else {
      for (Map.Entry<String, String> entry : rules.entrySet()) {
        String value = entry.getKey();
        String tag = entry.getValue();
        sb.append("{\"value\": \"").append(value).append("\", \"tag\": \"").append(tag)
            .append("\"},");
      }
      return String.format(string, sb.substring(0, sb.length() - 1));
    }
  }

  private Tweet getFormattedTweet(String data) throws JsonProcessingException {
    JSONObject jsonData = (JSONObject) new JSONObject(data).get("data");
    return Tweet.fromJson(jsonData.toString());
  }
}
