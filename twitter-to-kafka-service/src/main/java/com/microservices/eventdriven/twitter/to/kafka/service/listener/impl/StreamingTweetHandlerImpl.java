package com.microservices.eventdriven.twitter.to.kafka.service.listener.impl;

import com.microservices.eventdriven.twitter.to.kafka.service.listener.StreamingTweetHandler;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.StreamingTweetResponse;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingTweetHandlerImpl extends StreamingTweetHandler {

  public StreamingTweetHandlerImpl(TwitterApi apiInstance) {
    super(apiInstance);
  }

  @Override
  public InputStream connectStream() throws ApiException {
    Set<String> tweetFields = new HashSet<>();
    tweetFields.add("author_id");
    tweetFields.add("id");
    tweetFields.add("created_at");
    tweetFields.add("geo");
    Set<String> expansions = new HashSet<>();
    expansions.add("geo.place_id");
    Set<String> placeFields = new HashSet<>();
    placeFields.add("geo");
    placeFields.add("id");
    placeFields.add("name");
    placeFields.add("place_type");

    return this.apiInstance.tweets().searchStream()
        .backfillMinutes(0)
        .tweetFields(tweetFields).expansions(expansions).placeFields(placeFields)
        .execute();
  }

  @Override
  public void actionOnStreamingObject(StreamingTweetResponse streamingTweet) {
    if (streamingTweet == null) {
      log.error("Error: actionOnTweetsStream - streamingTweet is null ");
      return;
    }

    if (streamingTweet.getErrors() != null) {
      streamingTweet.getErrors().forEach(problem -> log.error(problem.toString()));
    } else if (streamingTweet.getData() != null) {
      log.info("New streaming tweet: " + streamingTweet.getData().toJson());
    }
  }
}
