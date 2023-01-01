package com.microservices.eventdriven.twitter.to.kafka.service.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.ZonedDateTime;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.ToString.Exclude;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet {

  private static final ObjectMapper mapper = new ObjectMapper().registerModule(
      new JavaTimeModule());

  @JsonProperty("created_at")
  private ZonedDateTime createdAt;
  private String id;
  private String text;
  @Getter(AccessLevel.NONE)
  private User user;

  @Exclude
  @JsonProperty(value = "author_id")
  private String authorId;

  public static Tweet fromJson(String jsonData) throws JsonProcessingException {
    return mapper.readValue(jsonData, Tweet.class);
  }

  public User getUser() {
    return new User().setId(authorId);
  }
}

@Data
@Accessors(chain = true)
class User {

  private String id;
}
