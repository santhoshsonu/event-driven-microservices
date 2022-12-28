package com.microservices.eventdriven.twitter.to.kafka.service;

import com.microservices.eventdriven.twitter.to.kafka.service.runner.ITwitterKafkaStreamRunner;
import com.microservices.eventdriven.twitter.to.kafka.service.runner.impl.TwitterKafkaStreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

  private final ITwitterKafkaStreamRunner twitterKafkaStreamRunner;

  public TwitterToKafkaServiceApplication(TwitterKafkaStreamRunner twitterKafkaStreamRunner) {
    this.twitterKafkaStreamRunner = twitterKafkaStreamRunner;
  }

  public static void main(String[] args) {
    SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
  }

  @Override
  public void run(String... args) {
    log.info("App starts...");
    twitterKafkaStreamRunner.start();
  }
}