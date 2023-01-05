package com.microservices.eventdriven.twitter.to.kafka.service;

import com.microservices.eventdriven.twitter.to.kafka.service.init.StreamInitializer;
import com.microservices.eventdriven.twitter.to.kafka.service.runner.IStreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.eventdriven")
@Slf4j
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

  private final IStreamRunner twitterKafkaStreamRunner;
  private final StreamInitializer streamInitializer;

  public TwitterToKafkaServiceApplication(IStreamRunner twitterKafkaStreamRunner,
      StreamInitializer streamInitializer) {
    this.twitterKafkaStreamRunner = twitterKafkaStreamRunner;
    this.streamInitializer = streamInitializer;
  }

  public static void main(String[] args) {
    SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
  }

  @Override
  public void run(String... args) {
    log.info("App starts...");
    streamInitializer.init();
    twitterKafkaStreamRunner.start();
  }
}
//  docker run -it --network docker-compose_application confluentinc/cp-kafkacat kafkacat -b localhost:29092  -L