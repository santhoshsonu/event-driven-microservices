package com.microservices.eventdriven.common.config;

import com.microservices.eventdriven.config.RetryConfigData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {

  private final RetryConfigData retryConfigData;

  public RetryConfig(RetryConfigData retryConfigData) {
    this.retryConfigData = retryConfigData;
  }

  @Bean
  public RetryTemplate retryTemplate() {
    RetryTemplate retryTemplate = new RetryTemplate();

    ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
    backOffPolicy.setInitialInterval(retryConfigData.getInitialIntervalMs());
    backOffPolicy.setMaxInterval(retryConfigData.getMaxIntervalMs());
    backOffPolicy.setMultiplier(retryConfigData.getMultiplier());

    retryTemplate.setBackOffPolicy(backOffPolicy);
    retryTemplate.setRetryPolicy(new SimpleRetryPolicy(retryConfigData.getMaxAttempts()));

    return retryTemplate;
  }
}
