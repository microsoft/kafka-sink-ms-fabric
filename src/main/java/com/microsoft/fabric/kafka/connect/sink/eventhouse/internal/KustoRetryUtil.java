package com.microsoft.fabric.kafka.connect.sink.eventhouse.internal;

import com.microsoft.azure.kusto.data.exceptions.KustoDataExceptionBase;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.jetbrains.annotations.NotNull;

import java.util.function.Predicate;

public class KustoRetryUtil {
  private KustoRetryUtil() {

  }

  @NotNull
  public static Retry getRetries(@NotNull KustoRetryConfig kustoRetryConfig) {
    final Retry retry;
    IntervalFunction backOffFunction = IntervalFunction.ofExponentialRandomBackoff(
        kustoRetryConfig.getBaseIntervalMillis(), IntervalFunction.DEFAULT_MULTIPLIER,
        IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR, kustoRetryConfig.getMaxIntervalMillis());
    Predicate<Throwable> isTransientException = e -> {
      if ((e instanceof KustoDataExceptionBase)) {
        return !((KustoDataExceptionBase) e).isPermanent();
      }
      return e instanceof IngestionServiceException;
    };
    RetryConfig retryConfig = RetryConfig.custom().maxAttempts(kustoRetryConfig.getMaxAttempts())
        .intervalFunction(backOffFunction).retryOnException(isTransientException).build();
    RetryRegistry registry = RetryRegistry.of(retryConfig);
    retry = registry.retry("tempStoreService", retryConfig);
    return retry;
  }
}
