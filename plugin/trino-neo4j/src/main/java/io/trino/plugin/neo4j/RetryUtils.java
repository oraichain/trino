/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.neo4j;

import io.airlift.log.Logger;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Utility class for implementing retry logic for Neo4j operations.
 */
public final class RetryUtils {
    private static final Logger log = Logger.get(RetryUtils.class);

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final Duration DEFAULT_INITIAL_DELAY = Duration.ofMillis(100);

    private RetryUtils() {
        // Utility class
    }

    /**
     * Executes a supplier with retry logic for transient Neo4j exceptions.
     */
    public static <T> T executeWithRetry(Supplier<T> operation) {
        return executeWithRetry(operation, DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_DELAY);
    }

    /**
     * Executes a supplier with configurable retry logic.
     */
    public static <T> T executeWithRetry(Supplier<T> operation, int maxAttempts, Duration initialDelay) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return operation.get();
            } catch (TransientException | ServiceUnavailableException | SessionExpiredException e) {
                lastException = e;

                if (attempt == maxAttempts) {
                    log.error("Operation failed after %d attempts", maxAttempts);
                    throw e;
                }

                Duration delay = Duration.ofMillis(initialDelay.toMillis() * (1L << (attempt - 1))); // Exponential
                                                                                                     // backoff
                log.warn("Attempt %d failed, retrying in %dms: %s", attempt, delay.toMillis(), e.getMessage());

                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry delay", ie);
                }
            }
        }

        throw new RuntimeException("Unexpected end of retry loop", lastException);
    }

    /**
     * Determines if an exception is retryable.
     */
    public static boolean isRetryableException(Throwable throwable) {
        return throwable instanceof TransientException ||
                throwable instanceof ServiceUnavailableException ||
                throwable instanceof SessionExpiredException;
    }
}
