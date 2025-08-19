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
package io.trino.plugin.duckdbhttp;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class DuckDbHttpQueryRunner
{
    private static final Logger log = Logger.get(DuckDbHttpQueryRunner.class);

    private DuckDbHttpQueryRunner() {}

    public static QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.of());
    }

    public static QueryRunner createQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("duckdb_http")
                .setSchema("main")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new DuckDbHttpPlugin());

        // Support both authentication methods:
        // 1. Basic auth via URL: http://user:pass@localhost:9999/
        // 2. API key via separate property: api-key
        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("http-endpoint", extraProperties.getOrDefault("http-endpoint", "http://localhost:9999/"))
                .putAll(extraProperties.entrySet().stream()
                        .filter(entry -> "api-key".equals(entry.getKey()))
                        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)))
                .buildOrThrow();
        
        // If no api-key is provided and URL doesn't contain credentials, add default api-key
        if (!connectorProperties.containsKey("api-key") && !connectorProperties.get("http-endpoint").contains("@")) {
            connectorProperties = ImmutableMap.<String, String>builder()
                    .putAll(connectorProperties)
                    .put("api-key", "test-key")
                    .buildOrThrow();
        }

        queryRunner.createCatalog("duckdb_http", "duckdb-http", connectorProperties);

        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        QueryRunner queryRunner = createQueryRunner();

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        log.info("\n== Authentication Examples ==");
        log.info("Basic Auth: http://username:password@localhost:9999/ (handled automatically by HTTP client)");
        log.info("API Key: http://localhost:9999/ with api-key property (uses X-API-Key header)");
    }
}
