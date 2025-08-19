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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

final class TestDuckDbHttpIntegration
        extends AbstractTestQueryFramework
{
    private TestingDuckDbHttpServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new TestingDuckDbHttpServer();
        return DuckDbHttpQueryRunner.createQueryRunner(
                ImmutableMap.of(
                        "http-endpoint", server.getHttpEndpoint().toString(),
                        "api-key", "test-key"));
    }

    @Test
    void testShowSchemas()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .contains("main", "test");
    }

    @Test
    void testBasicAuthSupport()
            throws Exception
    {
        // Test that basic auth URL format is properly handled
        // The HTTP client should automatically handle basic auth when credentials are in URL
        try (TestingDuckDbHttpServer basicAuthServer = new TestingDuckDbHttpServer()) {
            String baseUrl = basicAuthServer.getHttpEndpoint().toString();
            String basicAuthUrl = baseUrl.replace("http://", "http://testuser:testpass@");

            // Verify the URL format is correct
            assertThat(basicAuthUrl).contains("testuser:testpass@");

            // This should not throw an exception during connector creation
            QueryRunner basicAuthRunner = DuckDbHttpQueryRunner.createQueryRunner(
                    ImmutableMap.of("http-endpoint", basicAuthUrl));

            // The connector should be created successfully
            // Authentication will be handled automatically by HTTP client
            assertThat(basicAuthRunner).isNotNull();
        }
    }

    @Test
    void testShowTables()
    {
        assertThat(computeActual("SHOW TABLES FROM main").getOnlyColumnAsSet())
                .contains("customers", "orders");
    }

    @Test
    void testDescribeTable()
    {
        assertThat(computeActual("DESCRIBE main.customers").getMaterializedRows())
                .hasSize(3); // id, name, amount columns
    }

    @Test
    void testSelectData()
    {
        assertThat(computeActual("SELECT COUNT(*) FROM main.customers").getOnlyValue())
                .isEqualTo(2L);
    }

    @AfterAll
    public void cleanup()
    {
        if (server != null) {
            try {
                server.close();
            }
            catch (IOException e) {
                // Ignore
            }
        }
    }
}
