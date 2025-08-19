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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDuckDbHttpClient
{
    @Test
    void testGetSchemaNames()
    {
        String responseBody = "{\"schema_name\":\"main\"}\n{\"schema_name\":\"test\"}";

        DuckDbHttpClient client = createTestClient(responseBody);
        List<String> schemas = client.getSchemaNames(TestingConnectorSession.SESSION);

        assertThat(schemas).containsExactly("main", "test");
    }

    @Test
    void testGetTableNames()
    {
        String responseBody = "{\"table_schema\":\"main\",\"table_name\":\"customers\"}\n" +
                             "{\"table_schema\":\"main\",\"table_name\":\"orders\"}";

        DuckDbHttpClient client = createTestClient(responseBody);
        List<SchemaTableName> tables = client.getTableNames(TestingConnectorSession.SESSION, Optional.of("main"));

        assertThat(tables).containsExactly(
                new SchemaTableName("main", "customers"),
                new SchemaTableName("main", "orders"));
    }

    @Test
    void testGetColumns()
    {
        String responseBody = "{\"column_name\":\"id\",\"data_type\":\"INTEGER\",\"is_nullable\":\"NO\"}\n" +
                             "{\"column_name\":\"name\",\"data_type\":\"VARCHAR\",\"is_nullable\":\"YES\"}\n" +
                             "{\"column_name\":\"amount\",\"data_type\":\"DOUBLE\",\"is_nullable\":\"YES\"}";

        DuckDbHttpClient client = createTestClient(responseBody);
        List<ColumnMetadata> columns = client.getColumns(TestingConnectorSession.SESSION, new SchemaTableName("main", "customers"));

        assertThat(columns).hasSize(3);
        assertThat(columns.get(0).getName()).isEqualTo("id");
        assertThat(columns.get(1).getName()).isEqualTo("name");
        assertThat(columns.get(2).getName()).isEqualTo("amount");
    }

    @Test
    void testExecuteQuery()
    {
        String responseBody = "{\"id\":1,\"name\":\"John Doe\",\"amount\":100.50}\n" +
                             "{\"id\":2,\"name\":\"Jane Smith\",\"amount\":250.75}";

        DuckDbHttpClient client = createTestClient(responseBody);
        List<Map<String, Object>> results = client.executeQuery("SELECT * FROM customers");

        assertThat(results).hasSize(2);
        assertThat(results.get(0).get("id")).isEqualTo(1);
        assertThat(results.get(0).get("name")).isEqualTo("John Doe");
        assertThat(results.get(0).get("amount")).isEqualTo(100.50);
        assertThat(results.get(1).get("id")).isEqualTo(2);
        assertThat(results.get(1).get("name")).isEqualTo("Jane Smith");
        assertThat(results.get(1).get("amount")).isEqualTo(250.75);
    }

    private DuckDbHttpClient createTestClient(String responseBody)
    {
        HttpClient httpClient = new TestingHttpClient(request -> mockResponse(OK, PLAIN_TEXT_UTF_8, responseBody));
        DuckDbHttpConfig config = new DuckDbHttpConfig()
                .setHttpEndpoint(URI.create("http://localhost:9999/"))
                .setApiKey("test-key");

        return new DuckDbHttpClient(httpClient, config);
    }
}
