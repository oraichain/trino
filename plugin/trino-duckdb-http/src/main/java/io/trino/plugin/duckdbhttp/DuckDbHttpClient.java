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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class DuckDbHttpClient
{
    private final HttpClient httpClient;
    private final DuckDbHttpConfig config;
    private final ObjectMapper objectMapper;

    @Inject
    public DuckDbHttpClient(@ForDuckDbHttp HttpClient httpClient, DuckDbHttpConfig config)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
        this.objectMapper = new ObjectMapper();
    }

    public List<String> getSchemaNames(ConnectorSession session)
    {
        String sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog')";
        return executeQuery(sql).stream()
                .map(row -> (String) row.get("schema_name"))
                .toList();
    }

    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schemaName)
    {
        String sql;
        if (schemaName.isPresent()) {
            sql = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = '" + schemaName.get() + "'";
        }
        else {
            sql = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')";
        }

        return executeQuery(sql).stream()
                .map(row -> new SchemaTableName((String) row.get("table_schema"), (String) row.get("table_name")))
                .toList();
    }

    public List<ColumnMetadata> getColumns(ConnectorSession session, SchemaTableName tableName)
    {
        String sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '" +
                     tableName.getSchemaName() + "' AND table_name = '" + tableName.getTableName() + "' ORDER BY ordinal_position";

        return executeQuery(sql).stream()
                .map(row -> {
                    String columnName = (String) row.get("column_name");
                    String dataType = (String) row.get("data_type");
                    boolean nullable = "YES".equals(row.get("is_nullable"));
                    Type type = mapDuckDbTypeToTrinoType(dataType);
                    return ColumnMetadata.builder()
                            .setName(columnName)
                            .setType(type)
                            .setNullable(nullable)
                            .build();
                })
                .toList();
    }

    public List<Map<String, Object>> executeQuery(String sql)
    {
        try {
            Request.Builder requestBuilder = preparePost()
                    .setUri(config.getHttpEndpoint())
                    .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8));

            // Add API key authentication if provided
            // Basic auth is handled automatically by HTTP client when credentials are in URL
            config.getApiKey().ifPresent(apiKey ->
                    requestBuilder.setHeader("X-API-Key", apiKey));

            Request request = requestBuilder.build();
            StringResponseHandler.StringResponse response = httpClient.execute(request, createStringResponseHandler());

            if (response.getStatusCode() != 200) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        "HTTP request failed with status: " + response.getStatusCode() + ", body: " + response.getBody());
            }

            return parseJsonResponse(response.getBody());
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to execute HTTP query: " + sql, e);
        }
    }

    private List<Map<String, Object>> parseJsonResponse(String responseBody)
            throws IOException
    {
        // Handle line-separated JSON format
        String[] lines = responseBody.trim().split("\n");
        List<Map<String, Object>> results = new ArrayList<>();

        for (String line : lines) {
            if (!line.trim().isEmpty()) {
                Map<String, Object> row = objectMapper.readValue(line, new TypeReference<Map<String, Object>>() {});
                results.add(row);
            }
        }

        return results;
    }

    private Type mapDuckDbTypeToTrinoType(String duckDbType)
    {
        String upperType = duckDbType.toUpperCase().trim();

        // Handle parameterized types (e.g., VARCHAR(50), DECIMAL(10,2))
        String baseType = upperType.split("\\(")[0].trim();

        return switch (baseType) {
            // Numeric - Small Integers
            case "TINYINT" -> TINYINT;
            case "SMALLINT", "INT2" -> SMALLINT;
            case "UTINYINT", "USMALLINT" -> SMALLINT; // Unsigned small types -> SMALLINT

            // Numeric - Integers
            case "INTEGER", "INT4", "INT", "UINTEGER" -> INTEGER;

            // Numeric - Big Integers
            case "BIGINT", "INT8", "UBIGINT", "HUGEINT", "UHUGEINT" -> BIGINT;

            // Numeric - Decimals
            case "DECIMAL", "NUMERIC" -> createDecimalType(18, 2); // Default precision/scale

            // Numeric - Floating Point
            case "REAL", "FLOAT4" -> REAL;
            case "DOUBLE", "FLOAT8", "FLOAT" -> DOUBLE;

            // Boolean
            case "BOOLEAN" -> BOOLEAN;

            // Character/String Types
            case "CHAR" -> createCharType(1); // Default length 1
            case "VARCHAR", "STRING" -> VARCHAR;
            case "TEXT" -> VARCHAR; // TEXT maps to unlimited VARCHAR

            // Date & Time
            case "DATE" -> DATE;
            case "TIME" -> createTimeType(3); // Default precision
            case "TIMESTAMP", "DATETIME" -> createTimestampType(3); // Default precision
            case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ" -> createTimestampWithTimeZoneType(3);
            case "INTERVAL" -> VARCHAR; // Trino doesn't have direct interval type mapping

            // Binary
            case "BLOB", "BYTEA" -> VARBINARY;

            // JSON
            case "JSON" -> VARCHAR; // JSON stored as VARCHAR

            // Spatial (map to VARCHAR for now)
            case "GEOMETRY", "GEOGRAPHY" -> VARCHAR;

            // Special/Complex Types
            case "UUID" -> createVarcharType(36); // UUID is 36 characters
            case "MAP", "ARRAY", "STRUCT", "UNION" -> VARCHAR; // Complex types -> VARCHAR

            // Default fallback
            default -> VARCHAR;
        };
    }
}
