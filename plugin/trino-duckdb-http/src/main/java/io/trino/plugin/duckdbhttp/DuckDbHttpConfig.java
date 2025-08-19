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

import io.airlift.configuration.Config;

import java.net.URI;
import java.util.Optional;

/**
 * Configuration for DuckDB HTTP connector.
 *
 * Supports two authentication methods:
 * 1. Basic Authentication: Include credentials in URL (http://user:pass@host:port/)
 * 2. API Key Authentication: Use separate 'api-key' property with X-API-Key header
 */
public class DuckDbHttpConfig
{
    private URI httpEndpoint = URI.create("http://localhost:9999/");
    private Optional<String> apiKey = Optional.empty();
    private int connectionTimeoutMillis = 30000;
    private int requestTimeoutMillis = 60000;
    private boolean includeSchemaInTableName = false;
    private boolean useSelectStar = true;
    private boolean limitPushdownEnabled = true;

    public URI getHttpEndpoint()
    {
        return httpEndpoint;
    }

    @Config("http-endpoint")
    public DuckDbHttpConfig setHttpEndpoint(URI httpEndpoint)
    {
        // Keep the original URI with credentials intact
        // Let the HTTP client handle basic authentication automatically
        this.httpEndpoint = httpEndpoint;
        return this;
    }

    public Optional<String> getApiKey()
    {
        return apiKey;
    }

    @Config("api-key")
    public DuckDbHttpConfig setApiKey(String apiKey)
    {
        this.apiKey = Optional.ofNullable(apiKey);
        return this;
    }

    public int getConnectionTimeoutMillis()
    {
        return connectionTimeoutMillis;
    }

    @Config("connection-timeout")
    public DuckDbHttpConfig setConnectionTimeoutMillis(int connectionTimeoutMillis)
    {
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        return this;
    }

    public int getRequestTimeoutMillis()
    {
        return requestTimeoutMillis;
    }

    @Config("request-timeout")
    public DuckDbHttpConfig setRequestTimeoutMillis(int requestTimeoutMillis)
    {
        this.requestTimeoutMillis = requestTimeoutMillis;
        return this;
    }

    public boolean hasBasicAuth()
    {
        return httpEndpoint.getUserInfo() != null;
    }

    public boolean isIncludeSchemaInTableName()
    {
        return includeSchemaInTableName;
    }

    @Config("include-schema-in-table-name")
    public DuckDbHttpConfig setIncludeSchemaInTableName(boolean includeSchemaInTableName)
    {
        this.includeSchemaInTableName = includeSchemaInTableName;
        return this;
    }

    public boolean isUseSelectStar()
    {
        return useSelectStar;
    }

    @Config("use-select-star")
    public DuckDbHttpConfig setUseSelectStar(boolean useSelectStar)
    {
        this.useSelectStar = useSelectStar;
        return this;
    }

    public boolean isLimitPushdownEnabled()
    {
        return limitPushdownEnabled;
    }

    @Config("limit-pushdown-enabled")
    public DuckDbHttpConfig setLimitPushdownEnabled(boolean limitPushdownEnabled)
    {
        this.limitPushdownEnabled = limitPushdownEnabled;
        return this;
    }
}
