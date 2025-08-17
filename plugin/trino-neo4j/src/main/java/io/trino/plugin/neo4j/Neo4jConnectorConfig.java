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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class Neo4jConnectorConfig {
    private URI uri;
    private String authType = "basic";
    private String basicAuthUser = "neo4j";
    private String basicAuthPassword = "";
    private String bearerAuthToken;

    // Connection pool settings
    private int maxConnectionPoolSize = 100;
    private Duration connectionAcquisitionTimeout = Duration.succinctDuration(60, TimeUnit.SECONDS);
    private Duration maxConnectionLifetime = Duration.succinctDuration(1, TimeUnit.HOURS);
    private Duration connectionIdleTimeout = Duration.succinctDuration(30, TimeUnit.MINUTES);
    private boolean connectionLivenessCheckTimeout = true;

    // Cache settings
    private Duration tableCacheExpiration = Duration.succinctDuration(1, TimeUnit.MINUTES);
    private Duration tableNameCacheExpiration = Duration.succinctDuration(1, TimeUnit.MINUTES);
    private int tableCacheMaxSize = 1000;
    private int tableNameCacheMaxSize = 1000;

    @NotNull
    public URI getURI() {
        return uri;
    }

    @Config("neo4j.uri")
    @ConfigDescription("URI for Neo4j instance")
    public Neo4jConnectorConfig setURI(URI uri) {
        this.uri = uri;
        return this;
    }

    @NotNull
    public String getAuthType() {
        return this.authType;
    }

    @Config("neo4j.auth.type")
    @ConfigDescription("Authentication type")
    public Neo4jConnectorConfig setAuthType(String type) {
        this.authType = type;
        return this;
    }

    @NotNull
    public String getBasicAuthUser() {
        return this.basicAuthUser;
    }

    @Config("neo4j.auth.basic.user")
    @ConfigDescription("User for basic auth")
    public Neo4jConnectorConfig setBasicAuthUser(String user) {
        this.basicAuthUser = user;
        return this;
    }

    @NotNull
    public String getBasicAuthPassword() {
        return this.basicAuthPassword;
    }

    @Config("neo4j.auth.basic.password")
    @ConfigDescription("Password for basic auth")
    @ConfigSecuritySensitive
    public Neo4jConnectorConfig setBasicAuthPassword(String password) {
        this.basicAuthPassword = password;
        return this;
    }

    public String getBearerAuthToken() {
        return this.bearerAuthToken;
    }

    @Config("neo4j.auth.bearer.token")
    @ConfigDescription("Bearer auth token")
    @ConfigSecuritySensitive
    public Neo4jConnectorConfig setBearerAuthToken(String token) {
        this.bearerAuthToken = token;
        return this;
    }

    @Min(1)
    public int getMaxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    @Config("neo4j.connection.max-pool-size")
    @ConfigDescription("Maximum size of the connection pool")
    public Neo4jConnectorConfig setMaxConnectionPoolSize(int maxConnectionPoolSize) {
        this.maxConnectionPoolSize = maxConnectionPoolSize;
        return this;
    }

    @NotNull
    public Duration getConnectionAcquisitionTimeout() {
        return connectionAcquisitionTimeout;
    }

    @Config("neo4j.connection.acquisition-timeout")
    @ConfigDescription("Maximum time to wait for a connection from the pool")
    public Neo4jConnectorConfig setConnectionAcquisitionTimeout(Duration connectionAcquisitionTimeout) {
        this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
        return this;
    }

    @NotNull
    public Duration getMaxConnectionLifetime() {
        return maxConnectionLifetime;
    }

    @Config("neo4j.connection.max-lifetime")
    @ConfigDescription("Maximum lifetime of a pooled connection")
    public Neo4jConnectorConfig setMaxConnectionLifetime(Duration maxConnectionLifetime) {
        this.maxConnectionLifetime = maxConnectionLifetime;
        return this;
    }

    @NotNull
    public Duration getConnectionIdleTimeout() {
        return connectionIdleTimeout;
    }

    @Config("neo4j.connection.idle-timeout")
    @ConfigDescription("Maximum time a connection can remain idle in the pool")
    public Neo4jConnectorConfig setConnectionIdleTimeout(Duration connectionIdleTimeout) {
        this.connectionIdleTimeout = connectionIdleTimeout;
        return this;
    }

    public boolean isConnectionLivenessCheckTimeout() {
        return connectionLivenessCheckTimeout;
    }

    @Config("neo4j.connection.liveness-check-timeout")
    @ConfigDescription("Enable connection liveness check timeout")
    public Neo4jConnectorConfig setConnectionLivenessCheckTimeout(boolean connectionLivenessCheckTimeout) {
        this.connectionLivenessCheckTimeout = connectionLivenessCheckTimeout;
        return this;
    }

    @NotNull
    public Duration getTableCacheExpiration() {
        return tableCacheExpiration;
    }

    @Config("neo4j.cache.table-expiration")
    @ConfigDescription("How long to cache table metadata")
    public Neo4jConnectorConfig setTableCacheExpiration(Duration tableCacheExpiration) {
        this.tableCacheExpiration = tableCacheExpiration;
        return this;
    }

    @NotNull
    public Duration getTableNameCacheExpiration() {
        return tableNameCacheExpiration;
    }

    @Config("neo4j.cache.table-name-expiration")
    @ConfigDescription("How long to cache table name metadata")
    public Neo4jConnectorConfig setTableNameCacheExpiration(Duration tableNameCacheExpiration) {
        this.tableNameCacheExpiration = tableNameCacheExpiration;
        return this;
    }

    @Min(1)
    public int getTableCacheMaxSize() {
        return tableCacheMaxSize;
    }

    @Config("neo4j.cache.table-max-size")
    @ConfigDescription("Maximum number of entries in table cache")
    public Neo4jConnectorConfig setTableCacheMaxSize(int tableCacheMaxSize) {
        this.tableCacheMaxSize = tableCacheMaxSize;
        return this;
    }

    @Min(1)
    public int getTableNameCacheMaxSize() {
        return tableNameCacheMaxSize;
    }

    @Config("neo4j.cache.table-name-max-size")
    @ConfigDescription("Maximum number of entries in table name cache")
    public Neo4jConnectorConfig setTableNameCacheMaxSize(int tableNameCacheMaxSize) {
        this.tableNameCacheMaxSize = tableNameCacheMaxSize;
        return this;
    }
}
