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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public final class DuckDbHttpTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final OptionalLong limit;

    @JsonCreator
    public DuckDbHttpTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    public DuckDbHttpTableHandle(SchemaTableName schemaTableName)
    {
        this(schemaTableName, OptionalLong.empty());
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DuckDbHttpTableHandle other = (DuckDbHttpTableHandle) obj;
        return Objects.equals(schemaTableName, other.schemaTableName) &&
                Objects.equals(limit, other.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, limit);
    }

    @Override
    public String toString()
    {
        return "DuckDbHttpTableHandle{" +
                "schemaTableName=" + schemaTableName +
                ", limit=" + limit +
                '}';
    }
}
