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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class DuckDbHttpMetadata
        implements ConnectorMetadata
{
    private final DuckDbHttpClient client;

    @Inject
    public DuckDbHttpMetadata(DuckDbHttpClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.getSchemaNames(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return client.getTableNames(session, schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        // Check if table exists by getting its columns
        List<ColumnMetadata> columns = client.getColumns(session, tableName);
        if (columns.isEmpty()) {
            return null;
        }
        return new DuckDbHttpTableHandle(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DuckDbHttpTableHandle tableHandle = (DuckDbHttpTableHandle) table;
        List<ColumnMetadata> columns = client.getColumns(session, tableHandle.getSchemaTableName());
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DuckDbHttpTableHandle table = (DuckDbHttpTableHandle) tableHandle;
        List<ColumnMetadata> columns = client.getColumns(session, table.getSchemaTableName());
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata column = columns.get(i);
            columnHandles.put(column.getName(), new DuckDbHttpColumnHandle(column.getName(), column.getType(), i));
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DuckDbHttpColumnHandle column = (DuckDbHttpColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .build();
    }

    public List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isPresent()) {
            return ImmutableList.of(new SchemaTableName(prefix.getSchema().orElse("main"), prefix.getTable().get()));
        }
        return client.getTableNames(session, prefix.getSchema());
    }
}
