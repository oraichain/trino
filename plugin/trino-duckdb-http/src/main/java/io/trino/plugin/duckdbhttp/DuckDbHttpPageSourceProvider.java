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

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class DuckDbHttpPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DuckDbHttpClient client;
    private final DuckDbHttpConfig config;

    @Inject
    public DuckDbHttpPageSourceProvider(DuckDbHttpClient client, DuckDbHttpConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        DuckDbHttpSplit httpSplit = (DuckDbHttpSplit) split;
        DuckDbHttpTableHandle httpTableHandle = (DuckDbHttpTableHandle) tableHandle;

        List<DuckDbHttpColumnHandle> httpColumns = columns.stream()
                .map(DuckDbHttpColumnHandle.class::cast)
                .toList();

        return new DuckDbHttpPageSource(client, config, session, httpSplit, httpTableHandle, httpColumns);
    }
}
