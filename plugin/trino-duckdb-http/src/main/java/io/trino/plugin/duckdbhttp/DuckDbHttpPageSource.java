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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class DuckDbHttpPageSource
        implements ConnectorPageSource
{
    private final DuckDbHttpClient client;
    private final DuckDbHttpConfig config;
    private final ConnectorSession session;
    private final DuckDbHttpSplit split;
    private final DuckDbHttpTableHandle tableHandle;
    private final List<DuckDbHttpColumnHandle> columns;
    private final List<Type> types;

    private Iterator<Map<String, Object>> dataIterator;
    private boolean finished;

    public DuckDbHttpPageSource(
            DuckDbHttpClient client,
            DuckDbHttpConfig config,
            ConnectorSession session,
            DuckDbHttpSplit split,
            DuckDbHttpTableHandle tableHandle,
            List<DuckDbHttpColumnHandle> columns)
    {
        this.client = requireNonNull(client, "client is null");
        this.config = requireNonNull(config, "config is null");
        this.session = requireNonNull(session, "session is null");
        this.split = requireNonNull(split, "split is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.types = columns.stream().map(DuckDbHttpColumnHandle::getType).toList();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }

        if (dataIterator == null) {
            // Build SELECT query
            StringBuilder sqlBuilder = new StringBuilder("SELECT ");
            
            if (config.isUseSelectStar()) {
                sqlBuilder.append("*");
            } else {
                for (int i = 0; i < columns.size(); i++) {
                    if (i > 0) {
                        sqlBuilder.append(", ");
                    }
                    sqlBuilder.append(columns.get(i).getName());
                }
            }
            sqlBuilder.append(" FROM ");
            
            String schemaName = tableHandle.getSchemaTableName().getSchemaName();
            String tableName = tableHandle.getSchemaTableName().getTableName();
            
            // Include schema prefix based on configuration
            if (config.isIncludeSchemaInTableName() && schemaName != null && !schemaName.isEmpty()) {
                sqlBuilder.append(schemaName);
                sqlBuilder.append(".");
            }
            sqlBuilder.append(tableName);

            // Add LIMIT clause if present
            if (tableHandle.getLimit().isPresent()) {
                sqlBuilder.append(" LIMIT ");
                sqlBuilder.append(tableHandle.getLimit().getAsLong());
            }

            List<Map<String, Object>> results = client.executeQuery(sqlBuilder.toString());
            dataIterator = results.iterator();
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        int maxRowsPerPage = 1000; // Process up to 1000 rows per page
        int rowCount = 0;

        while (dataIterator.hasNext() && rowCount < maxRowsPerPage) {
            Map<String, Object> row = dataIterator.next();
            pageBuilder.declarePosition();

            for (int i = 0; i < columns.size(); i++) {
                DuckDbHttpColumnHandle column = columns.get(i);
                Type type = column.getType();
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                Object value = row.get(column.getName());

                writeValue(type, blockBuilder, value);
            }
            rowCount++;
        }

        if (!dataIterator.hasNext()) {
            finished = true;
        }

        return SourcePage.create(pageBuilder.build());
    }

    private void writeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        String typeName = type.getDisplayName().toLowerCase();

        // Handle numeric types
        if (typeName.equals("tinyint") || typeName.equals("smallint") || typeName.equals("integer")) {
            if (value instanceof Number number) {
                type.writeLong(blockBuilder, number.longValue());
            }
            else {
                blockBuilder.appendNull();
            }
            return;
        }

        if (typeName.equals("bigint")) {
            if (value instanceof Number number) {
                type.writeLong(blockBuilder, number.longValue());
            }
            else {
                blockBuilder.appendNull();
            }
            return;
        }

        if (typeName.equals("real") || typeName.equals("double")) {
            if (value instanceof Number number) {
                if (typeName.equals("real")) {
                    type.writeLong(blockBuilder, Float.floatToIntBits(number.floatValue()));
                }
                else {
                    type.writeDouble(blockBuilder, number.doubleValue());
                }
            }
            else {
                blockBuilder.appendNull();
            }
            return;
        }

        if (typeName.startsWith("decimal")) {
            if (value instanceof Number number) {
                // For decimal, we need to handle it as a string representation
                Slice slice = Slices.utf8Slice(value.toString());
                type.writeSlice(blockBuilder, slice);
            }
            else {
                blockBuilder.appendNull();
            }
            return;
        }

        // Handle boolean
        if (typeName.equals("boolean")) {
            if (value instanceof Boolean boolValue) {
                type.writeBoolean(blockBuilder, boolValue);
            }
            else {
                blockBuilder.appendNull();
            }
            return;
        }

        // Handle date/time types
        if (typeName.equals("date")) {
            // Convert date string to epoch days
            try {
                if (value instanceof String dateStr) {
                    // Assuming ISO date format (YYYY-MM-DD)
                    long epochDays = java.time.LocalDate.parse(dateStr).toEpochDay();
                    type.writeLong(blockBuilder, epochDays);
                }
                else {
                    blockBuilder.appendNull();
                }
            }
            catch (Exception e) {
                blockBuilder.appendNull();
            }
            return;
        }

        if (typeName.startsWith("time")) {
            // Handle time types - convert to appropriate representation
            if (value instanceof String timeStr) {
                try {
                    if (typeName.contains("timestamp")) {
                        // Parse timestamp and convert to microseconds since epoch
                        java.time.Instant instant = java.time.Instant.parse(timeStr + "Z");
                        long microseconds = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;
                        type.writeLong(blockBuilder, microseconds);
                    }
                    else {
                        // Time only - convert to nanoseconds since midnight
                        java.time.LocalTime time = java.time.LocalTime.parse(timeStr);
                        long nanoseconds = time.toNanoOfDay();
                        type.writeLong(blockBuilder, nanoseconds);
                    }
                }
                catch (Exception e) {
                    blockBuilder.appendNull();
                }
            }
            else {
                blockBuilder.appendNull();
            }
            return;
        }

        // Handle binary types
        if (typeName.equals("varbinary")) {
            if (value instanceof byte[] bytes) {
                Slice slice = Slices.wrappedBuffer(bytes);
                type.writeSlice(blockBuilder, slice);
            }
            else if (value instanceof String str) {
                // Assume hex string representation or base64
                try {
                    // Try to decode as hex first
                    if (str.startsWith("\\x")) {
                        str = str.substring(2); // Remove \x prefix
                    }
                    byte[] bytes = hexStringToByteArray(str);
                    Slice slice = Slices.wrappedBuffer(bytes);
                    type.writeSlice(blockBuilder, slice);
                }
                catch (Exception e) {
                    // Fallback: treat as UTF-8 string
                    Slice slice = Slices.utf8Slice(str);
                    type.writeSlice(blockBuilder, slice);
                }
            }
            else {
                blockBuilder.appendNull();
            }
            return;
        }

        // Handle JSON type
        if (typeName.equals("json")) {
            Slice slice = Slices.utf8Slice(value.toString());
            type.writeSlice(blockBuilder, slice);
            return;
        }

        // Default: handle as string/varchar (including char, varchar, text, uuid, etc.)
        Slice slice = Slices.utf8Slice(value.toString());
        type.writeSlice(blockBuilder, slice);
    }

    private static byte[] hexStringToByteArray(String hexString)
    {
        int len = hexString.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have even length");
        }
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                                  + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        // No resources to close
    }
}
