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
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public final class DuckDbHttpExecuteProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle EXECUTE;

    static {
        try {
            EXECUTE = lookup().unreflect(DuckDbHttpExecuteProcedure.class.getMethod("execute", ConnectorSession.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final DuckDbHttpClient client;

    @Inject
    public DuckDbHttpExecuteProcedure(DuckDbHttpClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "execute",
                ImmutableList.of(new Procedure.Argument("QUERY", VARCHAR)),
                EXECUTE.bindTo(this));
    }

    public void execute(ConnectorSession session, String query)
    {
        try {
            List<Map<String, Object>> results = client.executeQuery(query);
            // For procedures, we typically don't return results, but we execute the query
            // If you want to return results, you'd need to modify this to be a table function instead
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to execute query: " + query, e);
        }
    }
}
