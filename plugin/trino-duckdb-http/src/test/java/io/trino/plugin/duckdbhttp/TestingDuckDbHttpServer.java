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

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

public class TestingDuckDbHttpServer
        implements Closeable
{
    private final MockWebServer server;

    public TestingDuckDbHttpServer()
            throws IOException
    {
        this.server = new MockWebServer();

        server.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request)
            {
                String sql = request.getBody().readUtf8();

                if (sql.contains("information_schema.schemata")) {
                    return new MockResponse()
                            .setBody("{\"schema_name\":\"main\"}\n{\"schema_name\":\"test\"}")
                            .setResponseCode(200);
                }
                else if (sql.contains("information_schema.tables")) {
                    return new MockResponse()
                            .setBody("{\"table_schema\":\"main\",\"table_name\":\"customers\"}\n{\"table_schema\":\"main\",\"table_name\":\"orders\"}")
                            .setResponseCode(200);
                }
                else if (sql.contains("information_schema.columns")) {
                    return new MockResponse()
                            .setBody("{\"column_name\":\"id\",\"data_type\":\"INTEGER\",\"is_nullable\":\"NO\"}\n" +
                                   "{\"column_name\":\"name\",\"data_type\":\"VARCHAR\",\"is_nullable\":\"YES\"}\n" +
                                   "{\"column_name\":\"amount\",\"data_type\":\"DOUBLE\",\"is_nullable\":\"YES\"}")
                            .setResponseCode(200);
                }
                else if (sql.contains("SELECT") && sql.contains("FROM")) {
                    // Sample data query
                    return new MockResponse()
                            .setBody("{\"id\":1,\"name\":\"John Doe\",\"amount\":100.50}\n" +
                                   "{\"id\":2,\"name\":\"Jane Smith\",\"amount\":250.75}")
                            .setResponseCode(200);
                }
                else {
                    return new MockResponse()
                            .setBody("[]")
                            .setResponseCode(200);
                }
            }
        });

        server.start();
    }

    public URI getHttpEndpoint()
    {
        return server.url("/").uri();
    }

    @Override
    public void close()
            throws IOException
    {
        server.close();
    }
}
