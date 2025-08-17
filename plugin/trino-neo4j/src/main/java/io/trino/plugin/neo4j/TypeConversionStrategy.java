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

import io.airlift.slice.Slice;
import io.trino.spi.type.Type;
import org.neo4j.driver.Value;

/**
 * Strategy interface for optimized type conversions from Neo4j values to Trino
 * types.
 * This allows for pre-computed conversion strategies based on type
 * combinations.
 */
public interface TypeConversionStrategy {
    boolean canConvert(org.neo4j.driver.types.Type neo4jType, Type trinoType);

    boolean toBoolean(Value value, Type type);

    long toLong(Value value, Type type);

    double toDouble(Value value, Type type);

    Slice toSlice(Value value, Type type);

    Object toObject(Value value, Type type);
}
