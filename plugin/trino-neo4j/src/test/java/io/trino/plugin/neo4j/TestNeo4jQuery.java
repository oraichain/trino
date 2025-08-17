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

import io.trino.plugin.neo4j.support.BaseNeo4jTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestNeo4jQuery
                extends BaseNeo4jTest {
        public record TestCase(String cypherValue, String schemaType, String sqlValue, Consumer<TestCase> handler) {
        }

        public TestCase ok(String cypherValue, String schemaType, String sqlValue) {
                return new TestCase(cypherValue, schemaType, sqlValue, this::assertSuccessful);
        }

        public TestCase fail(String cypherValue, String schemaType) {
                return new TestCase(cypherValue, schemaType, null, this::assertFails);
        }

        public Stream<TestCase> booleanTests() {
                return Stream.of(
                                ok("true", "boolean", "true"),
                                ok("false", "boolean", "false"),

                                // json
                                ok("true", "json", "json 'true'"),
                                ok("false", "json", "json 'false'"));
        }

        public Stream<TestCase> integerTests() {
                return Stream.of(
                                ok("toInteger(null)", "integer", "CAST(NULL AS INTEGER)"),
                                ok("-2147483648", "integer", "-2147483648"),
                                ok("1234567890", "integer", "1234567890"),
                                ok("2147483647", "integer", "2147483647"),

                                ok("toInteger(null)", "bigint", "CAST(NULL AS BIGINT)"),
                                ok("-9223372036854775808", "bigint", "-9223372036854775808"),
                                ok("123456789012", "bigint", "123456789012"),
                                ok("9223372036854775807", "bigint", "9223372036854775807"),

                                // json
                                ok("-9223372036854775808", "json", "json '-9223372036854775808'"),
                                ok("123456789012", "json", "json '123456789012'"),
                                ok("9223372036854775807", "json", "json '9223372036854775807'"));
        }

        public Stream<TestCase> floatTests() {
                return Stream.of(
                                ok("toFloat(null)", "double", "CAST(NULL AS DOUBLE)"),
                                ok("3.1415926835", "double", "CAST(3.1415926835 AS DOUBLE)"),
                                ok("1.79769E308", "double", "CAST(1.79769E308 AS DOUBLE)"),
                                ok("2.225E-307", "double", "CAST(2.225E-307 AS DOUBLE)"),

                                // json
                                ok("3.1415926835", "json", "json '3.1415926835'"),
                                ok("1.79769E308", "json", "json '1.79769E308'"),
                                ok("2.225E-307", "json", "json '2.225E-307'"));
        }

        public Stream<TestCase> stringTests() {
                return Stream.of(
                                ok("null", "varchar", "CAST(NULL AS varchar)"),
                                ok("'text_a'", "varchar", "CAST('text_a' AS varchar)"),
                                ok("'攻殻機動隊'", "varchar", "CAST('攻殻機動隊' AS varchar)"),
                                ok("'😂'", "varchar", "CAST('😂' AS varchar)"),
                                ok("'Ну, погоди!'", "varchar", "CAST('Ну, погоди!' AS varchar)"));
        }

        public Stream<TestCase> dateTests() {
                return Stream.of(
                                ok("date(null)", "date", "CAST(NULL AS DATE)"),
                                ok("date('0001-01-01')", "date", "DATE '0001-01-01'"),
                                ok("date('1582-10-04')", "date", "DATE '1582-10-04'"), // before julian->gregorian
                                                                                       // switch
                                ok("date('1582-10-05')", "date", "DATE '1582-10-05'"), // begin julian->gregorian switch
                                ok("date('1582-10-14')", "date", "DATE '1582-10-14'"), // end julian->gregorian switch
                                ok("date('1952-04-03')", "date", "DATE '1952-04-03'"), // before epoch
                                ok("date('1970-01-01')", "date", "DATE '1970-01-01'"),
                                ok("date('1970-02-03')", "date", "DATE '1970-02-03'"),
                                ok("date('2017-07-01')", "date", "DATE '2017-07-01'"), // summer on northern hemisphere
                                                                                       // (possible DST)
                                ok("date('2017-01-01')", "date", "DATE '2017-01-01'"), // winter on northern hemisphere
                                                                                       // (possible DST on
                                                                                       // southern hemisphere)
                                ok("date('1983-04-01')", "date", "DATE '1983-04-01'"),
                                ok("date('1983-10-01')", "date", "DATE '1983-10-01'"));
        }

        public Stream<TestCase> localTimeTests() {
                return Stream.of(
                                ok("localtime(null)", "time(9)", "CAST(NULL AS TIME(9))"),
                                ok("localtime('09:12:34')", "time(9)", "TIME '09:12:34.000000000'"),
                                ok("localtime('10:12:34.000000000')", "time(9)", "TIME '10:12:34.000000000'"),
                                ok("localtime('15:12:34.567000000')", "time(9)", "TIME '15:12:34.567000000'"),
                                ok("localtime('23:59:59.000000000')", "time(9)", "TIME '23:59:59.000000000'"),
                                ok("localtime('23:59:59.999000000')", "time(9)", "TIME '23:59:59.999000000'"),
                                ok("localtime('23:59:59.999900000')", "time(9)", "TIME '23:59:59.999900000'"),
                                ok("localtime('23:59:59.999990000')", "time(9)", "TIME '23:59:59.999990000'"),
                                ok("localtime('23:59:59.999999999')", "time(9)", "TIME '23:59:59.999999999'"),

                                ok("localtime('02:03:04')", "time(0)", "time '02:03:04'"),
                                ok("localtime('02:03:04.123')", "time(3)", "time '02:03:04.123'"),
                                ok("localtime('02:03:04.123456')", "time(6)", "time '02:03:04.123456'"),
                                ok("localtime('02:03:04.123456789')", "time(9)", "time '02:03:04.123456789'"),

                                ok("localtime('02:03:04.123456789')", "time(0)", "time '02:03:04'"),
                                ok("localtime('02:03:04.123456789')", "time(3)", "time '02:03:04.123'"),
                                ok("localtime('02:03:04.123456789')", "time(6)", "time '02:03:04.123457'"),
                                ok("localtime('02:03:04.123456789')", "time(9)", "time '02:03:04.123456789'"));
        }

        public Stream<TestCase> timeTests() {
                // H2 doesn't support TIME WITH TIME ZONE - skip these tests
                return Stream.of();
        }

        public Stream<TestCase> localDateTimeTests() {
                return Stream.of(
                                ok("localdatetime('2017-07-01T01:02:03')", "timestamp(0)",
                                                "timestamp '2017-07-01 01:02:03'"),
                                ok("localdatetime('2017-07-01T01:02:03.456')", "timestamp(3)",
                                                "timestamp '2017-07-01 01:02:03.456'"));
        }

        public Stream<TestCase> dateTimeTests() {
                // H2 doesn't support TIMESTAMP WITH TIME ZONE - skip these tests
                return Stream.of();
        }

        public Stream<TestCase> pointTests() {
                // ok("point({x: 1.0, y: 2.0})", "json", "json '{\"srid\": 7203, \"x\": 1.0,
                // \"y\": 2.0}'")
                return Stream.of();
        }

        public Stream<TestCase> listTests() {
                return Stream.of(
                                // homogenous list to array - H2 compatible syntax
                                ok("[]", "array(unknown)", "array[]"),
                                ok("[1]", "array(integer)", "array[1]"),
                                ok("[1, 2, 3]", "array(integer)", "array[1, 2, 3]"),
                                // Skip complex array types that H2 doesn't support
                                fail("[true, 42]", "array(boolean)"),

                                // to json
                                ok("[true, 42, 'hello']", "json", "json '[true, 42, \"hello\"]'"));
        }

        public Stream<TestCase> mapTests() {
                // H2 doesn't support MAP functions - skip these tests
                return Stream.of();
        }

        @ParameterizedTest
        @MethodSource({
                        "booleanTests",
                        "integerTests",
                        "floatTests",
                        "stringTests",
                        // "durationTests"
                        "dateTests",
                        "localTimeTests",
                        "localDateTimeTests",
                        "timeTests",
                        "dateTimeTests",
                        "pointTests",
                        "listTests",
                        "mapTests"
        })
        public void testTypeMappings(TestCase testCase) {
                testCase.handler.accept(testCase);

                /*
                 * String query = """
                 * select * from table(
                 * system.query(
                 * query => 'return %s as answer',
                 * schema => descriptor(answer %s)
                 * )
                 * )
                 * """.formatted(testCase.cypherValue.replaceAll("'", "''"),
                 * testCase.schemaType);
                 * 
                 * QueryAssertions.QueryAssert assertion = assertThat(this.query(query));
                 * 
                 * //QueryAssertions.QueryAssert assertion =
                 * assertThat(queryAssertions.query(session, "SELECT * FROM " +
                 * temporaryRelation.getName()));
                 * MaterializedResult expected =
                 * this.getQueryRunner().execute("VALUES ROW(%s)".formatted(testCase.sqlValue));
                 * 
                 * assertion.matches(expected);
                 */

                // Verify types if specified
                /*
                 * for (int column = 0; column < testCases.size(); column++) {
                 * SqlDataTypeTest.TestCase testCase = testCases.get(column);
                 * if (testCase.getExpectedType().isPresent()) {
                 * Type expectedType = testCase.getExpectedType().get();
                 * assertion.outputHasType(column, expectedType);
                 * assertThat(expected.getTypes())
                 * .as(
                 * format("Expected literal type at column %d (check consistency of expected type and expected literal)"
                 * , column + 1))
                 * .element(column).isEqualTo(expectedType);
                 * }
                 * }
                 */
        }

        public void assertSuccessful(TestCase testCase) {
                String cypherQuery = """
                                select * from table(
                                  system.query(
                                    query => 'return %s as answer',
                                    schema => descriptor(answer %s)
                                  )
                                )
                                """.formatted(testCase.cypherValue.replaceAll("'", "''"), testCase.schemaType);

                assertQuery(cypherQuery, "VALUES ROW(%s)".formatted(testCase.sqlValue));
        }

        public void assertFails(TestCase testCase) {
                String cypherQuery = """
                                select * from table(
                                  system.query(
                                    query => 'return %s as answer',
                                    schema => descriptor(answer %s)
                                  )
                                )
                                """.formatted(testCase.cypherValue.replaceAll("'", "''"), testCase.schemaType);

                assertThatThrownBy(() -> getQueryRunner().execute(cypherQuery))
                                .satisfiesAnyOf(
                                                throwable -> assertThat(throwable)
                                                                .hasMessageContaining("Type mismatch"),
                                                throwable -> assertThat(throwable)
                                                                .hasMessageContaining("Cannot convert Neo4j value"),
                                                throwable -> assertThat(throwable)
                                                                .hasMessageContaining("type conversion"),
                                                throwable -> assertThat(throwable)
                                                                .hasMessageContaining("Incompatible type"));
        }
}
