/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.hamcrest.Matchers.containsString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.ResponseException;

import com.google.gson.JsonElement;

import lombok.SneakyThrows;

public class LogPatternToolIT extends BaseAgentToolsIT {

    public static String requestBodyResourceFile = "org/opensearch/agent/tools/register_flow_agent_of_log_pattern_tool_request_body.json";
    public static String responseBodyResourceFile = "org/opensearch/agent/tools/expected_flow_agent_of_log_pattern_tool_response_body.json";
    public String registerAgentRequestBody;
    public static String TEST_PATTERN_INDEX_NAME = "test_pattern_index";

    public LogPatternToolIT() {}

    @SneakyThrows
    private void prepareIndex() {
        // prepare index for neural sparse query type
        createIndexWithConfiguration(
            TEST_PATTERN_INDEX_NAME,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"field1\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"field2\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"field3\": {\n"
                + "        \"type\": \"integer\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(TEST_PATTERN_INDEX_NAME, "0", List.of("field1", "field2", "field3"), List.of("123", "123.abc-AB * De /", 12345));
        addDocToIndex(TEST_PATTERN_INDEX_NAME, "1", List.of("field1", "field2", "field3"), List.of("123", "45.abc-AB * De /", 12345));
        addDocToIndex(TEST_PATTERN_INDEX_NAME, "2", List.of("field1", "field2", "field3"), List.of("123", "12.abc_AB * De /", 12345));
        addDocToIndex(TEST_PATTERN_INDEX_NAME, "3", List.of("field1", "field2", "field3"), List.of("123", "45.ab_AB * De /", 12345));
        addDocToIndex(TEST_PATTERN_INDEX_NAME, "4", List.of("field1", "field2", "field3"), List.of("123", ".abAB * De /", 12345));
    }

    private String agentId;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        prepareIndex();
        registerAgentRequestBody = Files.readString(Path.of(this.getClass().getClassLoader().getResource(requestBodyResourceFile).toURI()));
        agentId = createAgent(registerAgentRequestBody);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
    }

    @SneakyThrows
    public void testLogPatternToolDefault() {
        JsonElement expected = gson
            .fromJson(
                Files.readString(Path.of(this.getClass().getClassLoader().getResource(responseBodyResourceFile).toURI())),
                JsonElement.class
            );
        JsonElement result = gson
            .fromJson(
                executeAgent(
                    agentId,
                    String.format(Locale.ROOT, "{\"parameters\": {\"index\": \"%s\", \"input\": \"%s\"}}", TEST_PATTERN_INDEX_NAME, "{}")
                ),
                JsonElement.class
            );
        assertEquals(expected, result);
    }

    @SneakyThrows
    public void testLogPatternToolWithSpecifiedPatternField() {
        JsonElement expected = gson
            .fromJson("[{\"total count\":5,\"sample logs\":[\"123\", \"123\"],\"pattern\":\"<*>\"}]", JsonElement.class);
        JsonElement result = gson
            .fromJson(
                executeAgent(
                    agentId,
                    String
                        .format(
                            "{\"parameters\": {\"index\": \"%s\", \"pattern_field\": \"field1\", \"sample_log_size\": 2, \"input\": \"%s\"}}",
                            TEST_PATTERN_INDEX_NAME,
                            "{}"
                        )
                ),
                JsonElement.class
            );
        assertEquals(expected, result);
    }

    public void testLogPatternToolWithNonStringPatternField() {
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(
                agentId,
                String
                    .format(
                        "{\"parameters\": {\"index\": \"%s\", \"pattern_field\": \"field3\", \"sample_log_size\": 2, \"input\": \"%s\"}}",
                        TEST_PATTERN_INDEX_NAME,
                        "{}"
                    )
            )
        );
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                containsString("Invalid parameter pattern_field: pattern field field3 in index test_pattern_index is not type of String")
            );
    }

    public void testLogPatternToolWithNonExistField() {
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(
                agentId,
                String
                    .format(
                        "{\"parameters\": {\"index\": \"%s\", \"pattern_field\": \"field4\", \"sample_log_size\": 2, \"input\": \"%s\"}}",
                        TEST_PATTERN_INDEX_NAME,
                        "{}"
                    )
            )
        );
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                containsString("Invalid parameter pattern_field: index test_pattern_index does not have a field named field4")
            );
    }

    public void testLogPatternToolWithNonIntegerSampleLogSize() {
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(
                agentId,
                String
                    .format(
                        "{\"parameters\": {\"index\": \"%s\", \"sample_log_size\": 1.5, \"input\": \"%s\"}}",
                        TEST_PATTERN_INDEX_NAME,
                        "{}"
                    )
            )
        );
        MatcherAssert
            .assertThat(exception.getMessage(), containsString("\"Invalid value 1.5 for parameter sample_log_size, it should be a number"));
    }

    public void testLogPatternToolWithNonPositiveSampleLogSize() {
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(
                agentId,
                String
                    .format(
                        "{\"parameters\": {\"index\": \"%s\", \"sample_log_size\": -1, \"input\": \"%s\"}}",
                        TEST_PATTERN_INDEX_NAME,
                        "{}"
                    )
            )
        );
        MatcherAssert
            .assertThat(exception.getMessage(), containsString("\"Invalid value -1 for parameter sample_log_size, it should be positive"));
    }

    @SneakyThrows
    public void testLogPatternToolWithPPLInput() {
        JsonElement expected = gson
            .fromJson(
                Files.readString(Path.of(this.getClass().getClassLoader().getResource(responseBodyResourceFile).toURI())),
                JsonElement.class
            );
        JsonElement result = gson
            .fromJson(
                executeAgent(
                    agentId,
                    String
                        .format(
                            "{\"parameters\": {\"index\": \"%s\", \"ppl\": \"%s\"}}",
                            TEST_PATTERN_INDEX_NAME,
                            String.format(Locale.ROOT, "source=%s", TEST_PATTERN_INDEX_NAME)
                        )
                ),
                JsonElement.class
            );
        assertEquals(expected, result);
    }

}
