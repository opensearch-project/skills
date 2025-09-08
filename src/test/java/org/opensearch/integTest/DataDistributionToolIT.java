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
import org.junit.Test;

import lombok.SneakyThrows;

public class DataDistributionToolIT extends BaseAgentToolsIT {

    public static String requestBodyResourceFile =
        "org/opensearch/agent/tools/register_flow_agent_of_data_distribution_tool_request_body.json";
    public String registerAgentRequestBody;
    public static String TEST_DATA_INDEX_NAME = "test_data_distribution_index";

    private String agentId;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        prepareDataIndex();
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
    private void prepareDataIndex() {
        createIndexWithConfiguration(
            TEST_DATA_INDEX_NAME,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"@timestamp\": {\n"
                + "        \"type\": \"date\",\n"
                + "        \"format\": \"yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis\"\n"
                + "      },\n"
                + "      \"status\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      },\n"
                + "      \"level\": {\n"
                + "        \"type\": \"integer\"\n"
                + "      },\n"
                + "      \"host\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      },\n"
                + "      \"response_time\": {\n"
                + "        \"type\": \"float\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );

        // Add baseline data (09:00:00 to 10:00:00)
        addDocToIndex(
            TEST_DATA_INDEX_NAME,
            "base1",
            List.of("@timestamp", "status", "level", "host", "response_time"),
            List.of("2025-01-01 09:30:00", "success", 1, "server-01", 120.5)
        );
        addDocToIndex(
            TEST_DATA_INDEX_NAME,
            "base2",
            List.of("@timestamp", "status", "level", "host", "response_time"),
            List.of("2025-01-01 09:45:00", "success", 1, "server-02", 95.2)
        );
        addDocToIndex(
            TEST_DATA_INDEX_NAME,
            "base3",
            List.of("@timestamp", "status", "level", "host", "response_time"),
            List.of("2025-01-01 09:50:00", "info", 2, "server-01", 110.8)
        );

        // Add selection data (10:00:00 to 11:00:00)
        addDocToIndex(
            TEST_DATA_INDEX_NAME,
            "sel1",
            List.of("@timestamp", "status", "level", "host", "response_time"),
            List.of("2025-01-01 10:15:00", "error", 3, "server-01", 250.3)
        );
        addDocToIndex(
            TEST_DATA_INDEX_NAME,
            "sel2",
            List.of("@timestamp", "status", "level", "host", "response_time"),
            List.of("2025-01-01 10:30:00", "error", 4, "server-02", 180.7)
        );
        addDocToIndex(
            TEST_DATA_INDEX_NAME,
            "sel3",
            List.of("@timestamp", "status", "level", "host", "response_time"),
            List.of("2025-01-01 10:45:00", "warning", 2, "server-03", 140.1)
        );
        addDocToIndex(
            TEST_DATA_INDEX_NAME,
            "sel4",
            List.of("@timestamp", "status", "level", "host", "response_time"),
            List.of("2025-01-01 10:50:00", "error", 3, "server-01", 300.5)
        );
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolSingleAnalysis() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("singleAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolComparisonAnalysis() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"baselineTimeRangeStart\": \"2025-01-01 09:00:00\", \"baselineTimeRangeEnd\": \"2025-01-01 10:00:00\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("comparisonAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolWithFilter() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"filter\": \"[\\\"{'term': {'status': 'error'}}\\\"]\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("singleAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolMissingRequiredParameters() {
        Exception exception = assertThrows(Exception.class, () -> executeAgent(agentId, "{\"parameters\": {\"index\": \"test_index\"}}"));
        MatcherAssert.assertThat(exception.getMessage(), containsString("Missing required parameters"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolInvalidIndex() {
        Exception exception = assertThrows(
            Exception.class,
            () -> executeAgent(
                agentId,
                "{\"parameters\": {\"index\": \"non_existent_index\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\"}}"
            )
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("no such index"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolPPLSingleAnalysis() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"queryType\": \"ppl\", \"ppl\": \"source=%s\"}}",
                    TEST_DATA_INDEX_NAME,
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("PPL Single Analysis result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("singleAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolPPLComparisonAnalysis() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"baselineTimeRangeStart\": \"2025-01-01 09:00:00\", \"baselineTimeRangeEnd\": \"2025-01-01 10:00:00\", \"queryType\": \"ppl\", \"ppl\": \"source=%s\"}}",
                    TEST_DATA_INDEX_NAME,
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("PPL Comparison Analysis result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("comparisonAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolPPLWithCustomQuery() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"queryType\": \"ppl\", \"ppl\": \"source=%s | where level > 2\"}}",
                    TEST_DATA_INDEX_NAME,
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("PPL Custom Query result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("singleAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolWithDSLQueryType() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"queryType\": \"dsl\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("DSL Query Type result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("singleAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolWithMultipleFilters() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"filter\": \"[\\\"{'term': {'status': 'error'}}\\\", \\\"{'range': {'level': {'gte': 3}}}\\\"]\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("Multiple Filters result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("singleAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolWithCustomSize() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"size\": \"500\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("Custom Size result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("singleAnalysis"));
    }

    @Test
    @SneakyThrows
    public void testDataDistributionToolWithCustomTimeField() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"timeField\": \"@timestamp\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );
        System.out.println("Custom Time Field result: " + result);
        assertNotNull(result);
        assertTrue(result.contains("singleAnalysis"));
    }
}
