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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":0.75,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":0.75,\"baselinePercentage\":0.0},{\"value\":\"warning\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"2\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"server-03\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.25,\"topChanges\":[{\"value\":\"140.1\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"250.3\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

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

        String expectedResult =
            "{\"comparisonAnalysis\":[{\"field\":\"status\",\"divergence\":0.75,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":0.75,\"baselinePercentage\":0.0},{\"value\":\"success\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.67},{\"value\":\"info\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.33},{\"value\":\"warning\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"1\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.67},{\"value\":\"3\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"2\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.33},{\"value\":\"4\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"110.8\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.33},{\"value\":\"95.2\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.33},{\"value\":\"120.5\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.33},{\"value\":\"140.1\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"250.3\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.25,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.67},{\"value\":\"server-02\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.33},{\"value\":\"server-03\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":1.0,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":1.0,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"250.3\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

    @SneakyThrows
    public void testDataDistributionToolMissingRequiredParameters() {
        Exception exception = assertThrows(Exception.class, () -> executeAgent(agentId, "{\"parameters\": {\"index\": \"test_index\"}}"));
        MatcherAssert.assertThat(exception.getMessage(), containsString("Unable to parse time string"));
    }

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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":0.75,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":0.75,\"baselinePercentage\":0.0},{\"value\":\"warning\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"3.0\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"2.0\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"4.0\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"server-03\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.25,\"topChanges\":[{\"value\":\"140.1\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"250.3\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

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

        String expectedResult =
            "{\"comparisonAnalysis\":[{\"field\":\"status\",\"divergence\":0.75,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":0.75,\"baselinePercentage\":0.0},{\"value\":\"success\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.67},{\"value\":\"info\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.33},{\"value\":\"warning\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"1.0\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.67},{\"value\":\"3.0\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"2.0\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.33},{\"value\":\"4.0\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"110.8\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.33},{\"value\":\"95.2\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.33},{\"value\":\"120.5\",\"selectionPercentage\":0.0,\"baselinePercentage\":0.33},{\"value\":\"140.1\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"250.3\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.25,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.67},{\"value\":\"server-02\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.33},{\"value\":\"server-03\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":1.0,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":1.0,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"3.0\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"4.0\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"250.3\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":0.75,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":0.75,\"baselinePercentage\":0.0},{\"value\":\"warning\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"2\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"server-03\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.25,\"topChanges\":[{\"value\":\"140.1\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"250.3\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":1.0,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":1.0,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"250.3\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":0.75,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":0.75,\"baselinePercentage\":0.0},{\"value\":\"warning\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"2\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"server-03\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.25,\"topChanges\":[{\"value\":\"140.1\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"250.3\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":0.75,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":0.75,\"baselinePercentage\":0.0},{\"value\":\"warning\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"2\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"server-03\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.25,\"topChanges\":[{\"value\":\"140.1\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"250.3\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

    @SneakyThrows
    public void testDataDistributionToolWithRangeFilter() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"filter\": \"[\\\"{'range': {'response_time': {'gte': 150.0}}}\\\"]\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":1.0,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":1.0,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"250.3\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

    @SneakyThrows
    public void testDataDistributionToolWithMatchFilter() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"filter\": \"[\\\"{'match': {'status': 'error'}}\\\"]\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":1.0,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":1.0,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"250.3\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

    @SneakyThrows
    public void testDataDistributionToolWithRawDSLQuery() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"dsl\": \"{\\\"bool\\\": {\\\"must\\\": [{\\\"term\\\": {\\\"status\\\": \\\"error\\\"}}]}}\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":1.0,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":1.0,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"250.3\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

    @SneakyThrows
    public void testDataDistributionToolWithExistsFilter() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"filter\": \"[\\\"{'exists': {'field': 'response_time'}}\\\"]\"}}",
                    TEST_DATA_INDEX_NAME
                )
        );

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":0.75,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":0.75,\"baselinePercentage\":0.0},{\"value\":\"warning\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"3\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"2\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"4\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.5,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.5,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"server-03\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.25,\"topChanges\":[{\"value\":\"140.1\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"250.3\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.25,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }

    @SneakyThrows
    public void testDataDistributionToolInvalidFilterFormat() {
        Exception exception = assertThrows(
            Exception.class,
            () -> executeAgent(
                agentId,
                String
                    .format(
                        Locale.ROOT,
                        "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"filter\": \"invalid-json\"}}",
                        TEST_DATA_INDEX_NAME
                    )
            )
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("Invalid 'filter' parameter"));
    }

    @SneakyThrows
    public void testDataDistributionToolInvalidSizeParameter() {
        Exception exception = assertThrows(
            Exception.class,
            () -> executeAgent(
                agentId,
                String
                    .format(
                        Locale.ROOT,
                        "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"2025-01-01 10:00:00\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\", \"size\": \"not-a-number\"}}",
                        TEST_DATA_INDEX_NAME
                    )
            )
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("Invalid 'size' parameter"));
    }

    @SneakyThrows
    public void testDataDistributionToolInvalidTimeFormat() {
        Exception exception = assertThrows(
            Exception.class,
            () -> executeAgent(
                agentId,
                String
                    .format(
                        Locale.ROOT,
                        "{\"parameters\": {\"index\": \"%s\", \"selectionTimeRangeStart\": \"invalid-time-format\", \"selectionTimeRangeEnd\": \"2025-01-01 11:00:00\"}}",
                        TEST_DATA_INDEX_NAME
                    )
            )
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("Unable to parse time string"));
    }

    @SneakyThrows
    public void testDataDistributionToolPPLWithComplexQuery() {
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

        String expectedResult =
            "{\"singleAnalysis\":[{\"field\":\"status\",\"divergence\":1.0,\"topChanges\":[{\"value\":\"error\",\"selectionPercentage\":1.0,\"baselinePercentage\":0.0}]},{\"field\":\"level\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"3.0\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"4.0\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"host\",\"divergence\":0.6666666666666666,\"topChanges\":[{\"value\":\"server-01\",\"selectionPercentage\":0.67,\"baselinePercentage\":0.0},{\"value\":\"server-02\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]},{\"field\":\"response_time\",\"divergence\":0.3333333333333333,\"topChanges\":[{\"value\":\"250.3\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"180.7\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0},{\"value\":\"300.5\",\"selectionPercentage\":0.33,\"baselinePercentage\":0.0}]}]}";
        assertEquals(expectedResult, result);
    }
}
