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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Parse and validate JSON structure
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("singleAnalysis should contain at least one field analysis", singleAnalysis.getAsJsonArray().size() > 0);

        // Verify structure of first analysis item
        JsonElement firstItem = singleAnalysis.getAsJsonArray().get(0);
        assertTrue("Analysis item should be a JSON object", firstItem.isJsonObject());
        assertTrue("Analysis item should have 'field' property", firstItem.getAsJsonObject().has("field"));
        assertTrue("Analysis item should have 'divergence' property", firstItem.getAsJsonObject().has("divergence"));
        assertTrue("Analysis item should have 'topChanges' property", firstItem.getAsJsonObject().has("topChanges"));
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain comparisonAnalysis", result.contains("comparisonAnalysis"));

        // Parse and validate JSON structure
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have comparisonAnalysis property", jsonResult.getAsJsonObject().has("comparisonAnalysis"));

        JsonElement comparisonAnalysis = jsonResult.getAsJsonObject().get("comparisonAnalysis");
        assertTrue("comparisonAnalysis should be a JSON array", comparisonAnalysis.isJsonArray());
        assertTrue("comparisonAnalysis should contain at least one field comparison", comparisonAnalysis.getAsJsonArray().size() > 0);

        // Verify structure of first comparison item
        JsonElement firstItem = comparisonAnalysis.getAsJsonArray().get(0);
        assertTrue("Comparison item should be a JSON object", firstItem.isJsonObject());
        assertTrue("Comparison item should have 'field' property", firstItem.getAsJsonObject().has("field"));
        assertTrue("Comparison item should have 'divergence' property", firstItem.getAsJsonObject().has("divergence"));
        assertTrue("Comparison item should have 'topChanges' property", firstItem.getAsJsonObject().has("topChanges"));

        // Verify divergence is a valid number
        double divergence = firstItem.getAsJsonObject().get("divergence").getAsDouble();
        assertTrue("Divergence should be non-negative", divergence >= 0.0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Parse and validate JSON structure with filter applied
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("singleAnalysis should contain field analyses even with filter", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Parse and validate PPL query results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("PPL singleAnalysis should contain field analyses", singleAnalysis.getAsJsonArray().size() > 0);

        // Verify PPL results have proper structure
        JsonElement firstItem = singleAnalysis.getAsJsonArray().get(0);
        assertTrue("PPL analysis item should have 'field' property", firstItem.getAsJsonObject().has("field"));
        assertTrue("PPL analysis item should have 'divergence' property", firstItem.getAsJsonObject().has("divergence"));
        assertTrue("PPL analysis item should have 'topChanges' property", firstItem.getAsJsonObject().has("topChanges"));
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain comparisonAnalysis", result.contains("comparisonAnalysis"));

        // Parse and validate PPL comparison results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have comparisonAnalysis property", jsonResult.getAsJsonObject().has("comparisonAnalysis"));

        JsonElement comparisonAnalysis = jsonResult.getAsJsonObject().get("comparisonAnalysis");
        assertTrue("comparisonAnalysis should be a JSON array", comparisonAnalysis.isJsonArray());
        assertTrue("PPL comparisonAnalysis should contain field comparisons", comparisonAnalysis.getAsJsonArray().size() > 0);

        // Verify PPL comparison results have proper structure
        JsonElement firstItem = comparisonAnalysis.getAsJsonArray().get(0);
        assertTrue("PPL comparison item should have 'field' property", firstItem.getAsJsonObject().has("field"));
        assertTrue("PPL comparison item should have 'divergence' property", firstItem.getAsJsonObject().has("divergence"));
        assertTrue("PPL comparison item should have 'topChanges' property", firstItem.getAsJsonObject().has("topChanges"));
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate custom PPL query results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("Custom PPL query should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate DSL query type results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("DSL query should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate multiple filters results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("Multiple filters should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate custom size parameter results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("Custom size should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate custom time field results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("Custom time field should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate range filter results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("Range filter should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate match filter results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("Match filter should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate exists filter results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("Exists filter should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
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
        assertNotNull("Result should not be null", result);
        assertTrue("Result should contain singleAnalysis", result.contains("singleAnalysis"));

        // Validate complex PPL query results
        JsonElement jsonResult = JsonParser.parseString(result);
        assertTrue("Result should be a JSON object", jsonResult.isJsonObject());
        assertTrue("Result should have singleAnalysis property", jsonResult.getAsJsonObject().has("singleAnalysis"));

        JsonElement singleAnalysis = jsonResult.getAsJsonObject().get("singleAnalysis");
        assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
        assertTrue("Complex PPL query should return field analyses", singleAnalysis.getAsJsonArray().size() > 0);
    }
}
