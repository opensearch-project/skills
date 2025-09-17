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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.75, selectionPercentage, 0.01);
                    } else if ("warning".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("4".equals(value) || "2".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value) || "server-03".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.25, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray comparisonAnalysis = jsonResult.getAsJsonArray("comparisonAnalysis");

        for (JsonElement element : comparisonAnalysis) {
            JsonObject fieldComparison = element.getAsJsonObject();
            String fieldName = fieldComparison.get("field").getAsString();
            JsonArray topChanges = fieldComparison.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();
                    double baselinePercentage = changeObj.get("baselinePercentage").getAsDouble();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();

                    if ("error".equals(value)) {
                        assertEquals(0.0, baselinePercentage, 0.01);
                        assertEquals(0.75, selectionPercentage, 0.01);
                    } else if ("success".equals(value)) {
                        assertEquals(0.667, baselinePercentage, 0.01);
                        assertEquals(0.0, selectionPercentage, 0.01);
                    } else if ("info".equals(value)) {
                        assertEquals(0.333, baselinePercentage, 0.01);
                        assertEquals(0.0, selectionPercentage, 0.01);
                    } else if ("warning".equals(value)) {
                        assertEquals(0.0, baselinePercentage, 0.01);
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(1.0, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("4".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.75, selectionPercentage, 0.01);
                    } else if ("warning".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("4".equals(value) || "2".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value) || "server-03".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.25, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray comparisonAnalysis = jsonResult.getAsJsonArray("comparisonAnalysis");

        for (JsonElement element : comparisonAnalysis) {
            JsonObject fieldComparison = element.getAsJsonObject();
            String fieldName = fieldComparison.get("field").getAsString();
            JsonArray topChanges = fieldComparison.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();
                    double baselinePercentage = changeObj.get("baselinePercentage").getAsDouble();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();

                    if ("error".equals(value)) {
                        assertEquals(0.0, baselinePercentage, 0.01);
                        assertEquals(0.75, selectionPercentage, 0.01);
                    } else if ("success".equals(value)) {
                        assertEquals(0.667, baselinePercentage, 0.01);
                        assertEquals(0.0, selectionPercentage, 0.01);
                    } else if ("info".equals(value)) {
                        assertEquals(0.333, baselinePercentage, 0.01);
                        assertEquals(0.0, selectionPercentage, 0.01);
                    } else if ("warning".equals(value)) {
                        assertEquals(0.0, baselinePercentage, 0.01);
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(1.0, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("4".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.333, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.75, selectionPercentage, 0.01);
                    } else if ("warning".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("4".equals(value) || "2".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value) || "server-03".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.25, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(1.0, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("4".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.333, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.75, selectionPercentage, 0.01);
                    } else if ("warning".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("4".equals(value) || "2".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value) || "server-03".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.25, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.75, selectionPercentage, 0.01);
                    } else if ("warning".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("4".equals(value) || "2".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value) || "server-03".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.25, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(1.0, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("4".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.333, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(1.0, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("4".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.333, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(1.0, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("4".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.333, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.75, selectionPercentage, 0.01);
                    } else if ("warning".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("4".equals(value) || "2".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.5, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value) || "server-03".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.25, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.25, selectionPercentage, 0.01);
                }
            }
        }
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

        JsonObject jsonResult = JsonParser.parseString(result).getAsJsonObject();
        JsonArray singleAnalysis = jsonResult.getAsJsonArray("singleAnalysis");

        for (JsonElement element : singleAnalysis) {
            JsonObject fieldAnalysis = element.getAsJsonObject();
            String fieldName = fieldAnalysis.get("field").getAsString();
            JsonArray topChanges = fieldAnalysis.getAsJsonArray("topChanges");

            if ("status".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("error".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(1.0, selectionPercentage, 0.01);
                    }
                }
            } else if ("level".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("3".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("4".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("host".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    String value = changeObj.get("value").getAsString();

                    if ("server-01".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.667, selectionPercentage, 0.01);
                    } else if ("server-02".equals(value)) {
                        double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                        assertEquals(0.333, selectionPercentage, 0.01);
                    }
                }
            } else if ("response_time".equals(fieldName)) {
                for (JsonElement change : topChanges) {
                    JsonObject changeObj = change.getAsJsonObject();
                    double selectionPercentage = changeObj.get("selectionPercentage").getAsDouble();
                    assertEquals(0.333, selectionPercentage, 0.01);
                }
            }
        }
    }
}
