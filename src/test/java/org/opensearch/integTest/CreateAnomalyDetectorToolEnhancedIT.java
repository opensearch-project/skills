/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.hamcrest.MatcherAssert;
import org.opensearch.agent.tools.CreateAnomalyDetectorToolEnhanced;
import org.opensearch.client.ResponseException;

import lombok.SneakyThrows;

public class CreateAnomalyDetectorToolEnhancedIT extends ToolIntegrationTest {
    private final String NORMAL_INDEX = "http_logs";
    private final String NORMAL_INDEX_WITH_NO_DATE_FIELDS = "normal_index_with_no_date_fields";
    private final String ABNORMAL_INDEX = "abnormal_index";

    @Override
    List<PromptHandler> promptHandlers() {
        PromptHandler createAnomalyDetectorToolHandler = new PromptHandler() {
            @Override
            String response(String prompt) {
                if (prompt.contains(NORMAL_INDEX)) {
                    int flag = randomIntBetween(0, 5);
                    switch (flag) {
                        case 0:
                            return "{category_field=|aggregation_field=response,responseLatency|aggregation_method=count,avg|interval=10}";
                        case 1:
                            return "{category_field=ip|aggregation_field=response,responseLatency|aggregation_method=count,avg|interval=10}";
                        case 2:
                            return "{category_field=|aggregation_field=responseLatency|aggregation_method=avg|interval=10}";
                        case 3:
                            return "{category_field=country.keyword|aggregation_field=response,responseLatency|aggregation_method=count,avg|interval=15}";
                        case 4:
                            return "{category_field=\"ip\"|aggregation_field=\"responseLatency\"|aggregation_method=\"avg\"|interval=10}";
                        case 5:
                            return "{category_field= |aggregation_field= responseLatency |aggregation_method= avg |interval=10}";
                        default:
                            return "{category_field=|aggregation_field=response|aggregation_method=count|interval=10}";
                    }
                } else {
                    return "wrong response";
                }
            }

            @Override
            boolean apply(String prompt) {
                return true;
            }
        };
        return List.of(createAnomalyDetectorToolHandler);
    }

    @Override
    String toolType() {
        return CreateAnomalyDetectorToolEnhanced.TYPE;
    }

    public void testCreateAnomalyDetectorToolEnhanced_SingleIndex() {
        prepareIndex();
        String agentId = registerAgent();
        String result = executeAgent(agentId, "{\"parameters\": {\"input\": \"{\\\"indices\\\":[\\\"" + NORMAL_INDEX + "\\\"]}\"}}");
        // Verify successful detector creation
        assertTrue(result.contains("indexName"));
        assertTrue(result.contains(NORMAL_INDEX));
        // Should have either success or a specific failure status
        assertTrue(result.contains("status"));
    }

    public void testCreateAnomalyDetectorToolEnhanced_MultipleIndices() {
        prepareIndex();
        String agentId = registerAgent();
        String result = executeAgent(
            agentId,
            "{\"parameters\": {\"input\": \"{\\\"indices\\\":[\\\""
                + NORMAL_INDEX
                + "\\\",\\\""
                + NORMAL_INDEX_WITH_NO_DATE_FIELDS
                + "\\\"]}\"}}"
        );

        // Both indices should be in response
        assertTrue(result.contains(NORMAL_INDEX));
        assertTrue(result.contains(NORMAL_INDEX_WITH_NO_DATE_FIELDS));

        // Both should have failed_validation status (one for LLM parsing, one for no date fields)
        assertTrue("Should contain failed_validation status", result.contains("\"status\":\"failed_validation\""));

        // Should contain specific error messages
        assertTrue("Should contain LLM parsing error", result.contains("Cannot parse LLM response"));
        assertTrue("Should contain no date fields error", result.contains("has no date fields"));
    }

    public void testCreateAnomalyDetectorToolEnhanced_WithNoDateFields() {
        prepareIndex();
        String agentId = registerAgent();
        String result = executeAgent(
            agentId,
            "{\"parameters\": {\"input\": \"{\\\"indices\\\":[\\\"" + NORMAL_INDEX_WITH_NO_DATE_FIELDS + "\\\"]}\"}}"
        );
        assertTrue(result.contains("failed_validation"));
        assertTrue(result.contains("no date fields"));
    }

    public void testCreateAnomalyDetectorToolEnhanced_WithSystemIndex() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"input\": \"{\\\"indices\\\":[\\\".test\\\"]}\"}}")
        );
        MatcherAssert.assertThat(exception.getMessage(), allOf(containsString("System indices not supported")));
    }

    public void testCreateAnomalyDetectorToolEnhanced_WithMissingIndex() {
        prepareIndex();
        String agentId = registerAgent();
        String result = executeAgent(agentId, "{\"parameters\": {\"input\": \"{\\\"indices\\\":[\\\"non-existent\\\"]}\"}}");
        assertTrue(result.contains("failed_validation"));
        assertTrue(result.contains("does not exist") || result.contains("no such index"));
    }

    public void testCreateAnomalyDetectorToolEnhanced_WithEmptyInput() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {}}"));
        MatcherAssert.assertThat(exception.getMessage(), allOf(containsString("Input parameter is required")));
    }

    @SneakyThrows
    private void prepareIndex() {
        createIndexWithConfiguration(
            NORMAL_INDEX,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"response\": {\n"
                + "        \"type\": \"integer\"\n"
                + "      },\n"
                + "      \"responseLatency\": {\n"
                + "        \"type\": \"float\"\n"
                + "      },\n"
                + "      \"ip\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      },\n"
                + "      \"country\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      },\n"
                + "      \"date\": {\n"
                + "        \"type\": \"date\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(
            NORMAL_INDEX,
            "0",
            List.of("response", "responseLatency", "ip", "country", "date"),
            List.of(200, 0.15, "192.168.1.1", "US", "2024-07-03T10:22:56,520")
        );
        addDocToIndex(
            NORMAL_INDEX,
            "1",
            List.of("response", "responseLatency", "ip", "country", "date"),
            List.of(200, 3.15, "192.168.1.2", "UK", "2024-07-03T10:22:57,520")
        );

        createIndexWithConfiguration(
            NORMAL_INDEX_WITH_NO_DATE_FIELDS,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"product\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(NORMAL_INDEX_WITH_NO_DATE_FIELDS, "0", List.of("product"), List.of("product1"));
        addDocToIndex(NORMAL_INDEX_WITH_NO_DATE_FIELDS, "1", List.of("product"), List.of("product2"));

        createIndexWithConfiguration(
            ABNORMAL_INDEX,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"date\": {\n"
                + "        \"type\": \"date\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(ABNORMAL_INDEX, "0", List.of("date"), List.of("2024-07-03T10:22:56,520"));
        addDocToIndex(ABNORMAL_INDEX, "1", List.of("date"), List.of("2024-07-03T10:22:57,520"));
    }

    @SneakyThrows
    private String registerAgent() {
        String registerAgentRequestBody = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource(
                                "org/opensearch/agent/tools/register_flow_agent_of_create_anomaly_detector_tool_enhanced_request_body.json"
                            )
                            .toURI()
                    )
            );
        registerAgentRequestBody = registerAgentRequestBody.replace("<MODEL_ID>", modelId);
        registerAgentRequestBody = registerAgentRequestBody
            .replace(
                "<CUSTOM_PROMPT>",
                "\n\nHuman: Analyze this index and suggest ONE anomaly detector for operational monitoring that generates clear, actionable alerts.\n\n"
                    + "Index: ${indexInfo.indexName}\n"
                    + "Mapping: ${indexInfo.indexMapping}\n"
                    + "Available date fields: ${dateFields}\n\n"
                    + "OUTPUT FORMAT:\n"
                    + "Return ONLY this structured format, no explanation:\n"
                    + "{category_field=field_name_or_empty|aggregation_field=field1,field2|aggregation_method=method1,method2|interval=minutes}\n\n"
                    + "Assistant:"
            );

        return createAgent(registerAgentRequestBody);
    }

    @SneakyThrows
    private String registerAgentWithWrongModelId() {
        String registerAgentRequestBody = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource(
                                "org/opensearch/agent/tools/register_flow_agent_of_create_anomaly_detector_tool_enhanced_request_body.json"
                            )
                            .toURI()
                    )
            );
        registerAgentRequestBody = registerAgentRequestBody.replace("<MODEL_ID>", "wrong_model_id");
        return createAgent(registerAgentRequestBody);
    }
}
