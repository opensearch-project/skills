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
import org.opensearch.agent.tools.CreateAnomalyDetectorTool;
import org.opensearch.client.ResponseException;

import lombok.SneakyThrows;

public class CreateAnomalyDetectorToolIT extends ToolIntegrationTest {
    private final String NORMAL_INDEX = "http_logs";
    private final String NORMAL_INDEX_WITH_NO_AVAILABLE_FIELDS = "products";
    private final String NORMAL_INDEX_WITH_NO_DATE_FIELDS = "normal_index_with_no_date_fields";
    private final String NORMAL_INDEX_WITH_NO_MAPPING = "normal_index_with_no_mapping";
    private final String ABNORMAL_INDEX = "abnormal_index";

    @Override
    List<PromptHandler> promptHandlers() {
        PromptHandler createAnomalyDetectorToolHandler = new PromptHandler() {
            @Override
            String response(String prompt) {
                int flag;
                if (prompt.contains(NORMAL_INDEX)) {
                    flag = randomIntBetween(0, 9);
                    switch (flag) {
                        case 0:
                            return "{category_field=|aggregation_field=response,responseLatency|aggregation_method=count,avg}";
                        case 1:
                            return "{category_field=ip|aggregation_field=response,responseLatency|aggregation_method=count,avg}";
                        case 2:
                            return "{category_field=|aggregation_field=responseLatency|aggregation_method=avg}";
                        case 3:
                            return "{category_field=country.keyword|aggregation_field=response,responseLatency|aggregation_method=count,avg}";
                        case 4:
                            return "{category_field=country.keyword|aggregation_field=response.keyword|aggregation_method=count}";
                        case 5:
                            return "{category_field=\"country.keyword\"|aggregation_field=\"response,responseLatency\"|aggregation_method=\"count,avg\"}";
                        case 6:
                            return "{category_field=ip|aggregation_field=responseLatency|aggregation_method=avg}";
                        case 7:
                            return "{category_field=\"ip\"|aggregation_field=\"responseLatency\"|aggregation_method=\"avg\"}";
                        case 8:
                            return "{category_field= ip |aggregation_field= responseLatency |aggregation_method= avg }";
                        case 9:
                            return "{category_field=\" ip \"|aggregation_field=\" responseLatency \"|aggregation_method=\" avg \"}";
                        default:
                            return "{category_field=|aggregation_field=response|aggregation_method=count}";
                    }
                } else if (prompt.contains(NORMAL_INDEX_WITH_NO_AVAILABLE_FIELDS)) {
                    flag = randomIntBetween(0, 9);
                    switch (flag) {
                        case 0:
                            return "{category_field=|aggregation_field=|aggregation_method=}";
                        case 1:
                            return "{category_field= |aggregation_field= |aggregation_method= }";
                        case 2:
                            return "{category_field=\"\"|aggregation_field=\"\"|aggregation_method=\"\"}";
                        case 3:
                            return "{category_field=product|aggregation_field=|aggregation_method=sum}";
                        case 4:
                            return "{category_field=product|aggregation_field=sales|aggregation_method=}";
                        case 5:
                            return "{category_field=product|aggregation_field=\"\"|aggregation_method=sum}";
                        case 6:
                            return "{category_field=product|aggregation_field=sales|aggregation_method=\"\"}";
                        case 7:
                            return "{category_field=product|aggregation_field= |aggregation_method=sum}";
                        case 8:
                            return "{category_field=product|aggregation_field=sales |aggregation_method= }";
                        case 9:
                            return "{category_field=\"\"|aggregation_field= |aggregation_method=\"\" }";
                        default:
                            return "{category_field=product|aggregation_field= |aggregation_method= }";
                    }
                } else {
                    flag = randomIntBetween(0, 1);
                    switch (flag) {
                        case 0:
                            return "wrong response";
                        case 1:
                            return "{category_field=product}";
                        default:
                            return "{category_field=}";
                    }
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
        return CreateAnomalyDetectorTool.TYPE;
    }

    public void testCreateAnomalyDetectorTool() {
        prepareIndex();
        String agentId = registerAgent();
        String index;
        if (randomIntBetween(0, 1) == 0) {
            index = NORMAL_INDEX;
        } else {
            index = NORMAL_INDEX_WITH_NO_AVAILABLE_FIELDS;
        }
        String result = executeAgent(agentId, "{\"parameters\": {\"index\":\"" + index + "\"}}");
        assertTrue(result.contains("index"));
        assertTrue(result.contains("categoryField"));
        assertTrue(result.contains("aggregationField"));
        assertTrue(result.contains("aggregationMethod"));
        assertTrue(result.contains("dateFields"));
    }

    public void testCreateAnomalyDetectorToolWithNonExistentModelId() {
        prepareIndex();
        String agentId = registerAgentWithWrongModelId();
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"index\":\"" + ABNORMAL_INDEX + "\"}}")
        );
        MatcherAssert.assertThat(exception.getMessage(), allOf(containsString("Failed to find model")));
    }

    public void testCreateAnomalyDetectorToolWithUnexpectedResult() {
        prepareIndex();
        String agentId = registerAgent();

        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"index\":\"" + NORMAL_INDEX_WITH_NO_MAPPING + "\"}}")
        );
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString(
                        "The index "
                            + NORMAL_INDEX_WITH_NO_MAPPING
                            + " doesn't have mapping metadata, please add data to it or using another index."
                    )
                )
            );

        exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"index\":\"" + NORMAL_INDEX_WITH_NO_DATE_FIELDS + "\"}}")
        );
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString(
                        "The index "
                            + NORMAL_INDEX_WITH_NO_DATE_FIELDS
                            + " doesn't have date type fields, cannot create an anomaly detector for it."
                    )
                )
            );

        exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"index\":\"" + ABNORMAL_INDEX + "\"}}")
        );
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString(
                        "The inference result from remote endpoint is not valid, cannot extract the key information from the result."
                    )
                )
            );
    }

    public void testCreateAnomalyDetectorToolWithSystemIndex() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"index\": \".test\"}}")
        );
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString(
                        "CreateAnomalyDetectionTool doesn't support searching indices starting with '.' since it could be system index, current searching index name: .test"
                    )
                )
            );
    }

    public void testCreateAnomalyDetectorToolWithMissingIndex() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"index\": \"non-existent\"}}")
        );
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString(
                        "Return this final answer to human directly and do not use other tools: 'The index doesn't exist, please provide another index and retry'. Please try to directly send this message to human to ask for index name"
                    )
                )
            );
    }

    public void testCreateAnomalyDetectorToolWithEmptyInput() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {}}"));
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString(
                        "Return this final answer to human directly and do not use other tools: 'Please provide index name'. Please try to directly send this message to human to ask for index name"
                    )
                )
            );
    }

    @SneakyThrows
    private void prepareIndex() {
        createIndexWithConfiguration(
            NORMAL_INDEX,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"response\": {\n"
                + "        \"type\": \"keyword\"\n"
                + "      },\n"
                + "      \"responseLatency\": {\n"
                + "        \"type\": \"float\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(NORMAL_INDEX, "0", List.of("response", "responseLatency", "date"), List.of(200, 0.15, "2024-07-03T10:22:56,520"));
        addDocToIndex(NORMAL_INDEX, "1", List.of("response", "responseLatency", "date"), List.of(200, 3.15, "2024-07-03T10:22:57,520"));

        createIndexWithConfiguration(
            NORMAL_INDEX_WITH_NO_AVAILABLE_FIELDS,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"product\": {\n"
                + "   "
                + "     \"type\": \"keyword\"\n"
                + "      }\n"
                + " }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(NORMAL_INDEX_WITH_NO_AVAILABLE_FIELDS, "0", List.of("product", "date"), List.of(1, "2024-07-03T10:22:56,520"));
        addDocToIndex(NORMAL_INDEX_WITH_NO_AVAILABLE_FIELDS, "1", List.of("product", "date"), List.of(2, "2024-07-03T10:22:57,520"));

        createIndexWithConfiguration(
            NORMAL_INDEX_WITH_NO_DATE_FIELDS,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"product\": {\n"
                + "   "
                + "     \"type\": \"keyword\"\n"
                + "      }\n"
                + " }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(NORMAL_INDEX_WITH_NO_DATE_FIELDS, "0", List.of("product"), List.of(1));
        addDocToIndex(NORMAL_INDEX_WITH_NO_DATE_FIELDS, "1", List.of("product"), List.of(2));

        createIndexWithConfiguration(NORMAL_INDEX_WITH_NO_MAPPING, "{}");

        createIndexWithConfiguration(
            ABNORMAL_INDEX,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"date\": {\n"
                + "   "
                + "     \"type\": \"date\"\n"
                + "      }\n"
                + " }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(ABNORMAL_INDEX, "0", List.of("date"), List.of(1, "2024-07-03T10:22:56,520"));
        addDocToIndex(ABNORMAL_INDEX, "1", List.of("date"), List.of(2, "2024-07-03T10:22:57,520"));
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
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_create_anomaly_detector_tool_request_body.json")
                            .toURI()
                    )
            );
        registerAgentRequestBody = registerAgentRequestBody.replace("<MODEL_ID>", "non-existent");
        return createAgent(registerAgentRequestBody);
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
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_create_anomaly_detector_tool_request_body.json")
                            .toURI()
                    )
            );
        registerAgentRequestBody = registerAgentRequestBody.replace("<MODEL_ID>", modelId);
        registerAgentRequestBody = registerAgentRequestBody
            .replace(
                "<CUSTOM_PROMPT>",
                "Here is an example of the create anomaly detector API: POST _plugins/_anomaly_detection/detectors, "
                    + " {\\\"time_field\\\":\\\"timestamp\\\",\\\"indices\\\":[\\\"server_log*\\\"],\\\"feature_attributes\\\":"
                    + "[{\\\"feature_name\\\":\\\"test\\\",\\\"feature_enabled\\\":true,"
                    + "\\\"aggregation_query\\\":{\\\"test\\\":{\\\"sum\\\":{\\\"field\\\":\\\"value\\\"}}}}],\\\"category_field\\\":[\\\"ip\\\"]},"
                    + " and here are the mapping info containing all the fields in the index ${indexInfo.indexName}: ${indexInfo.indexMapping}, "
                    + "and the optional aggregation methods are count, avg, min, max and sum. Please give me some suggestion about creating an anomaly detector "
                    + "for the index ${indexInfo.indexName}, "
                    + "you need to give the key information: the top 3 suitable aggregation fields which are numeric types and "
                    + "the suitable aggregation method for each field, "
                    + "if there are no numeric type fields, both the aggregation field and method are empty string, "
                    + " and also give the category field if there exists a keyword type field like ip, address, host, city, country or region,"
                    + " if not exist, the category field is empty. Show me a format of keyed and pipe-delimited list "
                    + "wrapped in a curly bracket just like {category_field=the category field if exists|aggregation_field=comma-delimited"
                    + " list of all the aggregation field names|aggregation_method=comma-delimited list of all the aggregation methods}. "
            );

        return createAgent(registerAgentRequestBody);
    }
}
