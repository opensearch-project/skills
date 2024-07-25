/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.opensearch.agent.tools.CreateAlertTool;
import org.opensearch.client.ResponseException;

@Log4j2
public class CreateAlertToolIT extends ToolIntegrationTest {
    private final String requestBodyResourceFile = "org/opensearch/agent/tools/register_flow_agent_of_create_alert_tool_request_body.json";
    private final String NORMAL_INDEX = "normal_index";
    private final String NON_EXISTENT_INDEX = "non-existent";
    private final String SYSTEM_INDEX = ".kibana";

    private final String alertJson = "{\"name\": \"Error 500 Response Alert\",\"search\": {\"indices\": [\"opensearch_dashboards_sample_data_logs\"],\"timeField\": \"timestamp\",\"bucketValue\": 60,\"bucketUnitOfTime\": \"m\",\"filters\": [{\"fieldName\": [{\"label\": \"response\",\"type\": \"text\"}],\"fieldValue\": \"500\",\"operator\": \"is\"}],\"aggregations\": [{\"aggregationType\": \"count\",\"fieldName\": \"bytes\"}]},\"triggers\": [{\"name\": \"Error 500 Response Count Above 1\",\"severity\": 1,\"thresholdValue\": 1,\"thresholdEnum\": \"ABOVE\"}]}";
    private final String question = "Create alert on the index when count of peoples whose age greater than 50 exceeds 10";
    private final String pureJsonResponseIndicator = "$PURE_JSON";
    private final String noJsonResponseIndicator = "$NO_JSON";

    private String agentId;

    @Before
    public void registerAgent() throws IOException, InterruptedException {
        agentId = registerAgent(modelId, requestBodyResourceFile);
    }

    @Override
    List<PromptHandler> promptHandlers() {
        PromptHandler CreateAlertHandler = new PromptHandler() {
            @Override
            String response(String prompt) {
                if (prompt.contains(pureJsonResponseIndicator)) {
                    return alertJson;
                } else if (prompt.contains(noJsonResponseIndicator)) {
                    return "No json response";
                }
                return "This is output: ```json" + alertJson + "```";
            }

            @Override
            boolean apply(String prompt) {
                return true;
            }
        };
        return List.of(CreateAlertHandler);
    }

    @Override
    String toolType() {
        return CreateAlertTool.TYPE;
    }

    @SneakyThrows
    public void testCreateAlertTool() {
        prepareIndex();
        String requestBody =  String.format("{\"parameters\": {\"question\": \"%s\", \"indices\": \"[%s]\"}}", question, NORMAL_INDEX);
        String result = executeAgent(agentId, requestBody);
        assertEquals(
            alertJson,
            result
        );
    }

    public void testCreateAlertToolWithPureJsonResponse() {
        prepareIndex();
        String requestBody =  String.format("{\"parameters\": {\"question\": \"%s\", \"indices\": \"[%s]\"}}", question + pureJsonResponseIndicator, NORMAL_INDEX);
        String result = executeAgent(agentId, requestBody);
        assertEquals(
            alertJson,
            result
        );
    }

    public void testCreateAlertToolWithNoJsonResponse() {
        prepareIndex();
        String requestBody =  String.format("{\"parameters\": {\"question\": \"%s\", \"indices\": \"[%s]\"}}", question + noJsonResponseIndicator, NORMAL_INDEX);
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, requestBody)
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("The response from LLM is not a json"));
    }

    public void testCreateAlertToolWithNonExistentModelId() {
        prepareIndex();
        String abnormalAgentId = registerAgent("NON_EXISTENT_MODEL_ID", requestBodyResourceFile);
        String requestBody =  String.format("{\"parameters\": {\"question\": \"%s\", \"indices\": \"[%s]\"}}", question, NORMAL_INDEX);
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(abnormalAgentId, requestBody)
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("Failed to find model"));
    }

    public void testCreateAlertToolWithNonExistentIndex() {
        prepareIndex();
        String requestBody =  String.format("{\"parameters\": {\"question\": \"%s\", \"indices\": \"[%s]\"}}", question, NON_EXISTENT_INDEX);
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, requestBody)
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("no such index"));
    }

    public void testCreateAlertToolWithSystemIndex() {
        prepareIndex();
        String agentId = registerAgent(modelId, requestBodyResourceFile);
        String requestBody =  String.format("{\"parameters\": {\"question\": \"%s\", \"indices\": \"[%s]\"}}", question, SYSTEM_INDEX);
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, requestBody)
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("contains system index, which is not allowed"));
    }

    public void testCreateAlertToolWithEmptyIndex() {
        prepareIndex();
        String agentId = registerAgent(modelId, requestBodyResourceFile);
        String requestBody =  String.format("{\"parameters\": {\"question\": \"%s\", \"indices\": \"\"}}", question);
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, requestBody)
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("No indices in the input parameter"));
    }

    @SneakyThrows
    private void prepareIndex() {
        createIndexWithConfiguration(
            NORMAL_INDEX,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"response\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"bytes\": {\n"
                + "        \"type\": \"long\"\n"
                + "      },\n"
                + "      \"timestamp\": {\n"
                + "        \"type\": \"date\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(NORMAL_INDEX, "0", List.of("response", "bytes", "timestamp"), List.of(200, 1, "2024-07-03T10:22:56,520"));
        addDocToIndex(NORMAL_INDEX, "1", List.of("response", "bytes", "timestamp"), List.of(200, 2, "2024-07-03T10:22:57,520"));
    }

}
