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
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.hamcrest.MatcherAssert;
import org.opensearch.agent.tools.CreateAlertTool;
import org.opensearch.agent.tools.PPLTool;
import org.opensearch.client.ResponseException;

@Log4j2
public class CreateAlertToolIT extends ToolIntegrationTest {
    private final String requestBodyResourceFile = "org/opensearch/agent/tools/register_flow_agent_of_create_alert_tool_request_body.json";

    @Override
    List<PromptHandler> promptHandlers() {
        PromptHandler CreateAlertHandler = new PromptHandler() {
            @Override
            String response(String prompt) {
                if (prompt.contains("correct")) {
                    return "source=employee | where age > 56 | stats COUNT() as cnt";
                } else {
                    return "source=employee | asd";
                }
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
        String agentId = registerAgent(modelId, requestBodyResourceFile);
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"Create alert on the index when count of peoples whose age greater than 50 exceeds 10\", \"indices\": \"[employee]\"}}");
        assertEquals(
            "",
            result
        );
    }

    @SneakyThrows
    private void prepareIndex() {
        String testIndexName = "employee";
        createIndexWithConfiguration(
            testIndexName,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"age\": {\n"
                + "        \"type\": \"long\"\n"
                + "      },\n"
                + "      \"name\": {\n"
                + "        \"type\": \"text\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(testIndexName, "0", List.of("age", "name"), List.of(56, "john"));
        addDocToIndex(testIndexName, "1", List.of("age", "name"), List.of(56, "smith"));
    }

}
