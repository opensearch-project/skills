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
import org.opensearch.agent.tools.PPLTool;
import org.opensearch.client.ResponseException;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class PPLToolIT extends ToolIntegrationTest {

    @Override
    List<PromptHandler> promptHandlers() {
        PromptHandler PPLHandler = new PromptHandler() {
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
        return List.of(PPLHandler);
    }

    @Override
    String toolType() {
        return PPLTool.TYPE;
    }

    @SneakyThrows
    public void testPPLTool() {
        prepareIndex();
        String agentId = registerAgent();
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"correct\", \"index\": \"employee\"}}");
        assertEquals(
            "{\"ppl\":\"source\\u003demployee| where age \\u003e 56 | stats COUNT() as cnt\",\"executionResult\":\"{\\n  \\\"schema\\\": [\\n    {\\n      \\\"name\\\": \\\"cnt\\\",\\n      \\\"type\\\": \\\"integer\\\"\\n    }\\n  ],\\n  \\\"datarows\\\": [\\n    [\\n      0\\n    ]\\n  ],\\n  \\\"total\\\": 1,\\n  \\\"size\\\": 1\\n}\"}",
            result
        );
    }

    public void test_PPLTool_whenPPLExecutionDisabled_ResultOnlyContainsPPL() {
        updateClusterSettings("plugins.skills.ppl_execution_enabled", false);
        prepareIndex();
        String agentId = registerAgent();
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"correct\", \"index\": \"employee\"}}");
        assertEquals(
            "{\"ppl\":\"source\\u003demployee| where age \\u003e 56 | stats COUNT() as cnt\"}",
            result
        );
    }

    public void testPPLTool_withWrongPPLGenerated_thenThrowException() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"wrong\", \"index\": \"employee\"}}")
        );
        MatcherAssert.assertThat(exception.getMessage(), allOf(containsString("execute ppl:source=employee| asd, get error")));

    }

    public void testPPLTool_withWrongModelId_thenThrowException() {
        prepareIndex();
        String agentId = registerAgentWithWrongModelId();
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"correct\", \"index\": \"employee\"}}")
        );
        MatcherAssert.assertThat(exception.getMessage(), allOf(containsString("Failed to find model")));

    }

    public void testPPLTool_withSystemQuery_thenThrowException() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"correct\", \"index\": \".employee\"}}")
        );
        MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString(
                        "PPLTool doesn't support searching indices starting with '.' since it could be system index, current searching index name: .employee"
                    )
                )
            );

    }

    public void testPPLTool_withNonExistingIndex_thenThrowException() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(
            ResponseException.class,
            () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"correct\", \"index\": \"employee2\"}}")
        );
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

    public void testPPLTool_withBlankInput_thenThrowException() {
        prepareIndex();
        String agentId = registerAgent();
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));
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
    private String registerAgent() {
        String registerAgentRequestBody = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_ppl_tool_request_body.json")
                            .toURI()
                    )
            );
        registerAgentRequestBody = registerAgentRequestBody.replace("<MODEL_ID>", modelId);
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
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_ppl_tool_request_body.json")
                            .toURI()
                    )
            );
        registerAgentRequestBody = registerAgentRequestBody.replace("<MODEL_ID>", "wrong_model_id");
        return createAgent(registerAgentRequestBody);
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
