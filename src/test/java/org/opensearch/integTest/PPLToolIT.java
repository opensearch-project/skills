/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.opensearch.agent.tools.PPLTool;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class PPLToolIT extends ToolIntegrationTest {

    private String TEST_INDEX_NAME = "employee";

    @Override
    List<PromptHandler> promptHandlers() {
        PromptHandler PPLHandler = new PromptHandler() {
            @Override
            String response(String prompt) {
                return "source=employee | where age > 56 | stats COUNT() as cnt";
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
        prepareIndex();
        String agentId = createAgent(registerAgentRequestBody);
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\", \"index\": \"employee\"}}");
        assertEquals(
            "{\"ppl\":\"source\\u003demployee| where age \\u003e 56 | stats COUNT() as cnt\",\"executionResult\":\"{\\n  \\\"schema\\\": [\\n    {\\n      \\\"name\\\": \\\"cnt\\\",\\n      \\\"type\\\": \\\"integer\\\"\\n    }\\n  ],\\n  \\\"datarows\\\": [\\n    [\\n      0\\n    ]\\n  ],\\n  \\\"total\\\": 1,\\n  \\\"size\\\": 1\\n}\"}",
            result
        );
        log.info(result);
    }

    @SneakyThrows
    private void prepareIndex() {
        createIndexWithConfiguration(
            TEST_INDEX_NAME,
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
        addDocToIndex(TEST_INDEX_NAME, "0", List.of("age", "name"), List.of(56, "john"));
        addDocToIndex(TEST_INDEX_NAME, "1", List.of("age", "name"), List.of(56, "smith"));
    }

}
