/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Before;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class PainlessToolIT extends BaseAgentToolsIT {

    private String registerAgentRequestBody;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        registerAgentRequestBody = Files
            .readString(
                Path.of(this.getClass().getClassLoader().getResource("org/opensearch/agent/tools/register_painless_agent.json").toURI())
            );
    }

    public void test_execute() {
        String script = "def x = new HashMap(); x.abc = '5'; return x.abc;";
        String agentRequestBody = registerAgentRequestBody.replaceAll("<SCRIPT>", script);
        String agentId = createAgent(agentRequestBody);
        String agentInput = "{\"parameters\":{}}";
        String result = executeAgent(agentId, agentInput);
        Assert.assertEquals("5", result);
    }

    public void test_execute_with_parameter() {
        String script = "params.x + params.y";
        String agentRequestBody = registerAgentRequestBody.replaceAll("<SCRIPT>", script);
        String agentId = createAgent(agentRequestBody);
        String agentInput = "{\"parameters\":{\"x\":1,\"y\":2}}";
        String result = executeAgent(agentId, agentInput);
        Assert.assertEquals("12", result);
    }

    public void test_execute_with_parsing_input() throws URISyntaxException, IOException {
        String script =
            "return 'An example output: with ppl:<ppl>' + params.get('PPL.output.ppl') + '</ppl>, and this is ppl result: <ppl_result>' + params.get('PPL.output.executionResult') + '</ppl_result>'";
        String mockPPLOutput = "return '{\\\\\"executionResult\\\\\":\\\\\"result\\\\\",\\\\\"ppl\\\\\":\\\\\"source=demo| head 1\\\\\"}'";
        String registerAgentRequestBody2 = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource("org/opensearch/agent/tools/register_painless_agent_with_multiple_tools.json")
                            .toURI()
                    )
            );
        String agentRequestBody = registerAgentRequestBody2.replaceAll("<SCRIPT1>", mockPPLOutput).replaceAll("<SCRIPT2>", script);

        log.info("agentRequestBody = {}", agentRequestBody);
        String agentId = createAgent(agentRequestBody);
        String agentInput = "{\"parameters\":{}}";
        String result = executeAgent(agentId, agentInput);
        Assert
            .assertEquals(
                "An example output: with ppl:<ppl>source=demo| head 1</ppl>, and this is ppl result: <ppl_result>result</ppl_result>",
                result
            );
    }
}
