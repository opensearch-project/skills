/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.opensearch.agent.tools.utils.ToolConstants;

import lombok.SneakyThrows;

public class SearchMonitorsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private static final String monitorId = "foo-id";
    private static final String monitorName = "foo-name";

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        registerAgentRequestBody = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_search_monitors_tool_request_body.json")
                            .toURI()
                    )
            );
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
        deleteSystemIndices();
    }

    @SneakyThrows
    public void testSearchMonitorsToolInFlowAgent_withNoSystemIndex() {
        deleteSystemIndices();
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorName\": \"" + monitorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("Monitors=[]TotalMonitors=0", result);
    }

    // TODO: Add IT to test against sample monitor data
}
