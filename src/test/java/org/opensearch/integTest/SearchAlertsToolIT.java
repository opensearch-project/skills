/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;

import lombok.SneakyThrows;

public class SearchAlertsToolIT extends BaseAgentToolsIT {
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
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_search_alerts_tool_request_body.json")
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
    public void testSearchAlertsToolInFlowAgent_withNoSystemIndex() {
        deleteSystemIndices();
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorId\": \"" + monitorId + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("Alerts=[]TotalAlerts=0", result);
    }

    // TODO: Add IT to test against sample alerts data
    // https://github.com/opensearch-project/skills/issues/136
}
