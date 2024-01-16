/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

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
        createMonitorsSystemIndex(monitorId, monitorName);
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

    // @SneakyThrows
    // public void testSearchAnomalyDetectorsToolInFlowAgent_noMatching() {
    // String agentId = createAgent(registerAgentRequestBody);
    // String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "foo" + "\"}}";
    // String result = executeAgent(agentId, agentInput);
    // assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);
    // }

    // @SneakyThrows
    // public void testSearchAnomalyDetectorsToolInFlowAgent_matching() {
    // String agentId = createAgent(registerAgentRequestBody);
    // String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
    // String result = executeAgent(agentId, agentInput);
    // assertEquals(
    // String.format(Locale.ROOT, "AnomalyDetectors=[{id=%s,name=%s}]TotalAnomalyDetectors=%d", detectorId, detectorName, 1),
    // result
    // );
    // }

    @SneakyThrows
    private void createMonitorsSystemIndex(String monitorId, String monitorName) {
        createIndexWithConfiguration(
            ToolConstants.ALERTING_CONFIG_INDEX,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"name\": {\n"
                + "        \"type\": \"text\",\n"
                + "             \"fields\": { \"keyword\": { \"type\": \"keyword\", \"ignore_above\": 256 }}"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(ToolConstants.ALERTING_CONFIG_INDEX, monitorId, List.of("name"), List.of(monitorName));
    }
}
