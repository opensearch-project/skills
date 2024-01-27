/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SearchMonitorsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private static final String monitorName = "foo-name";
    private static final String monitorName2 = "bar-name";

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

    @BeforeEach
    @SneakyThrows
    public void prepareTest() {
        deleteSystemIndices();
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
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorName\": \"" + monitorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("Monitors=[]TotalMonitors=0", result);
    }

    @SneakyThrows
    public void testSearchMonitorsToolInFlowAgent_searchById() {
        String monitorId = indexMonitor(getMonitorJsonString(monitorName, true));

        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorId\": \"" + monitorId + "\"}}";

        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("name=%s", monitorName)));
        assertTrue(result.contains("TotalMonitors=1"));
    }

    @SneakyThrows
    public void testSearchMonitorsToolInFlowAgent_singleMonitor_noFilter() {
        indexMonitor(getMonitorJsonString(monitorName, true));

        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("name=%s", monitorName)));
        assertTrue(result.contains("TotalMonitors=1"));
    }

    @SneakyThrows
    public void testSearchMonitorsToolInFlowAgent_singleMonitor_filter() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorId\": \"" + "foo-id" + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains("TotalMonitors=0"));
    }

    @SneakyThrows
    public void testSearchMonitorsToolInFlowAgent_multipleMonitors_noFilter() {
        indexMonitor(getMonitorJsonString(monitorName, true));
        indexMonitor(getMonitorJsonString(monitorName2, false));

        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("name=%s", monitorName)));
        assertTrue(result.contains(String.format("name=%s", monitorName2)));
        assertTrue(result.contains("enabled=true"));
        assertTrue(result.contains("enabled=false"));
        assertTrue(result.contains("TotalMonitors=2"));
    }

    @SneakyThrows
    public void testSearchMonitorsToolInFlowAgent_multipleMonitors_filter() {
        indexMonitor(getMonitorJsonString(monitorName, true));
        indexMonitor(getMonitorJsonString(monitorName2, false));

        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorName\": \"" + monitorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("name=%s", monitorName)));
        assertFalse(result.contains(String.format("name=%s", monitorName2)));
        assertTrue(result.contains("enabled=true"));
        assertTrue(result.contains("TotalMonitors=1"));
    }

    // Helper fn to create the JSON string to use in a REST request body when indexing a monitor
    private String getMonitorJsonString(String monitorName, boolean enabled) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("type", "monitor");
        jsonObj.put("name", monitorName);
        jsonObj.put("enabled", String.valueOf(enabled));
        jsonObj.put("inputs", Collections.emptyList());
        jsonObj.put("triggers", Collections.emptyList());
        Map scheduleMap = new HashMap<String, Object>();
        Map periodMap = new HashMap<String, Object>();
        periodMap.put("interval", 5);
        periodMap.put("unit", "MINUTES");
        scheduleMap.put("period", periodMap);
        jsonObj.put("schedule", scheduleMap);
        return jsonObj.toString();
    }
}
