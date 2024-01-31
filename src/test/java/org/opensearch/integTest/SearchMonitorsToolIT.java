/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SearchMonitorsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private String sampleMonitor;
    private static final String monitorName = "foo-name";
    private static final String monitorName2 = "bar-name";
    private static final String registerAgentFilepath =
        "org/opensearch/agent/tools/alerting/register_flow_agent_of_search_monitors_tool_request_body.json";
    private static final String sampleMonitorFilepath = "org/opensearch/agent/tools/alerting/sample_monitor.json";

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        registerAgentRequestBody = Files.readString(Path.of(this.getClass().getClassLoader().getResource(registerAgentFilepath).toURI()));
        sampleMonitor = Files.readString(Path.of(this.getClass().getClassLoader().getResource(sampleMonitorFilepath).toURI()));
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
        String monitorId = ingestSampleMonitor(monitorName, true);

        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorId\": \"" + monitorId + "\"}}";

        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("name=%s", monitorName)));
        assertTrue(result.contains("TotalMonitors=1"));
    }

    @SneakyThrows
    public void testSearchMonitorsToolInFlowAgent_singleMonitor_noFilter() {
        ingestSampleMonitor(monitorName, true);

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
        ingestSampleMonitor(monitorName, true);
        ingestSampleMonitor(monitorName2, false);

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
        ingestSampleMonitor(monitorName, true);
        ingestSampleMonitor(monitorName2, false);

        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorName\": \"" + monitorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("name=%s", monitorName)));
        assertFalse(result.contains(String.format("name=%s", monitorName2)));
        assertTrue(result.contains("enabled=true"));
        assertTrue(result.contains("TotalMonitors=1"));
    }

    @SneakyThrows
    public void testSearchMonitorsToolInFlowAgent_multipleMonitors_complexParams() {
        ingestSampleMonitor(monitorName, true);
        ingestSampleMonitor(monitorName2, false);

        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorName\": \""
            + monitorName
            + "\", \"enabled\": true, \"hasTriggers\": false, \"sortOrder\": \"asc\", \"sortString\": \"monitor.name.keyword\", \"size\": 10, \"startIndex\": 0 }}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains("TotalMonitors=1"));
    }

    private String ingestSampleMonitor(String monitorName, boolean enabled) {
        JsonObject sampleMonitorJson = new Gson().fromJson(sampleMonitor, JsonObject.class);
        sampleMonitorJson.addProperty("name", monitorName);
        sampleMonitorJson.addProperty("enabled", String.valueOf(enabled));
        return indexMonitor(sampleMonitorJson.toString());
    }
}
