/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.SneakyThrows;

public class SearchAlertsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private String alertsIndexMappings;
    private String alertingConfigIndexMappings;
    private String sampleAlert;
    private static final String monitorId = "foo-id";
    private static final String monitorName = "foo-name";
    private static final String registerAgentFilepath =
        "org/opensearch/agent/tools/alerting/register_flow_agent_of_search_alerts_tool_request_body.json";
    private static final String alertsIndexMappingsFilepath = "org/opensearch/agent/tools/alerting/alert_index_mappings.json";
    private static final String alertingConfigIndexMappingsFilepath =
        "org/opensearch/agent/tools/alerting/alerting_config_index_mappings.json";
    private static final String sampleAlertFilepath = "org/opensearch/agent/tools/alerting/sample_alert.json";

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        registerAgentRequestBody = Files.readString(Path.of(this.getClass().getClassLoader().getResource(registerAgentFilepath).toURI()));
        alertsIndexMappings = Files.readString(Path.of(this.getClass().getClassLoader().getResource(alertsIndexMappingsFilepath).toURI()));
        alertingConfigIndexMappings = Files
            .readString(Path.of(this.getClass().getClassLoader().getResource(alertingConfigIndexMappingsFilepath).toURI()));
        sampleAlert = Files.readString(Path.of(this.getClass().getClassLoader().getResource(sampleAlertFilepath).toURI()));
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
    public void testSearchAlertsToolInFlowAgent_withNoSystemIndex() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("Alerts=[]TotalAlerts=0", result);
    }

    @SneakyThrows
    public void testSearchAlertsToolInFlowAgent_withSystemIndex() {
        setupAlertingSystemIndices();
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("Alerts=[]TotalAlerts=0", result);
    }

    @SneakyThrows
    public void testSearchAlertsToolInFlowAgent_singleAlert_noFilter() {
        setupAlertingSystemIndices();
        ingestSampleAlert(monitorId, "1");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains("TotalAlerts=1"));
    }

    @SneakyThrows
    public void testSearchAlertsToolInFlowAgent_singleAlert_filter_match() {
        setupAlertingSystemIndices();
        ingestSampleAlert(monitorId, "1");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorId\": \"" + monitorId + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains("TotalAlerts=1"));
    }

    @SneakyThrows
    public void testSearchAlertsToolInFlowAgent_singleAlert_filter_noMatch() {
        setupAlertingSystemIndices();
        ingestSampleAlert(monitorId, "1");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorId\": \"" + monitorId + "foo" + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains("TotalAlerts=0"));
    }

    @SneakyThrows
    public void testSearchAlertsToolInFlowAgent_multipleAlerts_noFilter() {
        setupAlertingSystemIndices();
        ingestSampleAlert(monitorId, "1");
        ingestSampleAlert(monitorId + "foo", "2");
        ingestSampleAlert(monitorId + "bar", "3");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains("TotalAlerts=3"));
    }

    @SneakyThrows
    public void testSearchAlertsToolInFlowAgent_multipleAlerts_filter() {
        setupAlertingSystemIndices();
        ingestSampleAlert(monitorId, "1");
        ingestSampleAlert(monitorId + "foo", "2");
        ingestSampleAlert(monitorId + "bar", "3");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"monitorId\": \"" + monitorId + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains("TotalAlerts=1"));
    }

    @SneakyThrows
    public void testSearchAlertsToolInFlowAgent_multipleAlerts_complex() {
        setupAlertingSystemIndices();
        ingestSampleAlert(monitorId, "1");
        ingestSampleAlert(monitorId + "foo", "2");
        ingestSampleAlert(monitorId + "bar", "3");
        String agentId = createAgent(registerAgentRequestBody);
        // TODO: make more complex and add more params to check here









        
        String agentInput = "{\"parameters\":{\"monitorId\": \"" + monitorId + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains("TotalAlerts=1"));
    }

    @SneakyThrows
    private void setupAlertingSystemIndices() {
        createIndexWithConfiguration(".opendistro-alerting-alerts", alertsIndexMappings);
        createIndexWithConfiguration(".opendistro-alerting-config", alertingConfigIndexMappings);
    }

    private void ingestSampleAlert(String monitorId, String docId) {
        JsonObject sampleAlertJson = new Gson().fromJson(sampleAlert, JsonObject.class);
        sampleAlertJson.addProperty("monitor_id", monitorId);
        addDocToIndex(".opendistro-alerting-alerts", docId, sampleAlertJson.toString());
    }

}
