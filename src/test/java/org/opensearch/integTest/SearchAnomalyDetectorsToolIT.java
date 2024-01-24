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

public class SearchAnomalyDetectorsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private static final String detectorId = "foo-id";
    private static final String detectorName = "foo-name";

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
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_search_detectors_tool_request_body.json")
                            .toURI()
                    )
            );
        createDetectorsSystemIndex(detectorId, detectorName);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
        deleteSystemIndices();
    }

    @SneakyThrows
    public void testSearchAnomalyDetectorsToolInFlowAgent_withNoSystemIndex() {
        deleteSystemIndices();
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);
    }

    @SneakyThrows
    public void testSearchAnomalyDetectorsToolInFlowAgent_noMatching() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "foo" + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);
    }

    @SneakyThrows
    public void testSearchAnomalyDetectorsToolInFlowAgent_matching() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("id=%s", detectorId)));
        assertTrue(result.contains(String.format("name=%s", detectorName)));
        assertTrue(result.contains(String.format("TotalAnomalyDetectors=%d", 1)));
    }

    @SneakyThrows
    private void createDetectorsSystemIndex(String detectorId, String detectorName) {
        createIndexWithConfiguration(
            ToolConstants.AD_DETECTORS_INDEX,
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
        addDocToIndex(ToolConstants.AD_DETECTORS_INDEX, detectorId, List.of("name"), List.of(detectorName));
    }
}
