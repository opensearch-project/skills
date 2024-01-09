/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import org.junit.After;
import org.junit.Before;
import org.opensearch.agent.tools.utils.ToolConstants;

import lombok.SneakyThrows;

public class SearchAnomalyResultsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private static final String detectorId = "foo-id";
    private static final String detectorName = "foo-name";
    private static final String resultsSystemIndexName = ".opendistro-anomaly-results-1";

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
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_search_anomaly_results_tool_request_body.json")
                            .toURI()
                    )
            );
        createAnomalyResultsSystemIndex(detectorId, detectorName);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
        deleteSystemIndices();
    }

    @SneakyThrows
    public void testSearchAnomalyResultsToolInFlowAgent_withNoSystemIndex() {
        deleteSystemIndices();
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorId\": \"" + detectorId + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyResults=[]TotalAnomalyResults=0", result);
    }

    // @SneakyThrows
    // public void testSearchAnomalyResultsToolInFlowAgent_noMatching() {
    //     String agentId = createAgent(registerAgentRequestBody);
    //     String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "foo" + "\"}}";
    //     String result = executeAgent(agentId, agentInput);
    //     assertEquals("AnomalyResults=[]TotalAnomalyResults=0", result);
    // }

    // @SneakyThrows
    // public void testSearchAnomalyResultsToolInFlowAgent_matching() {
    //     String agentId = createAgent(registerAgentRequestBody);
    //     String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
    //     String result = executeAgent(agentId, agentInput);
    //     assertEquals(
    //         String.format(Locale.ROOT, "AnomalyResults=[{id=%s,name=%s}]TotalAnomalyResults=%d", detectorId, detectorName, 1),
    //         result
    //     );
    // }

    @SneakyThrows
    private void createAnomalyResultsSystemIndex(String detectorId, String detectorName) {
        createIndexWithConfiguration(
            resultsSystemIndexName,
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
