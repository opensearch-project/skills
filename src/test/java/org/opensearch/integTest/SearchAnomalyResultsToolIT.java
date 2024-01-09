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

import lombok.SneakyThrows;

public class SearchAnomalyResultsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private static final String detectorId = "foo-id";
    private static final double anomalyGrade = 0.5;
    private static final double confidence = 0.6;
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
        createAnomalyResultsSystemIndex(detectorId, anomalyGrade, confidence);
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

    @SneakyThrows
    public void testSearchAnomalyResultsToolInFlowAgent_noMatching() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorId\": \"" + detectorId + "foo" + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyResults=[]TotalAnomalyResults=0", result);
    }

    @SneakyThrows
    public void testSearchAnomalyResultsToolInFlowAgent_matching() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorId\": \"" + detectorId + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals(
            String
                .format(
                    Locale.ROOT,
                    "AnomalyResults=[{detectorId=%s,grade=%2.1f,confidence=%2.1f}]TotalAnomalyResults=%d",
                    detectorId,
                    anomalyGrade,
                    confidence,
                    1
                ),
            result
        );
    }

    @SneakyThrows
    private void createAnomalyResultsSystemIndex(String detectorId, double anomalyGrade, double confidence) {
        createIndexWithConfiguration(
            resultsSystemIndexName,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"detector_id\": {\"type\": \"keyword\"},"
                + "      \"anomaly_grade\": {\"type\": \"double\"},"
                + "      \"confidence\": {\"type\": \"double\"},"
                + "      \"data_start_time\": {\"type\": \"date\", \"format\": \"strict_date_time||epoch_millis\"}"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(
            resultsSystemIndexName,
            "foo-id",
            List.of("detector_id", "anomaly_grade", "confidence"),
            List.of(detectorId, anomalyGrade, confidence)
        );
    }
}
