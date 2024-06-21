/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.opensearch.agent.tools.utils.ToolConstants;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.SneakyThrows;

@TestMethodOrder(OrderAnnotation.class)
public class SearchAnomalyResultsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private String resultsIndexMappings;
    private String sampleResult;
    private static final String detectorId = "foo-id";
    private static final double anomalyGrade = 0.5;
    private static final double confidence = 0.6;
    private static final String resultsSystemIndexName = ".opendistro-anomaly-results-1";
    private static final String registerAgentFilepath =
        "org/opensearch/agent/tools/anomaly-detection/register_flow_agent_of_search_anomaly_results_tool_request_body.json";
    private static final String resultsIndexMappingsFilepath = "org/opensearch/agent/tools/anomaly-detection/results_index_mappings.json";
    private static final String sampleResultFilepath = "org/opensearch/agent/tools/anomaly-detection/sample_result.json";

    @Before
    @SneakyThrows
    public void setUp() {
        deleteExternalIndices();
        deleteSystemIndices();
        super.setUp();
        registerAgentRequestBody = Files.readString(Path.of(this.getClass().getClassLoader().getResource(registerAgentFilepath).toURI()));
        resultsIndexMappings = Files
            .readString(Path.of(this.getClass().getClassLoader().getResource(resultsIndexMappingsFilepath).toURI()));
        sampleResult = Files.readString(Path.of(this.getClass().getClassLoader().getResource(sampleResultFilepath).toURI()));
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
    @Order(1)
    public void testSearchAnomalyResultsToolInFlowAgent_withNoSystemIndex() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorId\": \"" + detectorId + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyResults=[]TotalAnomalyResults=0", result);
    }

    @SneakyThrows
    @Order(2)
    public void testSearchAnomalyResultsToolInFlowAgent_noMatching() {
        setupADSystemIndices();
        ingestSampleResult(detectorId, 0.5, 0.5, "1");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorId\": \"" + detectorId + "foo" + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyResults=[]TotalAnomalyResults=0", result);
    }

    @SneakyThrows
    @Order(3)
    public void testSearchAnomalyResultsToolInFlowAgent_matching() {
        setupADSystemIndices();
        ingestSampleResult(detectorId, anomalyGrade, confidence, "1");
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
    @Order(4)
    public void testSearchAnomalyResultsToolInFlowAgent_complexParams() {
        setupADSystemIndices();
        ingestSampleResult(detectorId, anomalyGrade, confidence, "1");
        ingestSampleResult(detectorId + "foo", anomalyGrade, confidence, "2");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorId\": \""
            + detectorId
            + "\","
            + "\"realTime\": true, \"anomalyGradeThreshold\": 0, \"sortOrder\": \"asc\","
            + "\"sortString\": \"data_start_time\", \"size\": 10, \"startIndex\": 0 }}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(
            String.format(Locale.ROOT, "total anomaly results is not 1, result: %s", result),
            result.contains(String.format(Locale.ROOT, "TotalAnomalyResults=%d", 1))
        );
    }

    @SneakyThrows
    private void setupADSystemIndices() {
        createIndexWithConfiguration(ToolConstants.AD_RESULTS_INDEX, resultsIndexMappings);
    }

    private void ingestSampleResult(String detectorId, double anomalyGrade, double anomalyConfidence, String docId) {
        JsonObject sampleResultJson = new Gson().fromJson(sampleResult, JsonObject.class);
        sampleResultJson.addProperty("detector_id", detectorId);
        sampleResultJson.addProperty("anomaly_grade", anomalyGrade);
        sampleResultJson.addProperty("confidence", confidence);
        addDocToIndex(ToolConstants.AD_RESULTS_INDEX, docId, sampleResultJson.toString());
    }
}
