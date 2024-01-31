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
import org.junit.jupiter.api.BeforeEach;
import org.opensearch.agent.tools.utils.ToolConstants;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import lombok.SneakyThrows;

public class SearchAnomalyDetectorsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private String detectorsIndexMappings;
    private String sampleDetector;
    private String sampleIndexMappings;
    private static final String detectorName = "foo-name";
    private static final String registerAgentFilepath =
        "org/opensearch/agent/tools/anomaly-detection/register_flow_agent_of_search_anomaly_detectors_tool_request_body.json";
    private static final String detectorsIndexMappingsFilepath =
        "org/opensearch/agent/tools/anomaly-detection/detectors_index_mappings.json";
    private static final String sampleDetectorFilepath = "org/opensearch/agent/tools/anomaly-detection/sample_detector.json";
    private static final String sampleIndexMappingsFilepath = "org/opensearch/agent/tools/anomaly-detection/sample_index_mappings.json";

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        registerAgentRequestBody = Files.readString(Path.of(this.getClass().getClassLoader().getResource(registerAgentFilepath).toURI()));
        detectorsIndexMappings = Files
            .readString(Path.of(this.getClass().getClassLoader().getResource(detectorsIndexMappingsFilepath).toURI()));
        sampleDetector = Files.readString(Path.of(this.getClass().getClassLoader().getResource(sampleDetectorFilepath).toURI()));
        sampleIndexMappings = Files.readString(Path.of(this.getClass().getClassLoader().getResource(sampleIndexMappingsFilepath).toURI()));
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
    public void testSearchAnomalyDetectorsToolInFlowAgent_withNoSystemIndex() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);
    }

    @SneakyThrows
    public void testSearchAnomalyDetectorsToolInFlowAgent_noMatching() {
        setupADSystemIndices();
        setupTestDetectionIndex("test-index");
        ingestSampleDetector(detectorName, "test-index");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "foo" + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);
    }

    @SneakyThrows
    public void testSearchAnomalyDetectorsToolInFlowAgent_matching() {
        setupADSystemIndices();
        setupTestDetectionIndex("test-index");
        String detectorId = ingestSampleDetector(detectorName, "test-index");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("id=%s", detectorId)));
        assertTrue(result.contains(String.format("name=%s", detectorName)));
        assertTrue(result.contains(String.format("TotalAnomalyDetectors=%d", 1)));
    }

    @SneakyThrows
    public void testSearchAnomalyDetectorsToolInFlowAgent_complexParams() {
        setupADSystemIndices();
        setupTestDetectionIndex("test-index");
        String detectorId = ingestSampleDetector(detectorName, "test-index");
        ingestSampleDetector(detectorName + "foo", "test-index");
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorName\": \""
            + detectorName
            + "\", \"highCardinality\": false, \"sortOrder\": \"asc\", \"sortString\": \"name.keyword\", \"size\": 10, \"startIndex\": 0 }}";
        String result = executeAgent(agentId, agentInput);
        assertTrue(result.contains(String.format("id=%s", detectorId)));
        assertTrue(result.contains(String.format("name=%s", detectorName)));
        assertTrue(result.contains(String.format("TotalAnomalyDetectors=%d", 1)));
    }

    @SneakyThrows
    private void setupADSystemIndices() {
        createIndexWithConfiguration(ToolConstants.AD_DETECTORS_INDEX, detectorsIndexMappings);
    }

    @SneakyThrows
    private void setupTestDetectionIndex(String indexName) {
        createIndexWithConfiguration(indexName, sampleIndexMappings);
        addDocToIndex(indexName, "foo-id", List.of("timestamp", "value"), List.of(1234, 1));
    }

    private String ingestSampleDetector(String detectorName, String detectionIndex) {
        JsonObject sampleDetectorJson = new Gson().fromJson(sampleDetector, JsonObject.class);
        JsonArray arr = new JsonArray(1);
        arr.add(detectionIndex);
        sampleDetectorJson.addProperty("name", detectorName);
        sampleDetectorJson.remove("indices");
        sampleDetectorJson.add("indices", arr);
        return indexDetector(sampleDetectorJson.toString());
    }
}
