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
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import lombok.SneakyThrows;

@TestMethodOrder(OrderAnnotation.class)
public class SearchAnomalyDetectorsToolIT extends BaseAgentToolsIT {
    private String registerAgentRequestBody;
    private String sampleDetector;
    private String sampleIndexMappings;
    private static final String detectorName = "foo-name";
    private static final String registerAgentFilepath =
        "org/opensearch/agent/tools/anomaly-detection/register_flow_agent_of_search_anomaly_detectors_tool_request_body.json";
    private static final String sampleDetectorFilepath = "org/opensearch/agent/tools/anomaly-detection/sample_detector.json";
    private static final String sampleIndexMappingsFilepath = "org/opensearch/agent/tools/anomaly-detection/sample_index_mappings.json";

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        registerAgentRequestBody = Files.readString(Path.of(this.getClass().getClassLoader().getResource(registerAgentFilepath).toURI()));
        sampleDetector = Files.readString(Path.of(this.getClass().getClassLoader().getResource(sampleDetectorFilepath).toURI()));
        sampleIndexMappings = Files.readString(Path.of(this.getClass().getClassLoader().getResource(sampleIndexMappingsFilepath).toURI()));
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
    }

    @SneakyThrows
    @Order(1)
    public void testSearchAnomalyDetectorsToolInFlowAgent_withNoSystemIndex() {
        String agentId = createAgent(registerAgentRequestBody);
        String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
        String result = executeAgent(agentId, agentInput);
        assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);
    }

    @SneakyThrows
    @Order(2)
    public void testSearchAnomalyDetectorsToolInFlowAgent_detectorNameParam() {
        String detectorId = "";
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "foo" + "\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format("id=%s", detectorId)));
            assertTrue(result2.contains(String.format("name=%s", detectorName)));
            assertTrue(result2.contains(String.format("TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }
    }

    @SneakyThrows
    @Order(3)
    public void testSearchAnomalyDetectorsToolInFlowAgent_detectorNamePatternParam() {
        String detectorId = "";
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"detectorNamePattern\": \"" + detectorName + "foo" + "\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"detectorNamePattern\": \"" + detectorName + "*" + "\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format("id=%s", detectorId)));
            assertTrue(result2.contains(String.format("name=%s", detectorName)));
            assertTrue(result2.contains(String.format("TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }

    }

    @SneakyThrows
    @Order(4)
    public void testSearchAnomalyDetectorsToolInFlowAgent_indicesParam() {
        String detectorId = "";
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"indices\": \"test-index-foo\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"indices\": \"test-index\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format("TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }

    }

    @SneakyThrows
    @Order(5)
    public void testSearchAnomalyDetectorsToolInFlowAgent_highCardinalityParam() {
        String detectorId = "";
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"highCardinality\": \"true\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"highCardinality\": \"false\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format("id=%s", detectorId)));
            assertTrue(result2.contains(String.format("name=%s", detectorName)));
            assertTrue(result2.contains(String.format("TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }

    }

    @SneakyThrows
    @Order(6)
    public void testSearchAnomalyDetectorsToolInFlowAgent_runningParam() {
        String detectorId = "";
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"running\": \"true\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"running\": \"false\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format("id=%s", detectorId)));
            assertTrue(result2.contains(String.format("name=%s", detectorName)));
            assertTrue(result2.contains(String.format("TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }

    }

    @SneakyThrows
    @Order(7)
    public void testSearchAnomalyDetectorsToolInFlowAgent_complexParams() {
        String detectorId = null;
        String detectorIdFoo = null;
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            detectorIdFoo = ingestSampleDetector(detectorName + "foo", "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"detectorName\": \""
                + detectorName
                + "\", \"highCardinality\": false, \"sortOrder\": \"asc\", \"sortString\": \"name.keyword\", \"size\": 10, \"startIndex\": 0 }}";
            String result = executeAgent(agentId, agentInput);
            assertTrue(result.contains(String.format("id=%s", detectorId)));
            assertTrue(result.contains(String.format("name=%s", detectorName)));
            assertTrue(result.contains(String.format("TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
            if (detectorIdFoo != null) {
                deleteDetector(detectorIdFoo);
            }
        }
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
