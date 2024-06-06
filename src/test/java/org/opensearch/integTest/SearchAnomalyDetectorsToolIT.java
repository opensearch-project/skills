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
        String detectorId = null;
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"detectorName\": \"" + detectorName + "foo" + "\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"detectorName\": \"" + detectorName + "\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format(Locale.ROOT, "id=%s", detectorId)));
            assertTrue(result2.contains(String.format(Locale.ROOT, "name=%s", detectorName)));
            assertTrue(result2.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }
    }

    @SneakyThrows
    @Order(3)
    public void testSearchAnomalyDetectorsToolInFlowAgent_detectorNamePatternParam() {
        String detectorId = null;
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"detectorNamePattern\": \"" + detectorName + "foo" + "\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"detectorNamePattern\": \"" + detectorName + "*" + "\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format(Locale.ROOT, "id=%s", detectorId)));
            assertTrue(result2.contains(String.format(Locale.ROOT, "name=%s", detectorName)));
            assertTrue(result2.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }

    }

    @SneakyThrows
    @Order(4)
    public void testSearchAnomalyDetectorsToolInFlowAgent_indicesParam() {
        String detectorId = null;
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"indices\": \"test-index-foo\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"indices\": \"test-index\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }

    }

    @SneakyThrows
    @Order(5)
    public void testSearchAnomalyDetectorsToolInFlowAgent_highCardinalityParam() {
        String detectorId = null;
        try {
            setupTestDetectionIndex("test-index");
            detectorId = ingestSampleDetector(detectorName, "test-index");
            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"highCardinality\": \"true\"}}";
            String result = executeAgent(agentId, agentInput);
            assertEquals("AnomalyDetectors=[]TotalAnomalyDetectors=0", result);

            String agentInput2 = "{\"parameters\":{\"highCardinality\": \"false\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format(Locale.ROOT, "id=%s", detectorId)));
            assertTrue(result2.contains(String.format(Locale.ROOT, "name=%s", detectorName)));
            assertTrue(result2.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
        } finally {
            if (detectorId != null) {
                deleteDetector(detectorId);
            }
        }

    }

    @SneakyThrows
    @Order(6)
    public void testSearchAnomalyDetectorsToolInFlowAgent_detectorStateParams() {
        String detectorIdRunning = null;
        String detectorIdDisabled1 = null;
        String detectorIdDisabled2 = null;
        try {
            // TODO: update test scenarios
            setupTestDetectionIndex("test-index");
            detectorIdRunning = ingestSampleDetector(detectorName + "-running", "test-index");
            detectorIdDisabled1 = ingestSampleDetector(detectorName + "-disabled-1", "test-index");
            detectorIdDisabled2 = ingestSampleDetector(detectorName + "-disabled-2", "test-index");
            startDetector(detectorIdRunning);
            Thread.sleep(5000);

            String agentId = createAgent(registerAgentRequestBody);
            String agentInput = "{\"parameters\":{\"running\": \"true\"}}";
            String result = executeAgent(agentId, agentInput);
            assertTrue(result.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
            assertTrue(result.contains(detectorIdRunning));

            String agentInput2 = "{\"parameters\":{\"running\": \"false\"}}";
            String result2 = executeAgent(agentId, agentInput2);
            assertTrue(result2.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 2)));
            assertTrue(result2.contains(detectorIdDisabled1));
            assertTrue(result2.contains(detectorIdDisabled2));

            String agentInput3 = "{\"parameters\":{\"failed\": \"true\"}}";
            String result3 = executeAgent(agentId, agentInput3);
            assertTrue(result3.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 0)));

            String agentInput4 = "{\"parameters\":{\"failed\": \"false\"}}";
            String result4 = executeAgent(agentId, agentInput4);
            assertTrue(result4.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 3)));
            assertTrue(result4.contains(detectorIdRunning));
            assertTrue(result4.contains(detectorIdDisabled1));
            assertTrue(result4.contains(detectorIdDisabled2));

            String agentInput5 = "{\"parameters\":{\"running\": \"true\", \"failed\": \"true\"}}";
            String result5 = executeAgent(agentId, agentInput5);
            assertTrue(result5.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
            assertTrue(result5.contains(detectorIdRunning));

            String agentInput6 = "{\"parameters\":{\"running\": \"true\", \"failed\": \"false\"}}";
            String result6 = executeAgent(agentId, agentInput6);
            assertTrue(result6.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
            assertTrue(result6.contains(detectorIdRunning));

            String agentInput7 = "{\"parameters\":{\"running\": \"false\", \"failed\": \"true\"}}";
            String result7 = executeAgent(agentId, agentInput7);
            assertTrue(result7.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 2)));
            assertTrue(result7.contains(detectorIdDisabled1));
            assertTrue(result7.contains(detectorIdDisabled2));

            String agentInput8 = "{\"parameters\":{\"running\": \"false\", \"failed\": \"false\"}}";
            String result8 = executeAgent(agentId, agentInput8);
            assertTrue(result8.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 2)));
            assertTrue(result8.contains(detectorIdDisabled1));
            assertTrue(result8.contains(detectorIdDisabled2));
        } finally {
            if (detectorIdRunning != null) {
                stopDetector(detectorIdRunning);
                Thread.sleep(5000);
                deleteDetector(detectorIdRunning);
            }
            if (detectorIdDisabled1 != null) {
                deleteDetector(detectorIdDisabled1);
            }
            if (detectorIdDisabled2 != null) {
                deleteDetector(detectorIdDisabled2);
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
            assertTrue(result.contains(String.format(Locale.ROOT, "id=%s", detectorId)));
            assertTrue(result.contains(String.format(Locale.ROOT, "name=%s", detectorName)));
            assertTrue(result.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
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
