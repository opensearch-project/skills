/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.agent.tools.utils.clustering.ClusteringHelper;
import org.opensearch.test.OpenSearchTestCase;

public class ClusteringHelperTests extends OpenSearchTestCase {

    public void testConstructorWithValidThreshold() {
        new ClusteringHelper(0.0);
        new ClusteringHelper(0.5);
        new ClusteringHelper(1.0);
    }

    public void testConstructorWithInvalidThreshold() {
        assertThrows(IllegalArgumentException.class, () -> new ClusteringHelper(-0.1));
        assertThrows(IllegalArgumentException.class, () -> new ClusteringHelper(1.1));
    }

    public void testClusterLogVectorsWithNullInput() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        assertTrue(helper.clusterLogVectorsAndGetRepresentative(null).isEmpty());
    }

    public void testClusterLogVectorsWithEmptyInput() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        assertTrue(helper.clusterLogVectorsAndGetRepresentative(new HashMap<>()).isEmpty());
    }

    public void testClusterLogVectorsWithSingleVector() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", new double[]{1.0, 2.0, 3.0});
        
        List<String> result = helper.clusterLogVectorsAndGetRepresentative(logVectors);
        assertEquals(1, result.size());
        assertEquals("trace1", result.get(0));
    }

    public void testClusterLogVectorsWithSmallDataset() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", new double[]{1.0, 0.0, 0.0});
        logVectors.put("trace2", new double[]{0.9, 0.1, 0.0});
        logVectors.put("trace3", new double[]{0.0, 1.0, 0.0});
        
        List<String> result = helper.clusterLogVectorsAndGetRepresentative(logVectors);
        assertFalse(result.isEmpty());
        assertTrue(result.size() <= 3);
    }

    public void testValidateLogVectorsWithNullTraceId() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put(null, new double[]{1.0, 2.0});
        
        assertThrows(IllegalArgumentException.class, 
            () -> helper.clusterLogVectorsAndGetRepresentative(logVectors));
    }

    public void testValidateLogVectorsWithEmptyTraceId() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("", new double[]{1.0, 2.0});
        
        assertThrows(IllegalArgumentException.class, 
            () -> helper.clusterLogVectorsAndGetRepresentative(logVectors));
    }

    public void testValidateLogVectorsWithNullVector() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", null);
        
        assertThrows(IllegalArgumentException.class, 
            () -> helper.clusterLogVectorsAndGetRepresentative(logVectors));
    }

    public void testValidateLogVectorsWithEmptyVector() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", new double[]{});
        
        assertThrows(IllegalArgumentException.class, 
            () -> helper.clusterLogVectorsAndGetRepresentative(logVectors));
    }

    public void testValidateLogVectorsWithDimensionMismatch() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", new double[]{1.0, 2.0});
        logVectors.put("trace2", new double[]{1.0, 2.0, 3.0});
        
        assertThrows(IllegalArgumentException.class, 
            () -> helper.clusterLogVectorsAndGetRepresentative(logVectors));
    }

    public void testValidateLogVectorsWithNaNValue() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", new double[]{1.0, Double.NaN});
        
        assertThrows(IllegalArgumentException.class, 
            () -> helper.clusterLogVectorsAndGetRepresentative(logVectors));
    }

    public void testValidateLogVectorsWithInfiniteValue() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", new double[]{1.0, Double.POSITIVE_INFINITY});
        
        assertThrows(IllegalArgumentException.class, 
            () -> helper.clusterLogVectorsAndGetRepresentative(logVectors));
    }

    public void testClusterLogVectorsWithLargeDataset() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        
        // Create 1500 vectors to trigger large dataset processing
        for (int i = 0; i < 1500; i++) {
            double[] vector = new double[]{Math.random(), Math.random(), Math.random()};
            logVectors.put("trace" + i, vector);
        }
        
        List<String> result = helper.clusterLogVectorsAndGetRepresentative(logVectors);
        assertFalse(result.isEmpty());
        assertTrue(result.size() < 1500); // Should reduce the number of representatives
    }

    public void testClusterLogVectorsWithIdenticalVectors() {
        ClusteringHelper helper = new ClusteringHelper(0.8);
        Map<String, double[]> logVectors = new HashMap<>();
        double[] vector = {1.0, 2.0, 3.0};
        
        for (int i = 0; i < 5; i++) {
            logVectors.put("trace" + i, vector.clone());
        }
        
        List<String> result = helper.clusterLogVectorsAndGetRepresentative(logVectors);
        assertEquals(1, result.size()); // Should cluster identical vectors into one
    }

    public void testClusterLogVectorsWithHighThreshold() {
        ClusteringHelper helper = new ClusteringHelper(0.99);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", new double[]{1.0, 0.0});
        logVectors.put("trace2", new double[]{0.0, 1.0});
        
        List<String> result = helper.clusterLogVectorsAndGetRepresentative(logVectors);
        assertEquals(2, result.size()); // High threshold should keep vectors separate
    }

    public void testClusterLogVectorsWithLowThreshold() {
        ClusteringHelper helper = new ClusteringHelper(0.1);
        Map<String, double[]> logVectors = new HashMap<>();
        logVectors.put("trace1", new double[]{1.0, 0.1});
        logVectors.put("trace2", new double[]{0.9, 0.2});
        
        List<String> result = helper.clusterLogVectorsAndGetRepresentative(logVectors);
        assertTrue(result.size() <= 2); // Low threshold may cluster similar vectors
    }
}
