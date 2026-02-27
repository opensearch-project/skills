/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;

public class MetricChangeAnalysisToolTests {

    private Map<String, Object> params = new HashMap<>();
    private final Client client = mock(Client.class);
    @Mock
    private AdminClient adminClient;
    @Mock
    private IndicesAdminClient indicesAdminClient;
    @Mock
    private GetMappingsResponse getMappingsResponse;
    @Mock
    private MappingMetadata mappingMetadata;
    @Mock
    private SearchResponse searchResponse;

    @SneakyThrows
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        setupMockMappings();
        MetricChangeAnalysisTool.Factory.getInstance().init(client);
    }

    private void setupMockMappings() {
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        Map<String, Object> properties = ImmutableMap
            .<String, Object>builder()
            .put("response_time", ImmutableMap.of("type", "long"))
            .put("cpu_usage", ImmutableMap.of("type", "double"))
            .put("memory_usage", ImmutableMap.of("type", "float"))
            .put("status", ImmutableMap.of("type", "keyword"))
            .put("@timestamp", ImmutableMap.of("type", "date"))
            .build();

        Map<String, Object> mappingSource = ImmutableMap.of("properties", properties);
        when(mappingMetadata.getSourceAsMap()).thenReturn(mappingSource);
        when(getMappingsResponse.getMappings()).thenReturn(ImmutableMap.of("test-index", mappingMetadata));

        doAnswer(invocation -> {
            ActionListener<GetMappingsResponse> listener = (ActionListener<GetMappingsResponse>) invocation.getArguments()[1];
            listener.onResponse(getMappingsResponse);
            return null;
        }).when(indicesAdminClient).getMappings(any(), any());
    }

    private SearchHit[] createSampleHits(Map<String, Object>... sources) {
        SearchHit[] hits = new SearchHit[sources.length];
        for (int i = 0; i < sources.length; i++) {
            hits[i] = new SearchHit(i);
            hits[i].sourceRef(null);
            hits[i].score(1.0f);
            // Use reflection to set source
            try {
                java.lang.reflect.Field sourceField = SearchHit.class.getDeclaredField("source");
                sourceField.setAccessible(true);
                sourceField.set(hits[i], sources[i]);
            } catch (Exception e) {
                // Fallback: create hit with source
            }
        }
        return hits;
    }

    private void mockSearchResponse(SearchHit[] hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        when(searchResponse.getHits()).thenReturn(searchHits);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
    }

    // ========== Percentile Calculation Tests ==========

    @Test
    @SneakyThrows
    public void testCalculatePercentile() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculatePercentileMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("calculatePercentile", List.class, int.class);
        calculatePercentileMethod.setAccessible(true);

        // Test with sorted values
        List<Double> values = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);

        double p25 = (double) calculatePercentileMethod.invoke(tool, values, 25);
        double p50 = (double) calculatePercentileMethod.invoke(tool, values, 50);
        double p75 = (double) calculatePercentileMethod.invoke(tool, values, 75);
        double p90 = (double) calculatePercentileMethod.invoke(tool, values, 90);

        assertEquals(3.25, p25, 0.01);
        assertEquals(5.5, p50, 0.01);
        assertEquals(7.75, p75, 0.01);
        assertEquals(9.1, p90, 0.01);
    }

    @Test
    @SneakyThrows
    public void testCalculatePercentileWithSingleValue() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculatePercentileMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("calculatePercentile", List.class, int.class);
        calculatePercentileMethod.setAccessible(true);

        List<Double> values = List.of(5.0);

        double p25 = (double) calculatePercentileMethod.invoke(tool, values, 25);
        double p50 = (double) calculatePercentileMethod.invoke(tool, values, 50);
        double p75 = (double) calculatePercentileMethod.invoke(tool, values, 75);
        double p90 = (double) calculatePercentileMethod.invoke(tool, values, 90);

        assertEquals(5.0, p25, 0.01);
        assertEquals(5.0, p50, 0.01);
        assertEquals(5.0, p75, 0.01);
        assertEquals(5.0, p90, 0.01);
    }

    @Test
    @SneakyThrows
    public void testCalculatePercentileWithEmptyList() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculatePercentileMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("calculatePercentile", List.class, int.class);
        calculatePercentileMethod.setAccessible(true);

        List<Double> values = List.of();

        double p50 = (double) calculatePercentileMethod.invoke(tool, values, 50);

        assertEquals(0.0, p50, 0.01);
    }

    @Test
    @SneakyThrows
    public void testCalculatePercentiles() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculatePercentilesMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("calculatePercentiles", List.class);
        calculatePercentilesMethod.setAccessible(true);

        List<Double> values = List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);

        Object result = calculatePercentilesMethod.invoke(tool, values);

        assertNotNull(result);
        java.lang.reflect.Method p50Method = result.getClass().getDeclaredMethod("p50");
        java.lang.reflect.Method p90Method = result.getClass().getDeclaredMethod("p90");

        double p50 = (double) p50Method.invoke(result);
        double p90 = (double) p90Method.invoke(result);

        assertEquals(5.5, p50, 0.01);
        assertEquals(9.1, p90, 0.01);
    }

    // ========== Numeric Value Extraction Tests ==========

    @Test
    @SneakyThrows
    public void testExtractNumericValues() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method extractNumericValuesMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("extractNumericValues", List.class, String.class);
        extractNumericValuesMethod.setAccessible(true);

        List<Map<String, Object>> data = List
            .of(
                Map.of("level", 1, "status", "error"),
                Map.of("level", 2, "status", "warning"),
                Map.of("level", 3, "status", "info"),
                Map.of("level", "4", "status", "debug"),  // String number
                Map.of("status", "error")  // Missing level field
            );

        @SuppressWarnings("unchecked")
        List<Double> values = (List<Double>) extractNumericValuesMethod.invoke(tool, data, "level");

        assertNotNull(values);
        assertEquals(4, values.size());
        assertTrue(values.contains(1.0));
        assertTrue(values.contains(2.0));
        assertTrue(values.contains(3.0));
        assertTrue(values.contains(4.0));
    }

    @Test
    @SneakyThrows
    public void testExtractNumericValuesWithNonNumericData() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method extractNumericValuesMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("extractNumericValues", List.class, String.class);
        extractNumericValuesMethod.setAccessible(true);

        List<Map<String, Object>> data = List.of(Map.of("status", "error"), Map.of("status", "warning"), Map.of("status", "info"));

        @SuppressWarnings("unchecked")
        List<Double> values = (List<Double>) extractNumericValuesMethod.invoke(tool, data, "status");

        assertNotNull(values);
        assertTrue(values.isEmpty());
    }

    @Test
    @SneakyThrows
    public void testExtractNumericValuesWithNestedField() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method extractNumericValuesMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("extractNumericValues", List.class, String.class);
        extractNumericValuesMethod.setAccessible(true);

        List<Map<String, Object>> data = List
            .of(
                Map.of("metrics", Map.of("response_time", 100)),
                Map.of("metrics", Map.of("response_time", 200)),
                Map.of("metrics", Map.of("response_time", 300))
            );

        @SuppressWarnings("unchecked")
        List<Double> values = (List<Double>) extractNumericValuesMethod.invoke(tool, data, "metrics.response_time");

        assertNotNull(values);
        assertEquals(3, values.size());
        assertTrue(values.contains(100.0));
        assertTrue(values.contains(200.0));
        assertTrue(values.contains(300.0));
    }

    // ========== Log Ratio Tests ==========

    @Test
    @SneakyThrows
    public void testSafeLogRatio() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method safeLogRatioMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("safeLogRatio", double.class, double.class);
        safeLogRatioMethod.setAccessible(true);

        // Test 2x increase: |log(2/1)| = log(2) ≈ 0.693
        double ratio1 = (double) safeLogRatioMethod.invoke(tool, 2.0, 1.0);
        assertEquals(Math.log(2.0), ratio1, 0.01);

        // Test 1.8x increase: |log(180/100)| = log(1.8) ≈ 0.588
        double ratio2 = (double) safeLogRatioMethod.invoke(tool, 180.0, 100.0);
        assertEquals(Math.log(1.8), ratio2, 0.01);

        // Test decrease: |log(5/10)| = |log(0.5)| = log(2) ≈ 0.693
        double ratio3 = (double) safeLogRatioMethod.invoke(tool, 5.0, 10.0);
        assertEquals(Math.log(2.0), ratio3, 0.01);

        // Test zero baseline: should return cap (10.0)
        double ratio4 = (double) safeLogRatioMethod.invoke(tool, 10.0, 0.0);
        assertEquals(10.0, ratio4, 0.01);

        // Test both near zero: should return 0.0
        double ratio5 = (double) safeLogRatioMethod.invoke(tool, 0.0, 0.0);
        assertEquals(0.0, ratio5, 0.01);

        // Test no change: |log(1)| = 0
        double ratio6 = (double) safeLogRatioMethod.invoke(tool, 100.0, 100.0);
        assertEquals(0.0, ratio6, 0.01);
    }

    // ========== Variance Calculation Tests ==========

    @Test
    @SneakyThrows
    public void testCalculatePercentileVarianceSkipsZeroBaseline() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculatePercentileVarianceMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod(
                "calculatePercentileVariance",
                Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats"),
                Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats")
            );
        calculatePercentileVarianceMethod.setAccessible(true);

        Class<?> percentileStatsClass = Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats");
        java.lang.reflect.Constructor<?> constructor = percentileStatsClass.getDeclaredConstructor(double.class, double.class);
        constructor.setAccessible(true);

        // Both baselines zero → score 0
        Object sel1 = constructor.newInstance(10.0, 5.0);
        Object base1 = constructor.newInstance(0.0, 0.0);
        assertEquals(0.0, (double) calculatePercentileVarianceMethod.invoke(tool, sel1, base1), 0.01);

        // Only P50 baseline zero → score based on P90 only
        Object sel2 = constructor.newInstance(10.0, 20.0);
        Object base2 = constructor.newInstance(0.0, 10.0);
        double expected = Math.abs(Math.log(20.0 / 10.0)); // log(2) ≈ 0.693
        assertEquals(expected, (double) calculatePercentileVarianceMethod.invoke(tool, sel2, base2), 0.01);

        // Only P90 baseline zero → score based on P50 only
        Object sel3 = constructor.newInstance(20.0, 5.0);
        Object base3 = constructor.newInstance(10.0, 0.0);
        double expected3 = Math.abs(Math.log(20.0 / 10.0)); // log(2) ≈ 0.693
        assertEquals(expected3, (double) calculatePercentileVarianceMethod.invoke(tool, sel3, base3), 0.01);
    }

    // ========== Variance Calculation Tests ==========

    @Test
    @SneakyThrows
    public void testCalculatePercentileVariance() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculatePercentileVarianceMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod(
                "calculatePercentileVariance",
                Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats"),
                Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats")
            );
        calculatePercentileVarianceMethod.setAccessible(true);

        // Create PercentileStats using reflection (p50, p90)
        Class<?> percentileStatsClass = Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats");
        java.lang.reflect.Constructor<?> constructor = percentileStatsClass.getDeclaredConstructor(double.class, double.class);
        constructor.setAccessible(true);

        // selection p50=20, p90=40; baseline p50=10, p90=20 → both are 2x
        Object selectionStats = constructor.newInstance(20.0, 40.0);
        Object baselineStats = constructor.newInstance(10.0, 20.0);

        double variance = (double) calculatePercentileVarianceMethod.invoke(tool, selectionStats, baselineStats);

        // score = 0.5 * log(2) + 0.5 * log(2) = log(2) ≈ 0.693
        assertEquals(Math.log(2.0), variance, 0.01);
    }

    @Test
    @SneakyThrows
    public void testCalculatePercentileVarianceWithNoChange() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculatePercentileVarianceMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod(
                "calculatePercentileVariance",
                Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats"),
                Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats")
            );
        calculatePercentileVarianceMethod.setAccessible(true);

        Class<?> percentileStatsClass = Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats");
        java.lang.reflect.Constructor<?> constructor = percentileStatsClass.getDeclaredConstructor(double.class, double.class);
        constructor.setAccessible(true);

        Object selectionStats = constructor.newInstance(20.0, 40.0);
        Object baselineStats = constructor.newInstance(20.0, 40.0);

        double variance = (double) calculatePercentileVarianceMethod.invoke(tool, selectionStats, baselineStats);

        assertEquals(0.0, variance, 0.01);
    }

    @Test
    @SneakyThrows
    public void testCalculatePercentileVarianceRelativeChange() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculatePercentileVarianceMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod(
                "calculatePercentileVariance",
                Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats"),
                Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats")
            );
        calculatePercentileVarianceMethod.setAccessible(true);

        Class<?> percentileStatsClass = Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats");
        java.lang.reflect.Constructor<?> constructor = percentileStatsClass.getDeclaredConstructor(double.class, double.class);
        constructor.setAccessible(true);

        // Test that 1->2 (2x) ranks higher than 100->180 (1.8x)
        Object smallChangeStats = constructor.newInstance(2.0, 2.0);
        Object smallBaselineStats = constructor.newInstance(1.0, 1.0);

        Object largeChangeStats = constructor.newInstance(180.0, 180.0);
        Object largeBaselineStats = constructor.newInstance(100.0, 100.0);

        double smallVariance = (double) calculatePercentileVarianceMethod.invoke(tool, smallChangeStats, smallBaselineStats);
        double largeVariance = (double) calculatePercentileVarianceMethod.invoke(tool, largeChangeStats, largeBaselineStats);

        // 2x change: 0.5 * log(2) + 0.5 * log(2) = log(2) ≈ 0.693
        // 1.8x change: 0.5 * log(1.8) + 0.5 * log(1.8) = log(1.8) ≈ 0.588
        assertEquals(Math.log(2.0), smallVariance, 0.01);
        assertEquals(Math.log(1.8), largeVariance, 0.01);
        assertTrue("2x change should rank higher than 1.8x change", smallVariance > largeVariance);
    }

    // ========== Percentile Analysis Tests ==========

    @Test
    @SneakyThrows
    public void testCalculateMetricChangeAnalysis() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculateMetricChangeAnalysisMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("calculateMetricChangeAnalysis", List.class, List.class, java.util.Set.class);
        calculateMetricChangeAnalysisMethod.setAccessible(true);

        // Use non-monotonic (gauge-like) data to avoid counter detection
        List<Map<String, Object>> selectionData = List
            .of(
                Map.of("response_time", 400, "cpu_usage", 80),
                Map.of("response_time", 100, "cpu_usage", 50),
                Map.of("response_time", 300, "cpu_usage", 60),
                Map.of("response_time", 200, "cpu_usage", 70)
            );

        List<Map<String, Object>> baselineData = List
            .of(
                Map.of("response_time", 150, "cpu_usage", 65),
                Map.of("response_time", 50, "cpu_usage", 45),
                Map.of("response_time", 200, "cpu_usage", 75),
                Map.of("response_time", 100, "cpu_usage", 55)
            );

        java.util.Set<String> numberFields = java.util.Set.of("response_time", "cpu_usage");

        @SuppressWarnings("unchecked")
        List<Object> analyses = (List<Object>) calculateMetricChangeAnalysisMethod.invoke(tool, selectionData, baselineData, numberFields);

        assertNotNull(analyses);
        assertEquals(2, analyses.size());

        // Verify first analysis has highest variance
        java.lang.reflect.Method varianceMethod = analyses.get(0).getClass().getDeclaredMethod("variance");
        double firstVariance = (double) varianceMethod.invoke(analyses.get(0));
        double secondVariance = (double) varianceMethod.invoke(analyses.get(1));

        assertTrue(firstVariance >= secondVariance);
    }

    @Test
    @SneakyThrows
    public void testCalculateMetricChangeAnalysisWithEmptyData() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculateMetricChangeAnalysisMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("calculateMetricChangeAnalysis", List.class, List.class, java.util.Set.class);
        calculateMetricChangeAnalysisMethod.setAccessible(true);

        List<Map<String, Object>> selectionData = List.of();
        List<Map<String, Object>> baselineData = List.of();
        java.util.Set<String> numberFields = java.util.Set.of("response_time");

        @SuppressWarnings("unchecked")
        List<Object> analyses = (List<Object>) calculateMetricChangeAnalysisMethod.invoke(tool, selectionData, baselineData, numberFields);

        assertNotNull(analyses);
        assertTrue(analyses.isEmpty());
    }

    @Test
    @SneakyThrows
    public void testCalculateMetricChangeAnalysisWithMissingFields() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculateMetricChangeAnalysisMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("calculateMetricChangeAnalysis", List.class, List.class, java.util.Set.class);
        calculateMetricChangeAnalysisMethod.setAccessible(true);

        List<Map<String, Object>> selectionData = List.of(Map.of("response_time", 100), Map.of("response_time", 200));

        List<Map<String, Object>> baselineData = List.of(Map.of("cpu_usage", 50), Map.of("cpu_usage", 60));

        java.util.Set<String> numberFields = java.util.Set.of("response_time", "cpu_usage");

        @SuppressWarnings("unchecked")
        List<Object> analyses = (List<Object>) calculateMetricChangeAnalysisMethod.invoke(tool, selectionData, baselineData, numberFields);

        assertNotNull(analyses);
        // Should skip fields that don't have data in both datasets
        assertTrue(analyses.isEmpty());
    }

    @Test
    @SneakyThrows
    public void testCalculateMetricChangeAnalysisRanking() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method calculateMetricChangeAnalysisMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("calculateMetricChangeAnalysis", List.class, List.class, java.util.Set.class);
        calculateMetricChangeAnalysisMethod.setAccessible(true);

        // Create non-monotonic data where response_time has high change and cpu_usage has low change
        List<Map<String, Object>> selectionData = List
            .of(
                Map.of("response_time", 3000, "cpu_usage", 52),
                Map.of("response_time", 1000, "cpu_usage", 54),
                Map.of("response_time", 4000, "cpu_usage", 51),
                Map.of("response_time", 2000, "cpu_usage", 53)
            );

        List<Map<String, Object>> baselineData = List
            .of(
                Map.of("response_time", 300, "cpu_usage", 52),
                Map.of("response_time", 100, "cpu_usage", 50),
                Map.of("response_time", 400, "cpu_usage", 53),
                Map.of("response_time", 200, "cpu_usage", 51)
            );

        java.util.Set<String> numberFields = java.util.Set.of("response_time", "cpu_usage");

        @SuppressWarnings("unchecked")
        List<Object> analyses = (List<Object>) calculateMetricChangeAnalysisMethod.invoke(tool, selectionData, baselineData, numberFields);

        assertNotNull(analyses);
        assertEquals(2, analyses.size());

        // First field should be response_time (higher variance)
        java.lang.reflect.Method fieldMethod = analyses.get(0).getClass().getDeclaredMethod("field");
        String firstField = (String) fieldMethod.invoke(analyses.get(0));
        assertEquals("response_time", firstField);

        // Second field should be cpu_usage (lower variance)
        String secondField = (String) fieldMethod.invoke(analyses.get(1));
        assertEquals("cpu_usage", secondField);
    }

    @Test
    @SneakyThrows
    public void testFormatResultsLimitsToTopTen() {
        MetricChangeAnalysisTool tool = MetricChangeAnalysisTool.Factory.getInstance().create(params);

        java.lang.reflect.Method formatResultsMethod = MetricChangeAnalysisTool.class
            .getDeclaredMethod("formatResults", List.class, int.class);
        formatResultsMethod.setAccessible(true);

        // Create 15 fields with different variance scores
        List<Object> analyses = new ArrayList<>();
        Class<?> fieldAnalysisClass = Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$FieldPercentileAnalysis");
        Class<?> percentileStatsClass = Class.forName("org.opensearch.agent.tools.MetricChangeAnalysisTool$PercentileStats");

        java.lang.reflect.Constructor<?> statsConstructor = percentileStatsClass.getDeclaredConstructor(double.class, double.class);
        statsConstructor.setAccessible(true);

        java.lang.reflect.Constructor<?> analysisConstructor = fieldAnalysisClass
            .getDeclaredConstructor(String.class, double.class, percentileStatsClass, percentileStatsClass);
        analysisConstructor.setAccessible(true);

        Object stats = statsConstructor.newInstance(20.0, 40.0);

        // Create 15 fields with descending variance scores
        for (int i = 0; i < 15; i++) {
            double variance = 15.0 - i; // 15.0, 14.0, 13.0, ..., 1.0
            Object analysis = analysisConstructor.newInstance("field_" + i, variance, stats, stats);
            analyses.add(analysis);
        }

        // Test with topN = 10
        @SuppressWarnings("unchecked")
        List<Object> results10 = (List<Object>) formatResultsMethod.invoke(tool, analyses, 10);
        assertNotNull(results10);
        assertEquals("Should return only top 10 results", 10, results10.size());

        // Test with topN = 5 (default)
        @SuppressWarnings("unchecked")
        List<Object> results5 = (List<Object>) formatResultsMethod.invoke(tool, analyses, 5);
        assertNotNull(results5);
        assertEquals("Should return only top 5 results", 5, results5.size());

        // Test with topN = 3
        @SuppressWarnings("unchecked")
        List<Object> results3 = (List<Object>) formatResultsMethod.invoke(tool, analyses, 3);
        assertNotNull(results3);
        assertEquals("Should return only top 3 results", 3, results3.size());
    }
}
