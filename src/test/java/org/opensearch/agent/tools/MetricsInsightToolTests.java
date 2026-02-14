/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.Percentiles;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.transport.client.Client;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import lombok.SneakyThrows;

public class MetricsInsightToolTests {

    private Map<String, Object> params = new HashMap<>();
    private final Client client = mock(Client.class);

    @SneakyThrows
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        MetricsInsightTool.Factory.getInstance().init(client);
    }

    // --- Factory & Metadata Tests ---

    @Test
    @SneakyThrows
    public void testCreateTool() {
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);
        assertEquals("MetricsInsightTool", tool.getType());
        assertEquals("MetricsInsightTool", tool.getName());
        assertEquals(MetricsInsightTool.Factory.getInstance().getDefaultDescription(), tool.getDescription());
        assertNull(MetricsInsightTool.Factory.getInstance().getDefaultVersion());
    }

    // --- Validation Tests ---

    @Test
    public void testValidate() {
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        // Valid parameters
        assertTrue(tool.validate(Map.of(
            "index", "test_index",
            "metricFields", "[\"cpu_usage\"]",
            "selectionTimeRangeStart", "2025-01-15 10:00:00",
            "selectionTimeRangeEnd", "2025-01-15 11:00:00"
        )));

        // Missing index
        assertFalse(tool.validate(Map.of(
            "metricFields", "[\"cpu_usage\"]",
            "selectionTimeRangeStart", "2025-01-15 10:00:00",
            "selectionTimeRangeEnd", "2025-01-15 11:00:00"
        )));

        // Missing metricFields
        assertFalse(tool.validate(Map.of(
            "index", "test_index",
            "selectionTimeRangeStart", "2025-01-15 10:00:00",
            "selectionTimeRangeEnd", "2025-01-15 11:00:00"
        )));

        // Empty params
        assertFalse(tool.validate(Map.of()));

        // Invalid aggregation type
        assertFalse(tool.validate(Map.of(
            "index", "test_index",
            "metricFields", "[\"cpu_usage\"]",
            "selectionTimeRangeStart", "2025-01-15 10:00:00",
            "selectionTimeRangeEnd", "2025-01-15 11:00:00",
            "aggregationType", "invalid"
        )));

        // Invalid metricFields JSON
        assertFalse(tool.validate(Map.of(
            "index", "test_index",
            "metricFields", "not-json",
            "selectionTimeRangeStart", "2025-01-15 10:00:00",
            "selectionTimeRangeEnd", "2025-01-15 11:00:00"
        )));
    }

    // --- Single Period Analysis Tests ---

    @Test
    @SneakyThrows
    public void testSinglePeriodAnalysis() {
        mockSearchWithAggregation(createNormalBuckets(60, 45.0, 10.0));
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 11:00:00"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                assertTrue(result.has("metrics"));
                assertTrue(result.has("summary"));

                JsonArray metrics = result.getAsJsonArray("metrics");
                assertTrue(metrics.size() > 0);

                JsonObject firstMetric = metrics.get(0).getAsJsonObject();
                assertEquals("cpu_usage", firstMetric.get("field").getAsString());
                assertEquals("avg", firstMetric.get("aggregationType").getAsString());
                assertTrue(firstMetric.has("statistics"));
                assertTrue(firstMetric.has("anomalies"));
                assertTrue(firstMetric.has("trend"));
                assertEquals(60, firstMetric.get("totalBuckets").getAsInt());

                JsonObject summary = result.getAsJsonObject("summary");
                assertEquals(1, summary.get("totalMetrics").getAsInt());
                assertEquals("single", summary.get("analysisMode").getAsString());
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    @Test
    @SneakyThrows
    public void testSinglePeriodWithZScoreAnomalies() {
        // Create data with a spike at index 30
        List<Histogram.Bucket> buckets = createNormalBuckets(60, 50.0, 5.0);
        // Override bucket 30 with a very high value (spike)
        Histogram.Bucket spikeBucket = createMockBucket(
            ZonedDateTime.of(2025, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC),
            "cpu_usage", "avg", 150.0
        );
        buckets.set(30, spikeBucket);

        mockSearchWithAggregation(buckets);
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 11:00:00"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                JsonArray metrics = result.getAsJsonArray("metrics");
                JsonObject metric = metrics.get(0).getAsJsonObject();

                int anomalyCount = metric.get("anomalyCount").getAsInt();
                assertTrue("Should detect at least one anomaly for spike", anomalyCount >= 1);

                JsonArray anomalies = metric.getAsJsonArray("anomalies");
                assertTrue(anomalies.size() >= 1);

                // Verify anomaly structure
                JsonObject anomaly = anomalies.get(0).getAsJsonObject();
                assertTrue(anomaly.has("timestamp"));
                assertTrue(anomaly.has("value"));
                assertTrue(anomaly.has("expectedValue"));
                assertTrue(anomaly.has("anomalyScore"));
                assertTrue(anomaly.has("type"));
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    @Test
    @SneakyThrows
    public void testSinglePeriodWithMovingAverageAnomalies() {
        // Create data with a sudden shift at index 30
        List<Histogram.Bucket> buckets = new ArrayList<>();
        ZonedDateTime baseTime = ZonedDateTime.of(2025, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC);

        for (int i = 0; i < 60; i++) {
            double value = (i < 30) ? 20.0 : 80.0; // sudden jump from 20 to 80
            buckets.add(createMockBucket(baseTime.plusMinutes(i), "cpu_usage", "avg", value));
        }

        mockSearchWithAggregation(buckets);
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 11:00:00"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                JsonArray metrics = result.getAsJsonArray("metrics");
                JsonObject metric = metrics.get(0).getAsJsonObject();

                int anomalyCount = metric.get("anomalyCount").getAsInt();
                assertTrue("Should detect anomalies for sudden change", anomalyCount >= 1);
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    // --- Baseline Comparison Tests ---

    @Test
    @SneakyThrows
    public void testBaselineComparison() {
        // First call: selection data (high values), second call: baseline data (normal values)
        List<Histogram.Bucket> selectionBuckets = createNormalBuckets(60, 90.0, 5.0);
        List<Histogram.Bucket> baselineBuckets = createNormalBuckets(60, 40.0, 5.0);

        // Mock sequential search calls
        SearchResponse selectionResponse = createSearchResponseWithAggregation(selectionBuckets);
        SearchResponse baselineResponse = createSearchResponseWithAggregation(baselineBuckets);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(selectionResponse);
            return null;
        }).doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(baselineResponse);
            return null;
        }).when(client).search(any(), any());

        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 11:00:00",
                "baselineTimeRangeStart", "2025-01-15 08:00:00",
                "baselineTimeRangeEnd", "2025-01-15 09:00:00"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                assertTrue(result.has("metrics"));
                assertTrue(result.has("summary"));

                JsonObject summary = result.getAsJsonObject("summary");
                assertEquals("baseline", summary.get("analysisMode").getAsString());

                JsonArray metrics = result.getAsJsonArray("metrics");
                JsonObject metric = metrics.get(0).getAsJsonObject();
                // Selection mean ~90 vs baseline mean ~40: >50% change
                assertTrue("Should detect baseline anomalies", metric.get("anomalyCount").getAsInt() > 0);
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    // --- Auto-Detect Interval Tests ---

    @Test
    public void testAutoDetectInterval() {
        // < 1h -> 1m
        assertEquals("1m", MetricsInsightTool.autoDetectInterval("2025-01-15 10:00:00", "2025-01-15 10:30:00"));
        // < 6h -> 5m
        assertEquals("5m", MetricsInsightTool.autoDetectInterval("2025-01-15 10:00:00", "2025-01-15 14:00:00"));
        // < 1d -> 15m
        assertEquals("15m", MetricsInsightTool.autoDetectInterval("2025-01-15 10:00:00", "2025-01-15 20:00:00"));
        // < 7d -> 1h
        assertEquals("1h", MetricsInsightTool.autoDetectInterval("2025-01-15 10:00:00", "2025-01-18 10:00:00"));
        // < 30d -> 6h
        assertEquals("6h", MetricsInsightTool.autoDetectInterval("2025-01-01 00:00:00", "2025-01-20 00:00:00"));
        // >= 30d -> 1d
        assertEquals("1d", MetricsInsightTool.autoDetectInterval("2025-01-01 00:00:00", "2025-03-01 00:00:00"));
    }

    // --- Multiple Metric Fields Tests ---

    @Test
    @SneakyThrows
    public void testMultipleMetricFields() {
        // Create buckets with two metrics
        List<Histogram.Bucket> buckets = createMultiFieldBuckets(30, new String[]{"cpu_usage", "memory"}, new double[]{50.0, 70.0}, new double[]{5.0, 10.0});

        mockSearchWithAggregation(buckets);
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\", \"memory\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 10:30:00"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                JsonArray metrics = result.getAsJsonArray("metrics");
                assertEquals(2, metrics.size());

                JsonObject summary = result.getAsJsonObject("summary");
                assertEquals(2, summary.get("totalMetrics").getAsInt());
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    // --- Filter Tests ---

    @Test
    @SneakyThrows
    public void testWithFilter() {
        mockSearchWithAggregation(createNormalBuckets(30, 45.0, 10.0));
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 10:30:00",
                "filter", "{\"term\": {\"host\": \"server-01\"}}"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                assertTrue(result.has("metrics"));
                assertTrue(result.has("summary"));
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    // --- Empty Result Tests ---

    @Test
    @SneakyThrows
    public void testEmptyResult() {
        mockSearchWithAggregation(List.of());
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 11:00:00"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                JsonArray metrics = result.getAsJsonArray("metrics");
                assertEquals(1, metrics.size());

                JsonObject metric = metrics.get(0).getAsJsonObject();
                assertEquals(0, metric.get("anomalyCount").getAsInt());
                assertEquals(0, metric.get("totalBuckets").getAsInt());
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    // --- Trend Detection Tests ---

    @Test
    public void testTrendDetection() {
        // Increasing trend
        List<double[]> increasing = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            increasing.add(new double[]{i * 60000.0, 10.0 + i * 5.0});
        }
        MetricsInsightTool.TrendInfo incTrend = MetricsInsightTool.detectTrend(increasing);
        assertEquals("increasing", incTrend.direction());
        assertTrue(incTrend.changeRate() > 0);

        // Decreasing trend
        List<double[]> decreasing = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            decreasing.add(new double[]{i * 60000.0, 100.0 - i * 5.0});
        }
        MetricsInsightTool.TrendInfo decTrend = MetricsInsightTool.detectTrend(decreasing);
        assertEquals("decreasing", decTrend.direction());
        assertTrue(decTrend.changeRate() < 0);

        // Stable trend
        List<double[]> stable = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            stable.add(new double[]{i * 60000.0, 50.0});
        }
        MetricsInsightTool.TrendInfo staTrend = MetricsInsightTool.detectTrend(stable);
        assertEquals("stable", staTrend.direction());
    }

    // --- Custom Thresholds Tests ---

    @Test
    @SneakyThrows
    public void testCustomThresholds() {
        // Data with a moderate spike
        List<Histogram.Bucket> buckets = createNormalBuckets(60, 50.0, 5.0);
        Histogram.Bucket spikeBucket = createMockBucket(
            ZonedDateTime.of(2025, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC),
            "cpu_usage", "avg", 70.0
        );
        buckets.set(30, spikeBucket);

        mockSearchWithAggregation(buckets);
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        // With low threshold (should detect more)
        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 11:00:00",
                "zScoreThreshold", "2.0"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                JsonArray metrics = result.getAsJsonArray("metrics");
                JsonObject metric = metrics.get(0).getAsJsonObject();
                assertTrue("Lower threshold should detect more anomalies", metric.get("anomalyCount").getAsInt() >= 1);
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    // --- P99 Aggregation Tests ---

    @Test
    @SneakyThrows
    public void testP99Aggregation() {
        List<Histogram.Bucket> buckets = createNormalBuckets(30, 45.0, 10.0, "p99", "response_time");

        mockSearchWithAggregation(buckets);
        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"response_time\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 10:30:00",
                "aggregationType", "p99"
            ),
            ActionListener.<String>wrap(response -> {
                JsonObject result = gson.fromJson(response, JsonObject.class);
                JsonArray metrics = result.getAsJsonArray("metrics");
                assertEquals(1, metrics.size());
                assertEquals("p99", metrics.get(0).getAsJsonObject().get("aggregationType").getAsString());
            }, e -> fail("Tool execution failed: " + e.getMessage()))
        );
    }

    // --- Search Failure Tests ---

    @Test
    @SneakyThrows
    public void testSearchFailure() {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onFailure(new RuntimeException("Search execution failed"));
            return null;
        }).when(client).search(any(), any());

        MetricsInsightTool tool = MetricsInsightTool.Factory.getInstance().create(params);

        tool.run(
            Map.of(
                "index", "test_index",
                "metricFields", "[\"cpu_usage\"]",
                "selectionTimeRangeStart", "2025-01-15 10:00:00",
                "selectionTimeRangeEnd", "2025-01-15 11:00:00"
            ),
            ActionListener.<String>wrap(
                response -> fail("Should have failed"),
                e -> assertTrue(e.getMessage().contains("Search execution failed"))
            )
        );
    }

    // --- Constant Time Series Tests ---

    @Test
    public void testConstantTimeSeries() {
        // All values are the same -> stddev = 0 -> no z-score anomalies
        List<double[]> series = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            series.add(new double[]{i * 60000.0, 50.0});
        }

        MetricsInsightTool.MetricStatistics stats = MetricsInsightTool.computeStatistics(series);
        assertEquals(50.0, stats.mean(), 0.01);
        assertEquals(0.0, stats.stddev(), 0.01);

        List<MetricsInsightTool.AnomalyRecord> zsAnomalies = MetricsInsightTool.detectZScoreAnomalies(series, 3.0, stats);
        assertTrue("Constant series should produce no z-score anomalies", zsAnomalies.isEmpty());

        List<MetricsInsightTool.AnomalyRecord> maAnomalies = MetricsInsightTool.detectMovingAverageAnomalies(series, 5);
        assertTrue("Constant series should produce no moving average anomalies", maAnomalies.isEmpty());
    }

    // --- Statistics Tests ---

    @Test
    public void testComputeStatistics() {
        List<double[]> series = new ArrayList<>();
        series.add(new double[]{0, 10.0});
        series.add(new double[]{1, 20.0});
        series.add(new double[]{2, 30.0});
        series.add(new double[]{3, 40.0});
        series.add(new double[]{4, 50.0});

        MetricsInsightTool.MetricStatistics stats = MetricsInsightTool.computeStatistics(series);
        assertEquals(30.0, stats.mean(), 0.01);
        assertEquals(10.0, stats.min(), 0.01);
        assertEquals(50.0, stats.max(), 0.01);
        assertTrue(stats.stddev() > 0);
    }

    @Test
    public void testComputeStatisticsEmpty() {
        MetricsInsightTool.MetricStatistics stats = MetricsInsightTool.computeStatistics(List.of());
        assertEquals(0.0, stats.mean(), 0.01);
        assertEquals(0.0, stats.stddev(), 0.01);
    }

    // --- Merge Anomalies Tests ---

    @Test
    public void testMergeAnomalies() {
        List<MetricsInsightTool.AnomalyRecord> zscore = new ArrayList<>();
        zscore.add(new MetricsInsightTool.AnomalyRecord("2025-01-15T10:30:00Z", 95.0, 50.0, 0, 0, 4.0, "zscore"));
        zscore.add(new MetricsInsightTool.AnomalyRecord("2025-01-15T10:31:00Z", 90.0, 50.0, 0, 0, 3.5, "zscore"));

        List<MetricsInsightTool.AnomalyRecord> ma = new ArrayList<>();
        ma.add(new MetricsInsightTool.AnomalyRecord("2025-01-15T10:30:00Z", 95.0, 48.0, 0, 0, 5.0, "moving_average"));
        ma.add(new MetricsInsightTool.AnomalyRecord("2025-01-15T10:32:00Z", 88.0, 50.0, 0, 0, 3.0, "moving_average"));

        List<MetricsInsightTool.AnomalyRecord> merged = MetricsInsightTool.mergeAnomalies(zscore, ma);

        assertEquals(3, merged.size());
        // The overlapping timestamp should keep the higher score (5.0 from MA)
        MetricsInsightTool.AnomalyRecord overlapRecord = merged.stream()
            .filter(a -> a.timestamp().equals("2025-01-15T10:30:00Z"))
            .findFirst()
            .orElse(null);
        assertNotNull(overlapRecord);
        assertEquals(5.0, overlapRecord.anomalyScore(), 0.01);
    }

    // --- Baseline Anomaly Detection Tests ---

    @Test
    public void testBaselineAnomalyDetection() {
        // Baseline: mean=50, stddev=5
        List<double[]> baseline = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            baseline.add(new double[]{i * 60000.0, 50.0 + (i % 2 == 0 ? 3.0 : -3.0)});
        }

        // Selection: significantly higher
        List<double[]> selection = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            selection.add(new double[]{i * 60000.0, 120.0});
        }

        List<MetricsInsightTool.AnomalyRecord> anomalies = MetricsInsightTool.detectBaselineAnomalies(
            selection, baseline, 3.0, 50.0
        );

        assertTrue("All selection points should be anomalous vs baseline", anomalies.size() > 0);
        for (MetricsInsightTool.AnomalyRecord a : anomalies) {
            assertEquals("baseline", a.type());
            assertTrue(a.percentageChange() > 50.0);
        }
    }

    @Test
    public void testBaselineAnomalyDetectionWithEmptyBaseline() {
        List<double[]> selection = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            selection.add(new double[]{i * 60000.0, 50.0});
        }

        List<MetricsInsightTool.AnomalyRecord> anomalies = MetricsInsightTool.detectBaselineAnomalies(
            selection, List.of(), 3.0, 50.0
        );
        assertTrue("Empty baseline should produce no anomalies", anomalies.isEmpty());
    }

    // --- Helper Methods ---

    private void mockSearchWithAggregation(List<Histogram.Bucket> buckets) {
        SearchResponse response = createSearchResponseWithAggregation(buckets);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(response);
            return null;
        }).when(client).search(any(), any());
    }

    private SearchResponse createSearchResponseWithAggregation(List<Histogram.Bucket> buckets) {
        Histogram histogram = mock(Histogram.class);
        when(histogram.getBuckets()).thenReturn((List) buckets);
        when(histogram.getName()).thenReturn("time_buckets");

        Aggregations aggregations = new Aggregations(List.of(histogram));

        SearchHits searchHits = SearchHits.empty();
        SearchResponseSections sections = new SearchResponseSections(searchHits, aggregations, null, false, null, null, 0);
        return new SearchResponse(sections, null, 0, 0, 0, 0, null, null);
    }

    private List<Histogram.Bucket> createNormalBuckets(int count, double mean, double noise) {
        return createNormalBuckets(count, mean, noise, "avg");
    }

    private List<Histogram.Bucket> createNormalBuckets(int count, double mean, double noise, String aggType) {
        List<Histogram.Bucket> buckets = new ArrayList<>();
        ZonedDateTime baseTime = ZonedDateTime.of(2025, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC);

        for (int i = 0; i < count; i++) {
            double value = mean + (noise * Math.sin(i * 0.5));
            buckets.add(createMockBucket(baseTime.plusMinutes(i), "cpu_usage", aggType, value));
        }
        return buckets;
    }

    private List<Histogram.Bucket> createNormalBuckets(int count, double mean, double noise, String aggType, String fieldName) {
        List<Histogram.Bucket> buckets = new ArrayList<>();
        ZonedDateTime baseTime = ZonedDateTime.of(2025, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC);

        for (int i = 0; i < count; i++) {
            double value = mean + (noise * Math.sin(i * 0.5));
            buckets.add(createMockBucket(baseTime.plusMinutes(i), fieldName, aggType, value));
        }
        return buckets;
    }

    private List<Histogram.Bucket> createMultiFieldBuckets(int count, String[] fields, double[] means, double[] noises) {
        List<Histogram.Bucket> buckets = new ArrayList<>();
        ZonedDateTime baseTime = ZonedDateTime.of(2025, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC);

        for (int i = 0; i < count; i++) {
            Histogram.Bucket bucket = mock(Histogram.Bucket.class);
            ZonedDateTime time = baseTime.plusMinutes(i);
            when(bucket.getKey()).thenReturn(time);

            List<org.opensearch.search.aggregations.Aggregation> subAggs = new ArrayList<>();
            for (int f = 0; f < fields.length; f++) {
                double value = means[f] + (noises[f] * Math.sin(i * 0.5));
                Avg avg = mock(Avg.class);
                when(avg.getValue()).thenReturn(value);
                when(avg.getName()).thenReturn("metric_" + fields[f]);
                subAggs.add(avg);
            }

            Aggregations bucketAggs = new Aggregations(subAggs);
            when(bucket.getAggregations()).thenReturn(bucketAggs);

            buckets.add(bucket);
        }
        return buckets;
    }

    private Histogram.Bucket createMockBucket(ZonedDateTime time, String fieldName, String aggType, double value) {
        Histogram.Bucket bucket = mock(Histogram.Bucket.class);
        when(bucket.getKey()).thenReturn(time);

        String aggName = "metric_" + fieldName;
        org.opensearch.search.aggregations.Aggregation metricAgg;

        switch (aggType) {
            case "sum" -> {
                Sum sum = mock(Sum.class);
                when(sum.getValue()).thenReturn(value);
                when(sum.getName()).thenReturn(aggName);
                metricAgg = sum;
            }
            case "max" -> {
                Max max = mock(Max.class);
                when(max.getValue()).thenReturn(value);
                when(max.getName()).thenReturn(aggName);
                metricAgg = max;
            }
            case "min" -> {
                Min min = mock(Min.class);
                when(min.getValue()).thenReturn(value);
                when(min.getName()).thenReturn(aggName);
                metricAgg = min;
            }
            case "p99" -> {
                Percentiles percentiles = mock(Percentiles.class);
                when(percentiles.percentile(99.0)).thenReturn(value);
                when(percentiles.getName()).thenReturn(aggName);
                metricAgg = percentiles;
            }
            default -> {
                Avg avg = mock(Avg.class);
                when(avg.getValue()).thenReturn(value);
                when(avg.getName()).thenReturn(aggName);
                metricAgg = avg;
            }
        }

        Aggregations bucketAggs = new Aggregations(List.of(metricAgg));
        when(bucket.getAggregations()).thenReturn(bucketAggs);

        return bucket;
    }
}
