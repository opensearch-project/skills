/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.CommonValue.TOOL_INPUT_SCHEMA_FIELD;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.utils.ToolUtils;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.Percentiles;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import com.google.gson.reflect.TypeToken;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
@ToolAnnotation(MetricsInsightTool.TYPE)
public class MetricsInsightTool implements Tool {
    public static final String TYPE = "MetricsInsightTool";
    public static final String STRICT_FIELD = "strict";

    private static final String DEFAULT_DESCRIPTION =
        "Detects anomalies in metrics data using statistical methods (z-score, moving average) and baseline comparison. "
            + "Analyzes time-series metrics from OpenSearch indices using DSL aggregation queries.";

    static final String DEFAULT_TIME_FIELD = "@timestamp";
    static final double DEFAULT_Z_SCORE_THRESHOLD = 3.0;
    static final double DEFAULT_CHANGE_THRESHOLD = 50.0;
    static final String DEFAULT_AGGREGATION_TYPE = "avg";
    static final int MOVING_AVG_WINDOW = 5;
    static final double MOVING_AVG_DEVIATION_MULTIPLIER = 2.0;
    static final Set<String> SUPPORTED_AGG_TYPES = Set.of("avg", "sum", "max", "min", "p99");

    private static final String PARAM_INDEX = "index";
    private static final String PARAM_METRIC_FIELDS = "metricFields";
    private static final String PARAM_TIME_FIELD = "timeField";
    private static final String PARAM_SELECTION_TIME_RANGE_START = "selectionTimeRangeStart";
    private static final String PARAM_SELECTION_TIME_RANGE_END = "selectionTimeRangeEnd";
    private static final String PARAM_BASELINE_TIME_RANGE_START = "baselineTimeRangeStart";
    private static final String PARAM_BASELINE_TIME_RANGE_END = "baselineTimeRangeEnd";
    private static final String PARAM_INTERVAL = "interval";
    private static final String PARAM_FILTER = "filter";
    private static final String PARAM_AGGREGATION_TYPE = "aggregationType";
    private static final String PARAM_Z_SCORE_THRESHOLD = "zScoreThreshold";
    private static final String PARAM_CHANGE_THRESHOLD = "changeThreshold";

    private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String TIME_BUCKETS_AGG = "time_buckets";

    public static final String DEFAULT_INPUT_SCHEMA = """
        {
            "type": "object",
            "properties": {
                "index": {
                    "type": "string",
                    "description": "Target OpenSearch index name"
                },
                "metricFields": {
                    "type": "string",
                    "description": "JSON array of numeric field names to analyze, e.g. [\\"cpu_usage\\", \\"memory\\"]"
                },
                "timeField": {
                    "type": "string",
                    "description": "Date/time field for filtering (default: @timestamp)"
                },
                "selectionTimeRangeStart": {
                    "type": "string",
                    "description": "Start time for analysis period"
                },
                "selectionTimeRangeEnd": {
                    "type": "string",
                    "description": "End time for analysis period"
                },
                "baselineTimeRangeStart": {
                    "type": "string",
                    "description": "Start time for baseline period (optional, for comparison)"
                },
                "baselineTimeRangeEnd": {
                    "type": "string",
                    "description": "End time for baseline period (optional, for comparison)"
                },
                "interval": {
                    "type": "string",
                    "description": "Date histogram interval (e.g. 1m, 5m, 1h). Auto-detected if not provided."
                },
                "filter": {
                    "type": "string",
                    "description": "Additional DSL query filter as JSON string (optional)"
                },
                "aggregationType": {
                    "type": "string",
                    "description": "Aggregation type: avg, sum, max, min, p99 (default: avg)"
                },
                "zScoreThreshold": {
                    "type": "string",
                    "description": "Z-score threshold for anomaly detection (default: 3.0)"
                },
                "changeThreshold": {
                    "type": "string",
                    "description": "Percentage change threshold for baseline comparison (default: 50.0)"
                }
            },
            "required": ["index", "metricFields", "selectionTimeRangeStart", "selectionTimeRangeEnd"],
            "additionalProperties": false
        }
        """;

    public static final Map<String, Object> DEFAULT_ATTRIBUTES = Map.of(TOOL_INPUT_SCHEMA_FIELD, DEFAULT_INPUT_SCHEMA, STRICT_FIELD, false);

    // Records for results
    record MetricStatistics(double mean, double stddev, double min, double max) {
    }

    record AnomalyRecord(String timestamp, double value, double expectedValue, double baselineMean, double percentageChange,
        double anomalyScore, String type) {
    }

    record TrendInfo(String direction, double changeRate) {
    }

    record MetricResult(String field, String aggregationType, int anomalyCount, int totalBuckets, MetricStatistics statistics,
        List<AnomalyRecord> anomalies, TrendInfo trend) {
    }

    /**
     * Parameter class to hold analysis parameters with validation
     */
    static class AnalysisParameters {
        final String index;
        final String timeField;
        final String selectionTimeRangeStart;
        final String selectionTimeRangeEnd;
        final String baselineTimeRangeStart;
        final String baselineTimeRangeEnd;
        final String interval;
        final String filter;
        final String aggregationType;
        final double zScoreThreshold;
        final double changeThreshold;
        final List<String> metricFields;

        AnalysisParameters(Map<String, String> parameters) {
            this.index = parameters.getOrDefault(PARAM_INDEX, "");
            this.timeField = parameters.getOrDefault(PARAM_TIME_FIELD, DEFAULT_TIME_FIELD);
            this.selectionTimeRangeStart = parameters.getOrDefault(PARAM_SELECTION_TIME_RANGE_START, "");
            this.selectionTimeRangeEnd = parameters.getOrDefault(PARAM_SELECTION_TIME_RANGE_END, "");
            this.baselineTimeRangeStart = parameters.getOrDefault(PARAM_BASELINE_TIME_RANGE_START, "");
            this.baselineTimeRangeEnd = parameters.getOrDefault(PARAM_BASELINE_TIME_RANGE_END, "");
            this.interval = parameters.getOrDefault(PARAM_INTERVAL, "");
            this.filter = parameters.getOrDefault(PARAM_FILTER, "");

            String aggType = parameters.getOrDefault(PARAM_AGGREGATION_TYPE, DEFAULT_AGGREGATION_TYPE);
            if (!SUPPORTED_AGG_TYPES.contains(aggType)) {
                throw new IllegalArgumentException("Unsupported aggregation type: " + aggType + ". Supported: " + SUPPORTED_AGG_TYPES);
            }
            this.aggregationType = aggType;

            try {
                this.zScoreThreshold = Double.parseDouble(parameters.getOrDefault(PARAM_Z_SCORE_THRESHOLD, String.valueOf(DEFAULT_Z_SCORE_THRESHOLD)));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid zScoreThreshold: " + parameters.get(PARAM_Z_SCORE_THRESHOLD));
            }

            try {
                this.changeThreshold = Double.parseDouble(parameters.getOrDefault(PARAM_CHANGE_THRESHOLD, String.valueOf(DEFAULT_CHANGE_THRESHOLD)));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid changeThreshold: " + parameters.get(PARAM_CHANGE_THRESHOLD));
            }

            String metricFieldsStr = parameters.getOrDefault(PARAM_METRIC_FIELDS, "");
            if (Strings.isEmpty(metricFieldsStr)) {
                this.metricFields = List.of();
            } else {
                try {
                    this.metricFields = Arrays.asList(gson.fromJson(metricFieldsStr, String[].class));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid metricFields: must be a JSON array of strings, got '" + metricFieldsStr + "'");
                }
            }
        }

        void validate() {
            List<String> missingParams = new ArrayList<>();
            if (Strings.isEmpty(index)) missingParams.add(PARAM_INDEX);
            if (metricFields.isEmpty()) missingParams.add(PARAM_METRIC_FIELDS);
            if (Strings.isEmpty(selectionTimeRangeStart)) missingParams.add(PARAM_SELECTION_TIME_RANGE_START);
            if (Strings.isEmpty(selectionTimeRangeEnd)) missingParams.add(PARAM_SELECTION_TIME_RANGE_END);
            if (!missingParams.isEmpty()) {
                throw new IllegalArgumentException("Missing required parameters: " + String.join(", ", missingParams));
            }
        }

        boolean hasBaselineTime() {
            return !Strings.isEmpty(baselineTimeRangeStart) && !Strings.isEmpty(baselineTimeRangeEnd);
        }
    }

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;
    private Client client;

    public MetricsInsightTool(Client client) {
        this.client = client;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return DEFAULT_ATTRIBUTES;
    }

    @Override
    public void setAttributes(Map<String, Object> map) {}

    @Override
    public boolean validate(Map<String, String> map) {
        try {
            new AnalysisParameters(map).validate();
        } catch (Exception e) {
            log.error("Failed to validate MetricsInsightTool parameters: {}", e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public <T> void run(Map<String, String> originalParameters, ActionListener<T> listener) {
        try {
            Map<String, String> parameters = ToolUtils.extractInputParameters(originalParameters, DEFAULT_ATTRIBUTES);
            log.debug("Starting metrics insight analysis with parameters: {}", parameters.keySet());
            AnalysisParameters params = new AnalysisParameters(parameters);
            params.validate();

            if (params.hasBaselineTime()) {
                executeBaselineComparison(params, listener);
            } else {
                executeSinglePeriodAnalysis(params, listener);
            }
        } catch (IllegalArgumentException e) {
            log.error("Invalid parameters for MetricsInsightTool: {}", e.getMessage());
            listener.onFailure(e);
        } catch (Exception e) {
            log.error("Unexpected error in MetricsInsightTool", e);
            listener.onFailure(e);
        }
    }

    <T> void executeSinglePeriodAnalysis(AnalysisParameters params, ActionListener<T> listener) {
        try {
            String resolvedInterval = resolveInterval(params);
            SearchRequest request = buildAggregationSearchRequest(
                params, params.selectionTimeRangeStart, params.selectionTimeRangeEnd, resolvedInterval
            );

            client.search(request, ActionListener.wrap(response -> {
                try {
                    Map<String, List<double[]>> seriesMap = parseAggregationResponse(response, params);
                    List<MetricResult> results = new ArrayList<>();

                    for (Map.Entry<String, List<double[]>> entry : seriesMap.entrySet()) {
                        String field = entry.getKey();
                        List<double[]> series = entry.getValue();

                        if (series.isEmpty()) {
                            results.add(new MetricResult(field, params.aggregationType, 0, 0, new MetricStatistics(0, 0, 0, 0), List.of(), new TrendInfo("stable", 0)));
                            continue;
                        }

                        MetricStatistics stats = computeStatistics(series);
                        List<AnomalyRecord> zScoreAnomalies = detectZScoreAnomalies(series, params.zScoreThreshold, stats);
                        List<AnomalyRecord> maAnomalies = detectMovingAverageAnomalies(series, MOVING_AVG_WINDOW);
                        List<AnomalyRecord> merged = mergeAnomalies(zScoreAnomalies, maAnomalies);
                        TrendInfo trend = detectTrend(series);

                        results.add(new MetricResult(field, params.aggregationType, merged.size(), series.size(), stats, merged, trend));
                    }

                    String output = buildOutput(results, "single");
                    listener.onResponse((T) output);
                } catch (Exception e) {
                    log.error("Failed to process single period analysis", e);
                    listener.onFailure(e);
                }
            }, listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    <T> void executeBaselineComparison(AnalysisParameters params, ActionListener<T> listener) {
        try {
            String resolvedInterval = resolveInterval(params);

            SearchRequest selectionRequest = buildAggregationSearchRequest(
                params, params.selectionTimeRangeStart, params.selectionTimeRangeEnd, resolvedInterval
            );

            client.search(selectionRequest, ActionListener.wrap(selectionResponse -> {
                try {
                    Map<String, List<double[]>> selectionSeries = parseAggregationResponse(selectionResponse, params);

                    SearchRequest baselineRequest = buildAggregationSearchRequest(
                        params, params.baselineTimeRangeStart, params.baselineTimeRangeEnd, resolvedInterval
                    );

                    client.search(baselineRequest, ActionListener.wrap(baselineResponse -> {
                        try {
                            Map<String, List<double[]>> baselineSeries = parseAggregationResponse(baselineResponse, params);
                            List<MetricResult> results = new ArrayList<>();

                            for (String field : params.metricFields) {
                                List<double[]> selSeries = selectionSeries.getOrDefault(field, List.of());
                                List<double[]> basSeries = baselineSeries.getOrDefault(field, List.of());

                                if (selSeries.isEmpty()) {
                                    results.add(new MetricResult(field, params.aggregationType, 0, 0, new MetricStatistics(0, 0, 0, 0), List.of(), new TrendInfo("stable", 0)));
                                    continue;
                                }

                                MetricStatistics stats = computeStatistics(selSeries);
                                List<AnomalyRecord> anomalies = detectBaselineAnomalies(
                                    selSeries, basSeries, params.zScoreThreshold, params.changeThreshold
                                );
                                TrendInfo trend = detectTrend(selSeries);

                                results.add(new MetricResult(field, params.aggregationType, anomalies.size(), selSeries.size(), stats, anomalies, trend));
                            }

                            String output = buildOutput(results, "baseline");
                            listener.onResponse((T) output);
                        } catch (Exception e) {
                            log.error("Failed to process baseline comparison", e);
                            listener.onFailure(e);
                        }
                    }, listener::onFailure));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    SearchRequest buildAggregationSearchRequest(AnalysisParameters params, String startTime, String endTime, String interval) {
        String formattedStart = formatTimeString(startTime);
        String formattedEnd = formatTimeString(endTime);

        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .filter(new RangeQueryBuilder(params.timeField).gte(formattedStart).lte(formattedEnd));

        if (!Strings.isEmpty(params.filter)) {
            try {
                Map<String, Object> filterMap = gson.fromJson(params.filter, new TypeToken<Map<String, Object>>() {}.getType());
                BoolQueryBuilder filterQuery = QueryBuilders.boolQuery();
                buildQueryFromMap(filterMap, filterQuery);
                query.must(filterQuery);
            } catch (Exception e) {
                log.warn("Failed to parse filter: {}", params.filter, e);
            }
        }

        DateHistogramAggregationBuilder dateHistogram = AggregationBuilders
            .dateHistogram(TIME_BUCKETS_AGG)
            .field(params.timeField)
            .fixedInterval(new DateHistogramInterval(interval))
            .minDocCount(0);

        for (String field : params.metricFields) {
            switch (params.aggregationType) {
                case "avg" -> dateHistogram.subAggregation(AggregationBuilders.avg("metric_" + field).field(field));
                case "sum" -> dateHistogram.subAggregation(AggregationBuilders.sum("metric_" + field).field(field));
                case "max" -> dateHistogram.subAggregation(AggregationBuilders.max("metric_" + field).field(field));
                case "min" -> dateHistogram.subAggregation(AggregationBuilders.min("metric_" + field).field(field));
                case "p99" -> dateHistogram.subAggregation(AggregationBuilders.percentiles("metric_" + field).field(field).percentiles(99.0));
            }
        }

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .query(query)
            .size(0)
            .aggregation(dateHistogram);

        return new SearchRequest(params.index).source(sourceBuilder);
    }

    Map<String, List<double[]>> parseAggregationResponse(
        org.opensearch.action.search.SearchResponse response,
        AnalysisParameters params
    ) {
        Map<String, List<double[]>> result = new HashMap<>();
        for (String field : params.metricFields) {
            result.put(field, new ArrayList<>());
        }

        if (response.getAggregations() == null) {
            return result;
        }

        Histogram histogram = response.getAggregations().get(TIME_BUCKETS_AGG);
        if (histogram == null) {
            return result;
        }

        for (Histogram.Bucket bucket : histogram.getBuckets()) {
            double timestamp = Instant.from((java.time.temporal.TemporalAccessor) bucket.getKey()).toEpochMilli();

            for (String field : params.metricFields) {
                String aggName = "metric_" + field;
                double value = extractMetricValue(bucket, aggName, params.aggregationType);

                if (!Double.isNaN(value) && !Double.isInfinite(value)) {
                    result.get(field).add(new double[]{timestamp, value});
                }
            }
        }

        return result;
    }

    private double extractMetricValue(Histogram.Bucket bucket, String aggName, String aggType) {
        return switch (aggType) {
            case "avg" -> ((Avg) bucket.getAggregations().get(aggName)).getValue();
            case "sum" -> ((Sum) bucket.getAggregations().get(aggName)).getValue();
            case "max" -> ((Max) bucket.getAggregations().get(aggName)).getValue();
            case "min" -> ((Min) bucket.getAggregations().get(aggName)).getValue();
            case "p99" -> {
                Percentiles percentiles = bucket.getAggregations().get(aggName);
                yield percentiles.percentile(99.0);
            }
            default -> Double.NaN;
        };
    }

    static String autoDetectInterval(String startTime, String endTime) {
        Instant start = parseToInstant(startTime);
        Instant end = parseToInstant(endTime);
        long durationMinutes = ChronoUnit.MINUTES.between(start, end);

        if (durationMinutes < 60) return "1m";
        if (durationMinutes < 360) return "5m";
        if (durationMinutes < 1440) return "15m";
        if (durationMinutes < 10080) return "1h";
        if (durationMinutes < 43200) return "6h";
        return "1d";
    }

    private String resolveInterval(AnalysisParameters params) {
        if (!Strings.isEmpty(params.interval)) {
            return params.interval;
        }
        return autoDetectInterval(params.selectionTimeRangeStart, params.selectionTimeRangeEnd);
    }

    // --- Statistical Analysis Methods ---

    static MetricStatistics computeStatistics(List<double[]> series) {
        if (series.isEmpty()) {
            return new MetricStatistics(0, 0, 0, 0);
        }

        double sum = 0;
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;

        for (double[] point : series) {
            double v = point[1];
            sum += v;
            if (v < min) min = v;
            if (v > max) max = v;
        }

        double mean = sum / series.size();

        double varianceSum = 0;
        for (double[] point : series) {
            double diff = point[1] - mean;
            varianceSum += diff * diff;
        }
        double stddev = Math.sqrt(varianceSum / series.size());

        return new MetricStatistics(
            roundTo(mean, 2),
            roundTo(stddev, 2),
            roundTo(min, 2),
            roundTo(max, 2)
        );
    }

    static List<AnomalyRecord> detectZScoreAnomalies(List<double[]> series, double threshold, MetricStatistics stats) {
        List<AnomalyRecord> anomalies = new ArrayList<>();
        if (stats.stddev() == 0) {
            return anomalies;
        }

        for (double[] point : series) {
            double zScore = Math.abs(point[1] - stats.mean()) / stats.stddev();
            if (zScore > threshold) {
                anomalies.add(new AnomalyRecord(
                    formatEpochMillis((long) point[0]),
                    roundTo(point[1], 2),
                    roundTo(stats.mean(), 2),
                    0,
                    0,
                    roundTo(zScore, 2),
                    "zscore"
                ));
            }
        }
        return anomalies;
    }

    static List<AnomalyRecord> detectMovingAverageAnomalies(List<double[]> series, int window) {
        List<AnomalyRecord> anomalies = new ArrayList<>();
        if (series.size() < window) {
            return anomalies;
        }

        // Compute moving averages
        double[] movingAvgs = new double[series.size()];
        Arrays.fill(movingAvgs, Double.NaN);

        for (int i = window - 1; i < series.size(); i++) {
            double sum = 0;
            for (int j = i - window + 1; j <= i; j++) {
                sum += series.get(j)[1];
            }
            movingAvgs[i] = sum / window;
        }

        // Compute average deviation from moving average
        double totalDeviation = 0;
        int deviationCount = 0;
        for (int i = window - 1; i < series.size(); i++) {
            totalDeviation += Math.abs(series.get(i)[1] - movingAvgs[i]);
            deviationCount++;
        }

        if (deviationCount == 0) {
            return anomalies;
        }

        double avgDeviation = totalDeviation / deviationCount;
        if (avgDeviation == 0) {
            return anomalies;
        }

        double deviationThreshold = avgDeviation * MOVING_AVG_DEVIATION_MULTIPLIER;

        for (int i = window - 1; i < series.size(); i++) {
            double deviation = Math.abs(series.get(i)[1] - movingAvgs[i]);
            if (deviation > deviationThreshold) {
                double score = deviation / avgDeviation;
                anomalies.add(new AnomalyRecord(
                    formatEpochMillis((long) series.get(i)[0]),
                    roundTo(series.get(i)[1], 2),
                    roundTo(movingAvgs[i], 2),
                    0,
                    0,
                    roundTo(score, 2),
                    "moving_average"
                ));
            }
        }
        return anomalies;
    }

    static List<AnomalyRecord> detectBaselineAnomalies(
        List<double[]> selSeries,
        List<double[]> basSeries,
        double zThreshold,
        double changeThreshold
    ) {
        List<AnomalyRecord> anomalies = new ArrayList<>();
        if (basSeries.isEmpty()) {
            return anomalies;
        }

        MetricStatistics baselineStats = computeStatistics(basSeries);
        double baselineMean = baselineStats.mean();
        double baselineStddev = baselineStats.stddev();

        for (double[] point : selSeries) {
            double value = point[1];
            boolean isAnomaly = false;
            double anomalyScore = 0;
            String type = "baseline";

            // Check percentage change
            double percentageChange = 0;
            if (baselineMean != 0) {
                percentageChange = ((value - baselineMean) / Math.abs(baselineMean)) * 100.0;
            }

            if (Math.abs(percentageChange) > changeThreshold) {
                isAnomaly = true;
                anomalyScore = Math.abs(percentageChange) / changeThreshold;
            }

            // Check z-score vs baseline
            if (baselineStddev > 0) {
                double zScore = Math.abs(value - baselineMean) / baselineStddev;
                if (zScore > zThreshold) {
                    isAnomaly = true;
                    anomalyScore = Math.max(anomalyScore, zScore);
                }
            }

            if (isAnomaly) {
                anomalies.add(new AnomalyRecord(
                    formatEpochMillis((long) point[0]),
                    roundTo(value, 2),
                    roundTo(baselineMean, 2),
                    roundTo(baselineMean, 2),
                    roundTo(percentageChange, 2),
                    roundTo(anomalyScore, 2),
                    type
                ));
            }
        }
        return anomalies;
    }

    static TrendInfo detectTrend(List<double[]> series) {
        if (series.size() < 2) {
            return new TrendInfo("stable", 0);
        }

        // Simple linear regression: y = mx + b
        int n = series.size();
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;

        for (int i = 0; i < n; i++) {
            double x = i;
            double y = series.get(i)[1];
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumX2 += x * x;
        }

        double denominator = n * sumX2 - sumX * sumX;
        if (denominator == 0) {
            return new TrendInfo("stable", 0);
        }

        double slope = (n * sumXY - sumX * sumY) / denominator;

        // Calculate change rate as percentage of mean
        double mean = sumY / n;
        double changeRate = 0;
        if (mean != 0) {
            changeRate = (slope * (n - 1)) / Math.abs(mean) * 100.0;
        }

        String direction;
        if (Math.abs(changeRate) < 5.0) {
            direction = "stable";
        } else if (changeRate > 0) {
            direction = "increasing";
        } else {
            direction = "decreasing";
        }

        return new TrendInfo(direction, roundTo(changeRate, 2));
    }

    static List<AnomalyRecord> mergeAnomalies(List<AnomalyRecord> zscore, List<AnomalyRecord> ma) {
        Map<String, AnomalyRecord> merged = new LinkedHashMap<>();

        for (AnomalyRecord a : zscore) {
            merged.put(a.timestamp(), a);
        }

        for (AnomalyRecord a : ma) {
            AnomalyRecord existing = merged.get(a.timestamp());
            if (existing == null) {
                merged.put(a.timestamp(), a);
            } else if (a.anomalyScore() > existing.anomalyScore()) {
                merged.put(a.timestamp(), a);
            }
        }

        return new ArrayList<>(merged.values());
    }

    // --- Utility Methods ---

    String formatTimeString(String timeString) throws DateTimeParseException {
        log.debug("Attempting to parse time string: {}", timeString);

        try {
            if (timeString.endsWith("Z")) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'", Locale.ROOT);
                ZonedDateTime dateTime = ZonedDateTime.parse(timeString, formatter.withZone(ZoneOffset.UTC));
                return dateTime.format(DateTimeFormatter.ISO_INSTANT);
            }
        } catch (DateTimeParseException e) {
            log.debug("Failed to parse as UTC time: {}", e.getMessage());
        }

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT_PATTERN, Locale.ROOT);
            LocalDateTime localDateTime = LocalDateTime.parse(timeString, formatter);
            ZonedDateTime zonedDateTime = localDateTime.atOffset(ZoneOffset.UTC).toZonedDateTime();
            return zonedDateTime.format(DateTimeFormatter.ISO_INSTANT);
        } catch (DateTimeParseException e) {
            log.debug("Failed to parse as local time: {}", e.getMessage());
        }

        try {
            ZonedDateTime dateTime = ZonedDateTime.parse(timeString);
            return dateTime.format(DateTimeFormatter.ISO_INSTANT);
        } catch (DateTimeParseException e) {
            log.debug("Failed to parse as ISO format: {}", e.getMessage());
        }

        throw new DateTimeParseException("Unable to parse time string: " + timeString, timeString, 0);
    }

    static Instant parseToInstant(String timeString) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
            LocalDateTime localDateTime = LocalDateTime.parse(timeString, formatter);
            return localDateTime.toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            // fallthrough
        }

        try {
            return ZonedDateTime.parse(timeString).toInstant();
        } catch (DateTimeParseException e) {
            // fallthrough
        }

        try {
            return Instant.parse(timeString);
        } catch (DateTimeParseException e) {
            throw new DateTimeParseException("Unable to parse time string: " + timeString, timeString, 0);
        }
    }

    private void buildQueryFromMap(Map<String, Object> filterMap, BoolQueryBuilder queryBuilder) {
        for (Map.Entry<String, Object> entry : filterMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            switch (key) {
                case "bool" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> boolMap = (Map<String, Object>) value;
                        processBoolQuery(boolMap, queryBuilder);
                    }
                }
                case "term" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> termMap = (Map<String, Object>) value;
                        for (Map.Entry<String, Object> termEntry : termMap.entrySet()) {
                            queryBuilder.must(QueryBuilders.termQuery(termEntry.getKey(), termEntry.getValue()));
                        }
                    }
                }
                case "terms" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> termsMap = (Map<String, Object>) value;
                        for (Map.Entry<String, Object> termsEntry : termsMap.entrySet()) {
                            if (termsEntry.getValue() instanceof List) {
                                queryBuilder.must(QueryBuilders.termsQuery(termsEntry.getKey(), (List<?>) termsEntry.getValue()));
                            }
                        }
                    }
                }
                case "range" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> rangeMap = (Map<String, Object>) value;
                        for (Map.Entry<String, Object> rangeEntry : rangeMap.entrySet()) {
                            if (rangeEntry.getValue() instanceof Map) {
                                processRangeQuery(rangeEntry.getKey(), rangeEntry.getValue(), queryBuilder);
                            }
                        }
                    }
                }
                case "match" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> matchMap = (Map<String, Object>) value;
                        for (Map.Entry<String, Object> matchEntry : matchMap.entrySet()) {
                            queryBuilder.must(QueryBuilders.matchQuery(matchEntry.getKey(), matchEntry.getValue()));
                        }
                    }
                }
                case "match_all" -> queryBuilder.must(QueryBuilders.matchAllQuery());
                case "exists" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> existsMap = (Map<String, Object>) value;
                        Object fieldValue = existsMap.get("field");
                        if (fieldValue != null) {
                            queryBuilder.must(QueryBuilders.existsQuery(fieldValue.toString()));
                        }
                    }
                }
                default -> {
                    if (value instanceof Map) {
                        // Treat as field name with nested operators (e.g. {"field": {"gte": 10}})
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        processRangeQuery(key, valueMap, queryBuilder);
                    } else {
                        queryBuilder.must(QueryBuilders.termQuery(key, value));
                    }
                }
            }
        }
    }

    private void processBoolQuery(Map<String, Object> boolMap, BoolQueryBuilder queryBuilder) {
        for (Map.Entry<String, Object> boolEntry : boolMap.entrySet()) {
            String boolType = boolEntry.getKey();
            Object boolValue = boolEntry.getValue();

            if (boolValue instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> clauses = (List<Map<String, Object>>) boolValue;
                for (Map<String, Object> clause : clauses) {
                    BoolQueryBuilder subQuery = QueryBuilders.boolQuery();
                    buildQueryFromMap(clause, subQuery);
                    switch (boolType) {
                        case "must" -> queryBuilder.must(subQuery);
                        case "should" -> queryBuilder.should(subQuery);
                        case "must_not" -> queryBuilder.mustNot(subQuery);
                        case "filter" -> queryBuilder.filter(subQuery);
                    }
                }
            }
        }
    }

    private void processRangeQuery(String field, Object operatorValue, BoolQueryBuilder queryBuilder) {
        if (!(operatorValue instanceof Map)) return;

        @SuppressWarnings("unchecked")
        Map<String, Object> rangeMap = (Map<String, Object>) operatorValue;
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(field);

        rangeMap.forEach((rangeOp, rangeVal) -> {
            switch (rangeOp) {
                case "gte" -> rangeQuery.gte(rangeVal);
                case "lte" -> rangeQuery.lte(rangeVal);
                case "gt" -> rangeQuery.gt(rangeVal);
                case "lt" -> rangeQuery.lt(rangeVal);
            }
        });

        queryBuilder.must(rangeQuery);
    }

    private String buildOutput(List<MetricResult> results, String analysisMode) {
        List<Map<String, Object>> metricsOutput = results.stream().map(r -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("field", r.field());
            m.put("aggregationType", r.aggregationType());
            m.put("anomalyCount", r.anomalyCount());
            m.put("totalBuckets", r.totalBuckets());
            m.put("statistics", Map.of(
                "mean", r.statistics().mean(),
                "stddev", r.statistics().stddev(),
                "min", r.statistics().min(),
                "max", r.statistics().max()
            ));
            m.put("anomalies", r.anomalies().stream().map(a -> {
                Map<String, Object> am = new LinkedHashMap<>();
                am.put("timestamp", a.timestamp());
                am.put("value", a.value());
                am.put("expectedValue", a.expectedValue());
                if ("baseline".equals(a.type())) {
                    am.put("baselineMean", a.baselineMean());
                    am.put("percentageChange", a.percentageChange());
                }
                am.put("anomalyScore", a.anomalyScore());
                am.put("type", a.type());
                return am;
            }).collect(Collectors.toList()));
            m.put("trend", Map.of("direction", r.trend().direction(), "changeRate", r.trend().changeRate()));
            return m;
        }).collect(Collectors.toList());

        int metricsWithAnomalies = (int) results.stream().filter(r -> r.anomalyCount() > 0).count();
        int totalAnomalies = results.stream().mapToInt(MetricResult::anomalyCount).sum();

        Map<String, Object> output = new LinkedHashMap<>();
        output.put("metrics", metricsOutput);
        output.put("summary", Map.of(
            "totalMetrics", results.size(),
            "metricsWithAnomalies", metricsWithAnomalies,
            "totalAnomalies", totalAnomalies,
            "analysisMode", analysisMode
        ));

        return gson.toJson(output);
    }

    private static String formatEpochMillis(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).toString();
    }

    private static double roundTo(double value, int places) {
        double factor = Math.pow(10, places);
        return Math.round(value * factor) / factor;
    }

    /**
     * Factory class for creating MetricsInsightTool instances
     */
    public static class Factory implements Tool.Factory<MetricsInsightTool> {
        private Client client;
        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (MetricsInsightTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public MetricsInsightTool create(Map<String, Object> map) {
            return new MetricsInsightTool(client);
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public Map<String, Object> getDefaultAttributes() {
            return DEFAULT_ATTRIBUTES;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }
    }
}
