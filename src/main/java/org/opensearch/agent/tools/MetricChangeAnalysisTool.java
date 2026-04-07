/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.CommonValue.TOOL_INPUT_SCHEMA_FIELD;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.utils.ToolUtils;
import org.opensearch.transport.client.Client;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Tool for analyzing metric changes by comparing percentile distributions between time periods.
 * Uses relative change (percentage change) to rank fields by significance.
 * 
 * Usage:
 * POST /_plugins/_ml/agents/{agent_id}/_execute
 * {
 *   "parameters": {
 *     "index": "logs-2025.01.15",
 *     "timeField": "@timestamp",
 *     "selectionTimeRangeStart": "2025-01-15 10:00:00",
 *     "selectionTimeRangeEnd": "2025-01-15 11:00:00",
 *     "baselineTimeRangeStart": "2025-01-15 08:00:00",
 *     "baselineTimeRangeEnd": "2025-01-15 09:00:00",
 *     "size": 1000
 *   }
 * }
 */
@Log4j2
@Setter
@Getter
@ToolAnnotation(MetricChangeAnalysisTool.TYPE)
public class MetricChangeAnalysisTool implements Tool {
    public static final String TYPE = "MetricChangeAnalysisTool";

    private static final String DEFAULT_DESCRIPTION =
        "Compares percentile distributions (P50, P90) of numeric fields between two time ranges. "
            + "Returns top fields ranked by change score. "
            + "Use for root cause analysis when investigating metric anomalies. "
            + "Keep both time ranges short (e.g. 15-30 minutes) and similar in duration for accurate comparison.";

    private static final int DEFAULT_TOP_N = 10;

    public static final String DEFAULT_INPUT_SCHEMA =
        """
            {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Target OpenSearch index name"
                    },
                    "timeField": {
                        "type": "string",
                        "description": "Date/time field for filtering"
                    },
                    "selectionTimeRangeStart": {
                        "type": "string",
                        "description": "Start of target period (format: yyyy-MM-dd HH:mm:ss)"
                    },
                    "selectionTimeRangeEnd": {
                        "type": "string",
                        "description": "End of target period (format: yyyy-MM-dd HH:mm:ss)"
                    },
                    "baselineTimeRangeStart": {
                        "type": "string",
                        "description": "Start of baseline period (format: yyyy-MM-dd HH:mm:ss)"
                    },
                    "baselineTimeRangeEnd": {
                        "type": "string",
                        "description": "End of baseline period (format: yyyy-MM-dd HH:mm:ss). Should be at or before selectionTimeRangeStart"
                    },
                    "size": {
                        "type": "integer",
                        "description": "Maximum number of documents to analyze (default: 1000, max: 10000)"
                    },
                    "topN": {
                        "type": "integer",
                        "description": "Number of top fields to return, ranked by change score (default: 10)"
                    },
                    "queryType": {
                        "type": "string",
                        "description": "Query type: 'ppl' or 'dsl' (default: 'dsl')"
                    },
                    "filter": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "Additional DSL query conditions (optional)"
                    },
                    "dsl": {
                        "type": "string",
                        "description": "Complete raw DSL query as JSON string (optional)"
                    },
                    "ppl": {
                        "type": "string",
                        "description": "Complete PPL statement without time information (optional)"
                    }
                },
                "required": ["index", "timeField", "selectionTimeRangeStart", "selectionTimeRangeEnd", "baselineTimeRangeStart", "baselineTimeRangeEnd"]
            }
            """;

    public static final Map<String, Object> DEFAULT_ATTRIBUTES = Map
        .of(TOOL_INPUT_SCHEMA_FIELD, gson.toJson(gson.fromJson(DEFAULT_INPUT_SCHEMA, Map.class)));

    /**
     * Record for percentile statistics
     */
    private record PercentileStats(double p50, double p90) {
    }

    /**
     * Record for field percentile analysis with variance
     */
    private record FieldPercentileAnalysis(String field, double variance, PercentileStats selectionStats, PercentileStats baselineStats) {
    }

    /**
     * Result item for JSON output
     */
    private record PercentileAnalysisResult(String field, Double changeScore, Map<String, Double> selectionPercentiles,
        Map<String, Double> baselinePercentiles, Map<String, Double> logRatios) {
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
    private DataFetchingHelper dataFetchingHelper;

    /**
     * Constructs a MetricChangeAnalysisTool with the given OpenSearch client
     *
     * @param client The OpenSearch client for executing queries
     */
    public MetricChangeAnalysisTool(Client client) {
        this.client = client;
        this.dataFetchingHelper = new DataFetchingHelper(client);
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
    public boolean validate(Map<String, String> parameters) {
        try {
            // Use helper's validation logic
            DataFetchingHelper.AnalysisParameters params = new DataFetchingHelper.AnalysisParameters(parameters);
            params.validate();
            return true;
        } catch (IllegalArgumentException e) {
            log.error("Invalid parameters: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Executes percentile analysis on numeric fields between selection and baseline periods
     *
     * @param <T> The response type
     * @param originalParameters Input parameters for analysis
     * @param listener Action listener for handling results or failures
     */
    @Override
    public <T> void run(Map<String, String> originalParameters, ActionListener<T> listener) {
        try {
            Map<String, String> parameters = ToolUtils.extractInputParameters(originalParameters, DEFAULT_ATTRIBUTES);
            log.debug("Starting metric change analysis with parameters: {}", parameters.keySet());

            // Extract topN parameter (use Double.valueOf to handle "30.0" from Gson serialization)
            int topN = DEFAULT_TOP_N;
            String topNStr = parameters.get("topN");
            if (topNStr != null && !topNStr.isEmpty()) {
                try {
                    topN = Double.valueOf(topNStr).intValue();
                    if (topN <= 0) {
                        topN = DEFAULT_TOP_N;
                    }
                } catch (NumberFormatException e) {
                    log.warn("Invalid topN parameter '{}', using default: {}", topNStr, DEFAULT_TOP_N);
                }
            }

            // Use DataDistributionTool's data fetching mechanism
            fetchDataAndAnalyze(parameters, topN, listener);

        } catch (IllegalArgumentException e) {
            log.error("Invalid parameters for MetricChangeAnalysisTool: {}", e.getMessage());
            listener.onFailure(e);
        } catch (Exception e) {
            log.error("Unexpected error in MetricChangeAnalysisTool", e);
            listener.onFailure(e);
        }
    }

    /**
     * Fetches data using DataDistributionTool's mechanism and performs percentile analysis
     */
    private <T> void fetchDataAndAnalyze(Map<String, String> parameters, int topN, ActionListener<T> listener) {
        try {
            // Create analysis parameters
            DataFetchingHelper.AnalysisParameters params = new DataFetchingHelper.AnalysisParameters(parameters);

            // Get field types first
            String index = parameters.get("index");
            dataFetchingHelper.getFieldTypes(index, ActionListener.wrap((Map<String, String> fieldTypes) -> {
                // Get number fields
                Set<String> numberFields = dataFetchingHelper.getNumberFields(fieldTypes);

                if (numberFields.isEmpty()) {
                    listener
                        .onFailure(
                            new IllegalStateException("No numeric fields found in index. Percentile analysis requires numeric fields.")
                        );
                    return;
                }

                // Fetch selection and baseline data
                fetchBothDatasets(params, numberFields, topN, listener);
            }, listener::onFailure));

        } catch (Exception e) {
            log.error("Failed to fetch data for percentile analysis", e);
            listener.onFailure(e);
        }
    }

    /**
     * Fetches both selection and baseline datasets
     */
    private <T> void fetchBothDatasets(
        DataFetchingHelper.AnalysisParameters params,
        Set<String> numberFields,
        int topN,
        ActionListener<T> listener
    ) {
        try {
            // Fetch selection data
            dataFetchingHelper
                .fetchIndexData(
                    params.selectionTimeRangeStart,
                    params.selectionTimeRangeEnd,
                    params,
                    ActionListener.wrap((List<Map<String, Object>> selectionData) -> {
                        // Fetch baseline data
                        dataFetchingHelper
                            .fetchIndexData(
                                params.baselineTimeRangeStart,
                                params.baselineTimeRangeEnd,
                                params,
                                ActionListener.wrap((List<Map<String, Object>> baselineData) -> {
                                    // Perform metric change analysis
                                    performAnalysis(selectionData, baselineData, numberFields, topN, listener);
                                }, listener::onFailure)
                            );
                    }, listener::onFailure)
                );

        } catch (Exception e) {
            log.error("Failed to fetch datasets", e);
            listener.onFailure(e);
        }
    }

    /**
     * Performs metric change analysis on the fetched data
     */
    private <T> void performAnalysis(
        List<Map<String, Object>> selectionData,
        List<Map<String, Object>> baselineData,
        Set<String> numberFields,
        int topN,
        ActionListener<T> listener
    ) {
        try {
            if (selectionData.isEmpty()) {
                listener.onFailure(new IllegalStateException("No data found for selection time range"));
                return;
            }

            if (baselineData.isEmpty()) {
                listener.onFailure(new IllegalStateException("No data found for baseline time range"));
                return;
            }

            // Calculate percentiles and relative changes
            List<FieldPercentileAnalysis> analyses = calculateMetricChangeAnalysis(selectionData, baselineData, numberFields);
            List<PercentileAnalysisResult> results = formatResults(analyses, topN);
            listener.onResponse((T) gson.toJson(Map.of("percentileAnalysis", results)));

        } catch (Exception e) {
            log.error("Failed to perform metric change analysis", e);
            listener.onFailure(e);
        }
    }

    /**
     * Formats analysis results for JSON output, limiting to top N results
     */
    private List<PercentileAnalysisResult> formatResults(List<FieldPercentileAnalysis> analyses, int topN) {
        return analyses.stream().limit(topN).map(analysis -> {
            Map<String, Double> selectionStats = Map.of("p50", analysis.selectionStats.p50, "p90", analysis.selectionStats.p90);

            Map<String, Double> baselineStats = Map.of("p50", analysis.baselineStats.p50, "p90", analysis.baselineStats.p90);

            Map<String, Double> logRatios = Map
                .of(
                    "p50",
                    safeLogRatio(analysis.selectionStats.p50, analysis.baselineStats.p50),
                    "p90",
                    safeLogRatio(analysis.selectionStats.p90, analysis.baselineStats.p90)
                );

            return new PercentileAnalysisResult(analysis.field, analysis.variance, selectionStats, baselineStats, logRatios);
        }).toList();
    }

    // ========== Metric Change Analysis Functions ==========

    /**
     * Calculates metric change analysis for all numeric fields
     */
    private List<FieldPercentileAnalysis> calculateMetricChangeAnalysis(
        List<Map<String, Object>> selectionData,
        List<Map<String, Object>> baselineData,
        Set<String> numberFields
    ) {
        List<FieldPercentileAnalysis> analyses = new ArrayList<>();

        for (String field : numberFields) {
            List<Double> selectionValues = extractNumericValues(selectionData, field);
            List<Double> baselineValues = extractNumericValues(baselineData, field);

            if (selectionValues.isEmpty() || baselineValues.isEmpty()) {
                continue;
            }

            PercentileStats selectionStats = calculatePercentiles(selectionValues);
            PercentileStats baselineStats = calculatePercentiles(baselineValues);
            double variance = calculatePercentileVariance(selectionStats, baselineStats);

            analyses.add(new FieldPercentileAnalysis(field, variance, selectionStats, baselineStats));
        }

        analyses.sort(Comparator.comparingDouble((FieldPercentileAnalysis a) -> a.variance).reversed());
        return analyses;
    }

    /**
     * Extracts numeric values from dataset for a specific field
     */
    private List<Double> extractNumericValues(List<Map<String, Object>> data, String field) {
        List<Double> values = new ArrayList<>();

        for (Map<String, Object> doc : data) {
            Object value = getFlattenedValue(doc, field);
            if (value != null) {
                try {
                    if (value instanceof Number) {
                        values.add(((Number) value).doubleValue());
                    } else {
                        values.add(Double.parseDouble(value.toString()));
                    }
                } catch (NumberFormatException e) {
                    // Skip non-numeric values
                }
            }
        }

        return values;
    }

    /**
     * Extracts nested field values from document using dot notation
     */
    private Object getFlattenedValue(Map<String, Object> doc, String field) {
        return dataFetchingHelper.getFlattenedValue(doc, field);
    }

    /**
     * Calculates statistics (avg, P50, P90) for a list of values
     */
    private PercentileStats calculatePercentiles(List<Double> values) {
        if (values.isEmpty()) {
            return new PercentileStats(0.0, 0.0);
        }

        List<Double> sorted = new ArrayList<>(values);
        sorted.sort(Double::compareTo);

        double p50 = calculatePercentile(sorted, 50);
        double p90 = calculatePercentile(sorted, 90);

        return new PercentileStats(p50, p90);
    }

    /**
     * Calculates a specific percentile from sorted values
     */
    private double calculatePercentile(List<Double> sortedValues, int percentile) {
        if (sortedValues.isEmpty()) {
            return 0.0;
        }

        if (sortedValues.size() == 1) {
            return sortedValues.get(0);
        }

        double index = (percentile / 100.0) * (sortedValues.size() - 1);
        int lowerIndex = (int) Math.floor(index);
        int upperIndex = (int) Math.ceil(index);

        if (lowerIndex == upperIndex) {
            return sortedValues.get(lowerIndex);
        }

        double lowerValue = sortedValues.get(lowerIndex);
        double upperValue = sortedValues.get(upperIndex);
        double fraction = index - lowerIndex;

        return lowerValue + (upperValue - lowerValue) * fraction;
    }

    private static final double LOG_RATIO_CAP = 10.0;
    private static final double EPSILON = 1e-10;

    /**
     * Calculates change score between selection and baseline statistics.
     * Uses weighted log-ratio scoring on avg, P50, and P90.
     * Skips a metric if its baseline is near zero to avoid inflated scores.
     * Redistributes weight equally among valid metrics.
     */
    private double calculatePercentileVariance(PercentileStats selectionStats, PercentileStats baselineStats) {
        boolean p50Valid = Math.abs(baselineStats.p50) >= EPSILON;
        boolean p90Valid = Math.abs(baselineStats.p90) >= EPSILON;

        if (!p50Valid && !p90Valid) {
            return 0.0;
        }
        if (p50Valid && p90Valid) {
            return 0.5 * safeLogRatio(selectionStats.p50, baselineStats.p50) + 0.5 * safeLogRatio(selectionStats.p90, baselineStats.p90);
        }
        if (p50Valid) {
            return safeLogRatio(selectionStats.p50, baselineStats.p50);
        }
        return safeLogRatio(selectionStats.p90, baselineStats.p90);
    }

    /**
     * Computes |log(selection / baseline)| with safe handling of near-zero values.
     * Returns 0 when both values are near zero, caps at LOG_RATIO_CAP when only baseline is near zero.
     */
    private double safeLogRatio(double selection, double baseline) {
        if (Math.abs(baseline) < EPSILON && Math.abs(selection) < EPSILON) {
            return 0.0;
        }
        if (Math.abs(baseline) < EPSILON) {
            return LOG_RATIO_CAP;
        }
        double ratio = selection / baseline;
        if (ratio <= 0) {
            return 0.0;
        }
        return Math.abs(Math.log(ratio));
    }

    /**
     * Factory class for creating MetricChangeAnalysisTool instances
     */
    public static class Factory implements Tool.Factory<MetricChangeAnalysisTool> {
        private Client client;
        private static Factory INSTANCE;

        /**
         * Create or return the singleton factory instance
         */
        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (MetricChangeAnalysisTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        /**
         * Initialize this factory
         *
         * @param client The OpenSearch client
         */
        public void init(Client client) {
            this.client = client;
        }

        @Override
        public MetricChangeAnalysisTool create(Map<String, Object> map) {
            return new MetricChangeAnalysisTool(client);
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
