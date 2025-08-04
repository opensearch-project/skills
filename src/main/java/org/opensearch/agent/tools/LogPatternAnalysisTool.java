/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.tools.utils.HierarchicalAgglomerativeClustering.calculateCosineSimilarity;
import static org.opensearch.agent.tools.utils.ToolHelper.getPPLTransportActionListener;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.json.JSONObject;
import org.opensearch.agent.tools.utils.HierarchicalAgglomerativeClustering;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import javax.swing.text.html.Option;

/**
 * Usage:
 * 1. Register agent:
 * POST /_plugins/_ml/agents/_register
 * {
 *   "name": "LogPatternAnalysis",
 *   "type": "flow",
 *   "tools": [
 *     {
 *       "name": "log_pattern_analysis_tool",
 *       "type": "LogPatternAnalysisTool",
 *       "parameters": {
 *       }
 *     }
 *   ]
 * }
 * 2. Execute agent:
 * POST /_plugins/_ml/agents/{agent_id}/_execute
 * {
 *   "parameters": {
 *     "index": "ss4o_logs-otel-2025.06.24",
 *     "logFieldName": "body",
 *     "traceFieldName": "traceId",
 *     "baseTimeRangeStart": "2025-06-24T07:33:05Z",
 *     "baseTimeRangeEnd": "2025-06-24T07:51:27Z",
 *     "selectionTimeRangeStart": "2025-06-24T07:50:26.999999999Z",
 *     "selectionTimeRangeEnd": "2025-06-24T07:55:56Z"
 *   }
 * }
 * 3. Result: a list of selection traceId
 * {
 *   "inference_results": [
 *     {
 *       "output": [
 *         {
 *           "name": "response",
 *           "result": """["34398ae14561313af05f1b02179aaf45","de0f0fa00083a5c54b8b732ae70ea158"]"""
 *         }
 *       ]
 *     }
 *   ]
 * }
 */
@Log4j2
@Setter
@Getter
@ToolAnnotation(LogPatternAnalysisTool.TYPE)
public class LogPatternAnalysisTool implements Tool {
    public static final String TYPE = "LogPatternAnalysisTool";

    // Constants
    private static final String DEFAULT_DESCRIPTION =
        "This is a tool used to detect selection log patterns by the patterns command in PPL or to detect selection log sequences by the log clustering algorithm.";
    private static final double LOG_VECTORS_CLUSTERING_THRESHOLD = 0.5;
    private static final double LOG_PATTERN_THRESHOLD = 0.75;
    private static final double LOG_PATTERN_LIFT = 3;
    private static final String DEFAULT_TIME_FIELD = "@timestamp";

    // Compiled regex patterns for better performance
    private static final Pattern REPEATED_WILDCARDS_PATTERN = Pattern.compile("(<\\*>)(\\s+<\\*>)+");

    /**
     * Parameter class to hold analysis parameters with validation
     */
    private static class AnalysisParameters {
        final String index;
        final String timeField;
        final String logFieldName;
        final String traceFieldName;
        final String baseTimeRangeStart;
        final String baseTimeRangeEnd;
        final String selectionTimeRangeStart;
        final String selectionTimeRangeEnd;

        AnalysisParameters(Map<String, String> parameters) {
            this.index = parameters.getOrDefault("index", "");
            this.timeField = parameters.getOrDefault("timeField", DEFAULT_TIME_FIELD);
            this.logFieldName = parameters.getOrDefault("logFieldName", "message");
            this.traceFieldName = parameters.getOrDefault("traceFieldName", "");
            this.baseTimeRangeStart = parameters.getOrDefault("baseTimeRangeStart", "");
            this.baseTimeRangeEnd = parameters.getOrDefault("baseTimeRangeEnd", "");
            this.selectionTimeRangeStart = parameters.getOrDefault("selectionTimeRangeStart", "");
            this.selectionTimeRangeEnd = parameters.getOrDefault("selectionTimeRangeEnd", "");
        }

        private void validate() {
            if (Strings.isEmpty(index)
                || Strings.isEmpty(timeField)
                || Strings.isEmpty(logFieldName)
                || Strings.isEmpty(selectionTimeRangeStart)
                || Strings.isEmpty(selectionTimeRangeEnd)) {
                throw new IllegalArgumentException(
                    "Invalid parameters: index, timeField, logFieldName, selectionTimeRangeStart, selectionTimeRangeEnd are required!"
                );
            }
        }

        boolean hasBaseTime() {
            return !Strings.isEmpty(baseTimeRangeStart) && !Strings.isEmpty(baseTimeRangeEnd);
        }

        boolean hasTraceField() {
            return !Strings.isEmpty(traceFieldName);
        }
    }

    /**
     * Result class for pattern analysis
     */
    private record PatternAnalysisResult(Map<String, Set<String>> tracePatternMap, Map<String, Set<String>> patternCountMap,
        Map<String, Double> patternValues, int totalTraceCount) {
    }

    private record PatternDiff(String pattern, Double base, Double selection, Double lift) {
    }

    private record PatternWithSamples(String pattern, double count, List<?> sampleLogs) {}

    // Instance fields
    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;
    private Client client;

    public LogPatternAnalysisTool(Client client) {
        this.client = client;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return Map.of();
    }

    @Override
    public void setAttributes(Map<String, Object> map) {

    }

    @Override
    public boolean validate(Map<String, String> map) {
        try {
            new AnalysisParameters(map).validate();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        try {
            log.info("Starting log pattern analysis with parameters: {}", parameters.keySet());
            AnalysisParameters params = new AnalysisParameters(parameters);

            if (params.hasTraceField() && params.hasBaseTime()) {
                log.info("Performing log sequence analysis for index: {}", params.index);
                logSequenceAnalysis(params, listener);
            } else if (params.hasBaseTime()){
                log.info("Performing log pattern analysis for index: {}", params.index);
                logPatternDiffAnalysis(params, listener);
            } else {
                logInsight(params, listener);
            }
        } catch (IllegalArgumentException e) {
            log.error("Invalid parameters for LogPatternAnalysisTool: {}", e.getMessage());
            listener.onFailure(e);
        } catch (Exception e) {
            log.error("Unexpected error in LogPatternAnalysisTool", e);
            listener.onFailure(new RuntimeException("Failed to execute log pattern analysis", e));
        }
    }

    private <T> void logSequenceAnalysis(AnalysisParameters params, ActionListener<T> listener) {
        log
            .debug(
                "Starting log sequence analysis for time ranges: base[{} - {}], selection[{} - {}]",
                params.baseTimeRangeStart,
                params.baseTimeRangeEnd,
                params.selectionTimeRangeStart,
                params.selectionTimeRangeEnd
            );

        // Step 1: Analyze base time range
        analyzeBaseTimeRange(params, ActionListener.wrap(baseResult -> {
            log.info("Base time range analysis completed, found {} traces", baseResult.totalTraceCount);

            // Step 2: Analyze selection time range
            analyzeSelectionTimeRange(params, ActionListener.wrap(selectionResult -> {
                log.info("Selection time range analysis completed, found {} traces", selectionResult.totalTraceCount);

                // Step 3: Generate comparison result
                generateSequenceComparisonResult(baseResult, selectionResult, listener);
            }, listener::onFailure));
        }, this::handlePPLError));
    }

    private <T> void analyzeBaseTimeRange(AnalysisParameters params, ActionListener<PatternAnalysisResult> listener) {
        String baseTimeRangeLogPatternPPL = buildLogPatternPPL(
            params.index,
            params.timeField,
            params.logFieldName,
            params.traceFieldName,
            params.baseTimeRangeStart,
            params.baseTimeRangeEnd
        );

        log.debug("Executing base time range PPL: {}", baseTimeRangeLogPatternPPL);
        executePPL(baseTimeRangeLogPatternPPL, ActionListener.wrap(response -> {
            try {
                PatternAnalysisResult result = processPatternAnalysisResponse(response);
                listener.onResponse(result);
            } catch (Exception e) {
                log.error("Failed to process base time range response", e);
                listener.onFailure(new RuntimeException("Failed to process base time range analysis", e));
            }
        }, listener::onFailure));
    }

    private <T> void analyzeSelectionTimeRange(AnalysisParameters params, ActionListener<PatternAnalysisResult> listener) {
        String selectionTimeRangeLogPatternPPL = buildLogPatternPPL(
            params.index,
            params.timeField,
            params.logFieldName,
            params.traceFieldName,
            params.selectionTimeRangeStart,
            params.selectionTimeRangeEnd
        );

        log.debug("Executing selection time range PPL: {}", selectionTimeRangeLogPatternPPL);
        executePPL(selectionTimeRangeLogPatternPPL, ActionListener.wrap(response -> {
            try {
                PatternAnalysisResult result = processPatternAnalysisResponse(response);
                listener.onResponse(result);
            } catch (Exception e) {
                log.error("Failed to process selection time range response", e);
                listener.onFailure(new RuntimeException("Failed to process selection time range analysis", e));
            }
        }, listener::onFailure));
    }

    private String buildLogPatternPPL(
        String index,
        String timeField,
        String logFieldName,
        String traceFieldName,
        String startTime,
        String endTime
    ) {
        return String
            .format(
                "source=%s | where %s!='' | where %s>'%s' and %s<'%s' | patterns %s method=brain "
                    + "variable_count_threshold=3 | fields %s, patterns_field, %s | sort %s",
                index,
                traceFieldName,
                timeField,
                startTime,
                timeField,
                endTime,
                logFieldName,
                traceFieldName,
                timeField,
                timeField
            );
    }

    private PatternAnalysisResult processPatternAnalysisResponse(String response) {
        Map<String, Object> pplResult = gson.fromJson(response, new TypeToken<Map<String, Object>>() {
        }.getType());

        Object datarowsObj = pplResult.get("datarows");
        if (!(datarowsObj instanceof List)) {
            throw new IllegalStateException("Invalid PPL response format: missing or invalid datarows");
        }

        @SuppressWarnings("unchecked")
        List<List<Object>> dataRows = (List<List<Object>>) datarowsObj;

        Map<String, Set<String>> tracePatternMap = new HashMap<>();
        Map<String, Set<String>> patternCountMap = new HashMap<>();
        Map<String, String> rawPatternCache = new HashMap<>();

        for (List<Object> row : dataRows) {
            if (row.size() < 2) {
                log.warn("Skipping invalid row with insufficient columns: {}", row);
                continue;
            }

            String traceId = (String) row.get(0);
            String rawPattern = (String) row.get(1);

            String simplifiedPattern = rawPatternCache.computeIfAbsent(rawPattern, this::postProcessPattern);

            tracePatternMap.computeIfAbsent(traceId, k -> new LinkedHashSet<>()).add(simplifiedPattern);
            patternCountMap.computeIfAbsent(simplifiedPattern, k -> new HashSet<>()).add(traceId);
        }

        // Calculate pattern values using IDF and sigmoid
        Map<String, Double> patternVectors = vectorizePattern(patternCountMap, tracePatternMap.size());

        return new PatternAnalysisResult(tracePatternMap, patternCountMap, patternVectors, tracePatternMap.size());
    }

    private Map<String, Double> vectorizePattern(Map<String, Set<String>> patternCountMap, int totalTraceCount) {
        Map<String, Double> patternValues = new HashMap<>();

        for (Map.Entry<String, Set<String>> entry : patternCountMap.entrySet()) {
            String pattern = entry.getKey();
            Set<String> traceIds = entry.getValue();

            if (traceIds != null && !traceIds.isEmpty()) {
                // IDF calculation
                double idf = Math.log((double) totalTraceCount / traceIds.size());
                // Apply sigmoid function
                double value = 1.0 / (1.0 + Math.exp(-idf));
                patternValues.put(pattern, value);
            } else {
                patternValues.put(pattern, 0.0);
            }
        }

        return patternValues;
    }

    private <T> void generateSequenceComparisonResult(
        PatternAnalysisResult baseResult,
        PatternAnalysisResult selectionResult,
        ActionListener<T> listener
    ) {
        try {
            // Step 3: Build pattern index for vector construction
            Map<String, Integer> patternIndexMap = buildPatternIndex(baseResult, selectionResult);
            log.debug("Built pattern index with {} patterns", patternIndexMap.size());

            // Step 4: Build vectors for base time range
            Map<String, double[]> baseVectorMap = buildVectorMap(
                baseResult.tracePatternMap,
                baseResult.patternValues,
                patternIndexMap,
                false
            );

            // Step 5: Cluster base vectors and find centroids
            List<String> baseRepresentative = clusterLogVectorsAndGetRepresentative(baseVectorMap);

            // Step 6: Build vectors for selection time range
            Map<String, double[]> selectionVectorMap = buildVectorMap(
                selectionResult.tracePatternMap,
                selectionResult.patternValues,
                patternIndexMap,
                true,
                baseResult.patternCountMap,
                selectionResult.patternCountMap
            );

            // Step 7: Find selection centroids
            List<String> selectionRepresentative = clusterLogVectorsAndGetRepresentative(selectionVectorMap);

            List<String> selction = filterSelectionCentroids(
                baseRepresentative,
                selectionRepresentative,
                baseVectorMap,
                selectionVectorMap
            );

            log.info("Identified {} selection centroids from {} candidates", selction.size(), selectionRepresentative.size());

            // Generate final result
            Map<String, Map<String, String>> result = buildFinalResult(
                baseRepresentative,
                selction,
                baseResult.tracePatternMap,
                selectionResult.tracePatternMap
            );

            System.out.println("==::::" + gson.toJson(result));

            listener.onResponse((T) gson.toJson(result));

        } catch (Exception e) {
            log.error("Failed to generate sequence comparison result", e);
            listener.onFailure(new RuntimeException("Failed to generate comparison result", e));
        }
    }

    private Map<String, Integer> buildPatternIndex(PatternAnalysisResult baseResult, PatternAnalysisResult selectionResult) {
        Set<String> allPatterns = new HashSet<>(baseResult.patternCountMap.keySet());
        allPatterns.addAll(selectionResult.patternCountMap.keySet());

        List<String> sortedPatterns = new ArrayList<>(allPatterns);
        Collections.sort(sortedPatterns);

        // pattern and its index in a vector
        Map<String, Integer> patternIndexMap = new HashMap<>();
        for (int i = 0; i < sortedPatterns.size(); i++) {
            patternIndexMap.put(sortedPatterns.get(i), i);
        }

        return patternIndexMap;
    }

    private Map<String, double[]> buildVectorMap(
        Map<String, Set<String>> tracePatternMap,
        Map<String, Double> patternValues,
        Map<String, Integer> patternIndexMap,
        boolean isSelection,
        Map<String, Set<String>>... additionalPatternMaps
    ) {
        Map<String, double[]> vectorMap = new HashMap<>();
        int vectorSize = patternIndexMap.size();

        for (Map.Entry<String, Set<String>> entry : tracePatternMap.entrySet()) {
            String traceId = entry.getKey();
            Set<String> patterns = entry.getValue();
            double[] vector = new double[vectorSize];

            for (String pattern : patterns) {
                Integer index = patternIndexMap.get(pattern);
                if (index != null) {
                    double baseValue = 0.5 * patternValues.getOrDefault(pattern, 0.0);

                    if (isSelection && additionalPatternMaps.length >= 2) {
                        // Add existence weight for selection patterns
                        Map<String, Set<String>> basePatterns = additionalPatternMaps[0];
                        Map<String, Set<String>> selectionPatterns = additionalPatternMaps[1];

                        int existenceWeight = (selectionPatterns.containsKey(pattern) && !basePatterns.containsKey(pattern)) ? 1 : 0;
                        vector[index] = baseValue + 0.5 * existenceWeight;
                    } else {
                        vector[index] = baseValue;
                    }
                }
            }

            vectorMap.put(traceId, vector);
        }

        return vectorMap;
    }

    private List<String> filterSelectionCentroids(
        List<String> baseCentroids,
        List<String> selectionCandidates,
        Map<String, double[]> baseVectorMap,
        Map<String, double[]> selectionVectorMap
    ) {
        List<String> selectionCentroids = new ArrayList<>();

        for (String candidate : selectionCandidates) {
            boolean isSelection = true;
            double[] candidateVector = selectionVectorMap.get(candidate);

            if (candidateVector == null) {
                log.warn("No vector found for selection candidate: {}", candidate);
                continue;
            }

            for (String baseCentroid : baseCentroids) {
                double[] baseVector = baseVectorMap.get(baseCentroid);
                if (baseVector != null && calculateCosineSimilarity(baseVector, candidateVector) > LOG_VECTORS_CLUSTERING_THRESHOLD) {
                    isSelection = false;
                    break;
                }
            }

            if (isSelection) {
                selectionCentroids.add(candidate);
            }
        }

        return selectionCentroids;
    }

    private Map<String, Map<String, String>> buildFinalResult(
        List<String> baseCentroids,
        List<String> selectionCentroids,
        Map<String, Set<String>> baseTracePatternMap,
        Map<String, Set<String>> selectionTracePatternMap
    ) {
        Map<String, String> baseSequences = new HashMap<>();
        for (String centroid : baseCentroids) {
            Set<String> patterns = baseTracePatternMap.get(centroid);
            if (patterns != null) {
                baseSequences.put(centroid, String.join(" -> ", patterns));
            }
        }

        Map<String, String> selectionSequences = new HashMap<>();
        for (String centroid : selectionCentroids) {
            Set<String> patterns = selectionTracePatternMap.get(centroid);
            if (patterns != null) {
                selectionSequences.put(centroid, String.join(" -> ", patterns));
            }
        }

        Map<String, Map<String, String>> result = new HashMap<>();
        result.put("BASE", baseSequences);
        result.put("EXCEPTIONAL", selectionSequences);

        return result;
    }

    private <T> void logPatternDiffAnalysis(AnalysisParameters params, ActionListener<T> listener) {
        log
            .debug(
                "Starting log pattern analysis for time ranges: base[{} - {}], selection[{} - {}]",
                params.baseTimeRangeStart,
                params.baseTimeRangeEnd,
                params.selectionTimeRangeStart,
                params.selectionTimeRangeEnd
            );

        // Step 1: Generate log patterns for baseline time range
        String baseTimeRangeLogPatternPPL = buildLogPatternPPL(
            params.index,
            params.timeField,
            params.logFieldName,
            params.baseTimeRangeStart,
            params.baseTimeRangeEnd
        );
        Function<List<List<Object>>, Map<String, Double>> dataRowsParser = dataRows -> {
            Map<String, Double> patternMap = new HashMap<>();
            for (List<Object> row : dataRows) {
                if (row.size() == 2) {
                    String pattern = (String) row.get(1);
                    double count = ((Number) row.get(0)).doubleValue();
                    patternMap.put(pattern, count);
                }
            }
            return patternMap;
        };

        log.debug("Executing base time range pattern PPL: {}", baseTimeRangeLogPatternPPL);
        executePPL(baseTimeRangeLogPatternPPL, ActionListener.wrap(baseResponse -> {
            try {
                Map<String, Double> basePatterns = parseLogPatterns(baseResponse, dataRowsParser).orElse(new HashMap<>());
                mergeSimilarPatterns(basePatterns);

                log.debug("Base patterns processed: {} patterns", basePatterns.size());

                // Step 2: Generate log patterns for selection time range
                String selectionTimeRangeLogPatternPPL = buildLogPatternPPL(
                    params.index,
                    params.timeField,
                    params.logFieldName,
                    params.selectionTimeRangeStart,
                    params.selectionTimeRangeEnd
                );

                log.debug("Executing selection time range pattern PPL: {}", selectionTimeRangeLogPatternPPL);
                executePPL(selectionTimeRangeLogPatternPPL, ActionListener.wrap(selectionResponse -> {
                    try {
                        Map<String, Double> selectionPatterns =
                                parseLogPatterns(selectionResponse, dataRowsParser).orElse(new HashMap<>());
                        mergeSimilarPatterns(selectionPatterns);

                        log.debug("Selection patterns processed: {} patterns", selectionPatterns.size());

                        // Step 3: Calculate pattern differences
                        List<PatternDiff> patternDifferences = calculatePatternDifferences(basePatterns, selectionPatterns);

                        Map<String, Object> finalResult = new HashMap<>();
                        finalResult.put("patternMapDifference", patternDifferences);

                        log.info("Pattern analysis completed: {} differences found", patternDifferences.size());
                        log.debug("finalResult={}", gson.toJson(finalResult));
                        listener.onResponse((T) gson.toJson(finalResult));

                    } catch (Exception e) {
                        log.error("Failed to process selection pattern response", e);
                        listener.onFailure(new RuntimeException("Failed to process selection patterns", e));
                    }
                }, listener::onFailure));

            } catch (Exception e) {
                log.error("Failed to process base pattern response", e);
                listener.onFailure(new RuntimeException("Failed to process base patterns", e));
            }
        }, this::handlePPLError));
    }


    private <T> void logInsight(AnalysisParameters params, ActionListener<T> listener) {
        List<String> errorKeywords = List.of(
                "error", "err", "exception", "failed", "failure", "timeout", "panic", "fatal", "critical", "severe",
                "abort", "aborted", "aborting", "crash", "crashed", "broken", "corrupt", "corrupted", "invalid",
                "malformed", "unprocessable", "denied", "forbidden", "unauthorized", "conflict", "deadlock",
                "overflow", "underflow", "resource_exhausted", "out_of_resources", "quota_exceeded",
                "rate_limit_exceeded", "throttled", "disk_full", "no_space_left", "insufficient_storage",
                "dependency", "retrying", "cold_start", "warmup", "saturation", "backpressure", "queue_full",
                "degraded", "unexpected", "unusual", "missing", "stale", "expired", "mismatch",
                "validation_failed", "schema_violation", "timeout_approaching", "deadline_exceeded", "retry_backoff",
                "invalid_token", "expired_token", "token_revoked", "authentication_failed", "auth_error",
                "permission_denied", "role_mismatch", "audit_failure", "access_violation"
        );

        String selectionTimeRangeLogPatternPPL = String
                .format(
                        "source=%s | where %s>'%s' and %s<'%s' | where match(%s, '%s') | patterns %s method=brain " +
                        "mode=aggregation max_sample_count=2"
                        + "variable_count_threshold=3 | fields patterns_field, pattern_count, sample_logs "
                        + "| sort -pattern_count | head 5",
                        params.index,
                        params.timeField,
                        params.selectionTimeRangeStart,
                        params.timeField,
                        params.selectionTimeRangeEnd,
                        params.logFieldName,
                        String.join(" ", errorKeywords),
                        params.logFieldName
                );

        Function<List<List<Object>>, List<PatternWithSamples>> dataRowsParser = dataRows -> {
            List<PatternWithSamples> patternWithSamplesList = new ArrayList<>();
            for (List<Object> row : dataRows) {
                if (row.size() == 3) {
                    String pattern = (String) row.get(0);
                    double count = ((Number) row.get(1)).doubleValue();
                    List<?> samples = (List<?>) row.get(2);
                    patternWithSamplesList.add(new PatternWithSamples(pattern, count, samples));
                }
            }
            return patternWithSamplesList;
        };

        executePPL(selectionTimeRangeLogPatternPPL, ActionListener.wrap(baseResponse -> {
            try {
                List<PatternWithSamples> logInsights =
                        parseLogPatterns(baseResponse, dataRowsParser).orElse(new ArrayList<>());
                Map<String, Object> finalResult = new HashMap<>();
                finalResult.put("logInsights", logInsights);
                listener.onResponse((T) gson.toJson(finalResult));
            } catch (Exception e) {
                log.error("Failed to process base pattern response", e);
                listener.onFailure(new RuntimeException("Failed to process base patterns", e));
            }
        }, this::handlePPLError));
    }

    private String buildLogPatternPPL(String index, String timeField, String logFieldName, String startTime, String endTime) {
        return String
            .format(
                "source=%s | where %s>'%s' and %s<'%s' | patterns %s method=brain "
                    + "variable_count_threshold=3 | stats count() as cnt by patterns_field | fields cnt, patterns_field",
                index,
                timeField,
                startTime,
                timeField,
                endTime,
                logFieldName
            );
    }

    private List<PatternDiff> calculatePatternDifferences(Map<String, Double> basePatterns, Map<String, Double> selectionPatterns) {
        List<PatternDiff> differences = new ArrayList<>();

        double selectionTotal = selectionPatterns.values().stream().mapToDouble(Double::doubleValue).sum();
        double baseTotal = basePatterns.values().stream().mapToDouble(Double::doubleValue).sum();

        for (Map.Entry<String, Double> entry : selectionPatterns.entrySet()) {
            String pattern = entry.getKey();
            double selectionCount = entry.getValue();

            if (basePatterns.containsKey(pattern)) {
                double baseCount = basePatterns.get(pattern);
                double lift = (selectionCount / selectionTotal) / (baseCount / baseTotal);

                if (lift < 1) {
                    lift = 1.0 / lift;
                }

                if (lift > LOG_PATTERN_LIFT) {
                    differences.add(new PatternDiff(pattern, baseCount/baseTotal , selectionCount/selectionTotal , lift));
                }
            } else {
                // Pattern only exists in selection time range
                differences.add(new PatternDiff(pattern, 0.0, selectionCount/selectionTotal, null));
                log.debug("New selection pattern detected: {} (count: {})", pattern, selectionCount);
            }
        }

        return differences;
    }

    private <T> Optional<T> parseLogPatterns(String response, Function<List<List<Object>>, T> rowParser) {
        try {
            Map<String, Object> pplResult = gson.fromJson(response, new TypeToken<Map<String, Object>>() {
            }.getType());

            Object datarowsObj = pplResult.get("datarows");
            if (datarowsObj == null || !(datarowsObj instanceof List)) {
                log.warn("Invalid PPL response format: missing or invalid datarows");
                return Optional.empty();
            }

            @SuppressWarnings("unchecked")
            List<List<Object>> dataRows = (List<List<Object>>) datarowsObj;

            return Optional.ofNullable(rowParser.apply(dataRows));
        } catch (Exception e) {
            log.error("Failed to parse log patterns from response", e);
            return Optional.empty();
        }
    }

    private void handlePPLError(Throwable error) {
        log.error("PPL execution failed: {}", error.getMessage());
        if (error.toString().contains("IndexNotFoundException")) {
            throw new IllegalArgumentException("Index not found: " + error.getMessage(), error);
        } else {
            throw new RuntimeException("PPL execution failed", error);
        }
    }

    private double jacCardSimilarity(String pattern1, String pattern2) {
        if (Strings.isEmpty(pattern1) || Strings.isEmpty(pattern2)) {
            return 0.0;
        }

        Set<String> set1 = new HashSet<>(Arrays.asList(pattern1.split("\\s+")));
        Set<String> set2 = new HashSet<>(Arrays.asList(pattern2.split("\\s+")));

        // Calculate intersection
        Set<String> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);

        // Calculate union
        Set<String> union = new HashSet<>(set1);
        union.addAll(set2);  // Fixed: was set1.addAll(set2) which is incorrect

        if (union.isEmpty()) {
            return 0.0;
        }

        return (double) intersection.size() / union.size();
    }

    private void mergeSimilarPatterns(Map<String, Double> patternMap) {
        if (patternMap.isEmpty()) {
            return;
        }

        List<String> patterns = new ArrayList<>(patternMap.keySet());
        patterns.sort(String::compareTo);
        Set<String> removed = new HashSet<>();

        for (int i = 0; i < patterns.size(); i++) {
            String pattern1 = patterns.get(i);
            if (removed.contains(pattern1)) {
                continue;
            }

            for (int j = i + 1; j < patterns.size(); j++) {
                String pattern2 = patterns.get(j);
                if (removed.contains(pattern2)) {
                    continue;
                }

                if (jacCardSimilarity(pattern1, pattern2) > LOG_PATTERN_THRESHOLD) {
                    // Merge pattern2 into pattern1
                    double count1 = patternMap.getOrDefault(pattern1, 0.0);
                    double count2 = patternMap.getOrDefault(pattern2, 0.0);
                    patternMap.put(pattern1, count1 + count2);
                    patternMap.remove(pattern2);
                    removed.add(pattern2);
                    log.debug("Merged similar patterns: '{}' + '{}' -> '{}'", pattern1, pattern2, pattern1);
                }
            }
        }

        // Post-process patterns and merge those with similar processed forms
        Map<String, String> toReplace = new HashMap<>();
        for (String pattern : patternMap.keySet()) {
            String processedPattern = postProcessPattern(pattern);
            if (!processedPattern.equals(pattern)) {
                toReplace.put(pattern, processedPattern);
            }
        }

        for (Map.Entry<String, String> entry : toReplace.entrySet()) {
            String originalPattern = entry.getKey();
            String processedPattern = entry.getValue();
            double count = patternMap.remove(originalPattern);
            patternMap.merge(processedPattern, count, Double::sum);
        }

        log.debug("Pattern merging completed: {} patterns remaining", patternMap.size());
    }

    private String postProcessPattern(String pattern) {
        if (Strings.isEmpty(pattern)) {
            return pattern;
        }

        // Replace repeated <*> with single <*> using compiled pattern
        pattern = REPEATED_WILDCARDS_PATTERN.matcher(pattern).replaceAll("<*>");

        // Merge repeated substrings
        // pattern = mergeRepeatedSubstrings(pattern);

        return pattern;
    }

    private String mergeRepeatedSubstrings(String input) {
        if (Strings.isEmpty(input)) {
            return input;
        }

        List<String> tokens = new ArrayList<>(Arrays.asList(input.split("\\s+")));
        if (tokens.size() <= 1) {
            return input;
        }

        List<String> result = new ArrayList<>();
        int i = 0;

        while (i < tokens.size()) {
            int maxSeqLen = 1;
            int maxRepeatCount = 1;
            int maxPossibleSeqLen = (tokens.size() - i) / 2;

            // Find the longest repeated sequence starting at position i
            for (int seqLen = 1; seqLen <= maxPossibleSeqLen; seqLen++) {
                int repeatCount = 1;

                while (true) {
                    int start1 = i;
                    int start2 = i + repeatCount * seqLen;

                    if (start2 + seqLen > tokens.size()) {
                        break;
                    }

                    boolean match = true;
                    for (int k = 0; k < seqLen; k++) {
                        if (!tokens.get(start1 + k).equals(tokens.get(start2 + k))) {
                            match = false;
                            break;
                        }
                    }

                    if (match) {
                        repeatCount++;
                    } else {
                        break;
                    }
                }

                if (repeatCount > 1 && repeatCount > maxRepeatCount) {
                    maxRepeatCount = repeatCount;
                    maxSeqLen = seqLen;
                }
            }

            // Add the sequence once (removing repetitions)
            for (int k = 0; k < maxSeqLen; k++) {
                result.add(tokens.get(i + k));
            }

            // Skip all repeated sequences
            i += maxSeqLen * maxRepeatCount;
        }

        return String.join(" ", result);
    }

    private void executePPL(String ppl, ActionListener<String> listener) {
        try {
            JSONObject jsonContent = new JSONObject(ImmutableMap.of("query", ppl));
            PPLQueryRequest pplQueryRequest = new PPLQueryRequest(ppl, jsonContent, null, "jdbc");
            TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest(pplQueryRequest);

            client
                .execute(
                    PPLQueryAction.INSTANCE,
                    transportPPLQueryRequest,
                    getPPLTransportActionListener(ActionListener.wrap(transportPPLQueryResponse -> {
                        String result = transportPPLQueryResponse.getResult();
                        if (Strings.isEmpty(result)) {
                            listener.onFailure(new RuntimeException("Empty PPL response"));
                        } else {
                            listener.onResponse(result);
                        }
                    }, error -> {
                        String errorMessage = String.format("PPL execution failed for query: %s, error: %s", ppl, error.getMessage());
                        log.error(errorMessage, error);
                        listener.onFailure(new RuntimeException(errorMessage, error));
                    }))
                );
        } catch (Exception e) {
            String errorMessage = String.format("Failed to execute PPL query: %s", ppl);
            log.error(errorMessage, e);
            listener.onFailure(new RuntimeException(errorMessage, e));
        }
    }

    /**
     * Clusters log vectors using hierarchical agglomerative clustering and returns representative centroids.
     *
     * @param logVectors Map of trace IDs to their corresponding log vectors
     * @return List of trace IDs representing the centroids of each cluster
     */
    /**
     * Cluster log vectors using a two-phase approach:
     * 1. K-means clustering to split large datasets into smaller groups (500-1000 data points each)
     * 2. Hierarchical clustering within each K-means cluster for fine-grained clustering
     * 
     * @param logVectors Map of trace IDs to their vector representations
     * @return List of trace IDs representing the centroids of each cluster
     */
    private List<String> clusterLogVectorsAndGetRepresentative(Map<String, double[]> logVectors) {
        if (logVectors.isEmpty()) {
            return new ArrayList<>();
        }

        log.debug("Starting two-phase clustering for {} log vectors", logVectors.size());

        // Convert map to arrays for processing
        double[][] vectors = new double[logVectors.size()][];
        Map<Integer, String> indexTraceIdMap = new HashMap<>();
        int i = 0;
        for (Map.Entry<String, double[]> entry : logVectors.entrySet()) {
            vectors[i] = entry.getValue();
            indexTraceIdMap.put(i, entry.getKey());
            i++;
        }

        List<String> finalCentroids = new ArrayList<>();

        // Phase 1: K-means clustering for large datasets
        if (logVectors.size() > 1000) {
            log.debug("Large dataset detected ({}), applying K-means pre-clustering", logVectors.size());

            // Calculate optimal number of K-means clusters (target 500-1000 points per cluster)
            int targetClusterSize = 500;
            int numKMeansClusters = (logVectors.size() + (targetClusterSize - 1)) / targetClusterSize;

            log.debug("Using {} K-means clusters for pre-clustering", numKMeansClusters);

            try {
                log.info("Starting performKMeansClustering");
                List<List<Integer>> kMeansClusters = performKMeansClustering(vectors, numKMeansClusters);
                log.info("Completing performKMeansClustering");

                // Phase 2: Apply hierarchical clustering within each K-means cluster
                for (int clusterIdx = 0; clusterIdx < kMeansClusters.size(); clusterIdx++) {
                    List<Integer> kMeansCluster = kMeansClusters.get(clusterIdx);
                    log.info("kMeansCluster " + kMeansCluster.size());

                    if (kMeansCluster.isEmpty()) {
                        continue;
                    }

                    if (kMeansCluster.size() == 1) {
                        // Single point cluster - add directly
                        finalCentroids.add(indexTraceIdMap.get(kMeansCluster.getFirst()));
                        continue;
                    }

                    if (kMeansCluster.size() > 500) {
                        log.info("the cluster size is greater than 500, perform partitioning");
                        List<String> clusterCentroids = performHierarchicalClusteringOfPartition(kMeansCluster, vectors,  indexTraceIdMap);
                        finalCentroids.addAll(clusterCentroids);
                        continue;
                    }

                    log.debug("Applying hierarchical clustering to K-means cluster {} with {} points", clusterIdx, kMeansCluster.size());

                    // Extract vectors for this K-means cluster
                    double[][] clusterVectors = new double[kMeansCluster.size()][];
                    Map<Integer, String> clusterIndexTraceIdMap = new HashMap<>();

                    for (int j = 0; j < kMeansCluster.size(); j++) {
                        int originalIndex = kMeansCluster.get(j);
                        clusterVectors[j] = vectors[originalIndex];
                        clusterIndexTraceIdMap.put(j, indexTraceIdMap.get(originalIndex));
                    }

                    // Apply hierarchical clustering within this K-means cluster
                    log.info("Starting performHierarchicalClustering");
                    List<String> clusterCentroids = performHierarchicalClustering(clusterVectors, clusterIndexTraceIdMap);

                    log.info("Completing performHierarchicalClustering");
                    finalCentroids.addAll(clusterCentroids);
                }

            } catch (Exception e) {
                log.warn("K-means clustering failed, falling back to hierarchical clustering only: {}", e.getMessage());
                // Fallback to hierarchical clustering only
                finalCentroids = performHierarchicalClustering(vectors, indexTraceIdMap);
            }

        } else {
            // Small dataset - use hierarchical clustering directly
            log.debug("Small dataset ({}), using hierarchical clustering only", logVectors.size());
            finalCentroids = performHierarchicalClustering(vectors, indexTraceIdMap);
        }

        log
            .debug(
                "Two-phase clustering completed: {} input vectors -> {} representative centroids",
                logVectors.size(),
                finalCentroids.size()
            );

        return finalCentroids;
    }

    /**
     * Perform K-means clustering using Apache Commons Math3
     * 
     * @param vectors Input vectors for clustering
     * @param numClusters Number of K-means clusters
     * @return List of clusters, each containing indices of points in that cluster
     */
    private List<List<Integer>> performKMeansClustering(double[][] vectors, int numClusters) {
        try {
            KMeansPlusPlusClusterer<DoublePoint> clusterer = new KMeansPlusPlusClusterer<>(numClusters, 300,
                    (DistanceMeasure) (a, b) -> 1- calculateCosineSimilarity(a, b));

            // Convert vectors to DoublePoint objects
            List<DoublePoint> points = new ArrayList<>();
            for (double[] vector : vectors) {
                points.add(new DoublePoint(vector));
            }

            // Perform K-means clustering
            List<CentroidCluster<DoublePoint>> clusters = clusterer.cluster(points);

            // Convert results back to our format
            List<List<Integer>> result = new ArrayList<>();
            for (CentroidCluster<DoublePoint> cluster : clusters) {
                List<Integer> clusterIndices = new ArrayList<>();
                for (DoublePoint point : cluster.getPoints()) {
                    // Find the original index of this point
                    for (int i = 0; i < vectors.length; i++) {
                        if (Arrays.equals(vectors[i], point.getPoint())) {
                            clusterIndices.add(i);
                            break;
                        }
                    }
                }
                if (!clusterIndices.isEmpty()) {
                    result.add(clusterIndices);
                }
            }

            return result;

        } catch (Exception e) {
            log.error("K-means clustering failed: {}", e.getMessage(), e);
            throw new RuntimeException("K-means clustering failed", e);
        }
    }

    /**
     * Perform hierarchical clustering on a subset of vectors
     * 
     * @param vectors Input vectors for clustering
     * @param indexTraceIdMap Mapping from vector index to trace ID
     * @return List of trace IDs representing cluster centroids
     */
    private List<String> performHierarchicalClustering(double[][] vectors, Map<Integer, String> indexTraceIdMap) {
        List<String> centroids = new ArrayList<>();

        if (vectors.length == 0) {
            return centroids;
        }

        if (vectors.length == 1) {
            centroids.add(indexTraceIdMap.get(0));
            return centroids;
        }

        try {
            HierarchicalAgglomerativeClustering hac = new HierarchicalAgglomerativeClustering(vectors);
            List<HierarchicalAgglomerativeClustering.ClusterNode> clusters = hac
                .fit(HierarchicalAgglomerativeClustering.LinkageMethod.COMPLETE, LOG_VECTORS_CLUSTERING_THRESHOLD);

            for (HierarchicalAgglomerativeClustering.ClusterNode cluster : clusters) {
                int centroidIndex = hac.getClusterCentroid(cluster);
                centroids.add(indexTraceIdMap.get(centroidIndex));
            }

        } catch (Exception e) {
            log.error("Hierarchical clustering failed: {}", e.getMessage(), e);
            // Fallback: return first point as representative
            centroids.add(indexTraceIdMap.get(0));
        }

        return centroids;
    }

    /**
     * If the first stage K-means clustering results exceed 500 clusters, implement batch processing and merge the results.
     * @param kMeansCluster Clustering results from the first stage.
     * @param vectors List of vectors by index.
     * @param indexTraceIdMap Map of index to their trace id.
     * @return
     */
    private List<String> performHierarchicalClusteringOfPartition(List<Integer> kMeansCluster, double[][] vectors, Map<Integer, String> indexTraceIdMap) {
        List<List<Integer>> partition = new ArrayList<>();
        int groupSize = 500;
        for (int j = 0; j < kMeansCluster.size(); j += groupSize) {
            int end = Math.min(j + groupSize, kMeansCluster.size());
            partition.add(new ArrayList<>(kMeansCluster.subList(j, end)));
        }
        log.info("Completing parting. {}", partition.size());
        List<double[]> vectorRes = new ArrayList<>();
        Map<Integer, String> index2Trace = new HashMap<>();
        for (List<Integer> partList: partition) {
            double[][] clusterVectors = new double[partList.size()][];
            Map<Integer, String> clusterIndexTraceIdMap = new HashMap<>();

            for (int j = 0; j < partList.size(); j++) {
                int originalIndex = partList.get(j);
                clusterVectors[j] = vectors[originalIndex];
                clusterIndexTraceIdMap.put(j, indexTraceIdMap.get(originalIndex));
            }

            log.info("Starting performHierarchicalClusteringOfPartition!");
            if (clusterVectors.length == 0) {
                continue;
            }

            if (clusterVectors.length == 1) {
                vectorRes.add(clusterVectors[0]);
                index2Trace.put(vectorRes.size() - 1, clusterIndexTraceIdMap.get(0));
                continue;
            }
            try {
                HierarchicalAgglomerativeClustering hac = new HierarchicalAgglomerativeClustering(clusterVectors);
                List<HierarchicalAgglomerativeClustering.ClusterNode> clusters = hac
                        .fit(HierarchicalAgglomerativeClustering.LinkageMethod.COMPLETE, LOG_VECTORS_CLUSTERING_THRESHOLD);
                log.info("Completing performHierarchicalClusteringOfPartition!");
                for (HierarchicalAgglomerativeClustering.ClusterNode cluster : clusters) {
                    int centroidIndex = hac.getClusterCentroid(cluster);
                    vectorRes.add(clusterVectors[centroidIndex]);
                    index2Trace.put(vectorRes.size() - 1, clusterIndexTraceIdMap.get(centroidIndex));
                }
            } catch (Exception e) {
                log.error("Hierarchical clustering failed: {}", e.getMessage(), e);
                // Fallback: return first point as representative
                vectorRes.add(clusterVectors[0]);
                index2Trace.put(vectorRes.size() - 1, clusterIndexTraceIdMap.get(0));
            }
        }
        return removeSimilarVectors(vectorRes, index2Trace);
    }

    /**
     * Compute the cosine distance pairwise and return the corresponding trace.
     * @param vectorRes List of vectors.
     * @param index2Trace Map of index to their trace id.
     * @return
     */
    private List<String> removeSimilarVectors(List<double[]> vectorRes, Map<Integer, String> index2Trace) {
        Set<Integer> toRemove = new HashSet<>();

        for (int i = 0; i < vectorRes.size(); i++) {
            if (toRemove.contains(i)) continue;

            for (int j = i + 1; j < vectorRes.size(); j++) {
                if (toRemove.contains(j)) continue;

                double distance =calculateCosineSimilarity(vectorRes.get(i), vectorRes.get(j));
                if (distance < LOG_VECTORS_CLUSTERING_THRESHOLD) {
                    toRemove.add(j);
                }
            }
        }
        List<String> result = new ArrayList<>();
        for (int i = 0; i < vectorRes.size(); i++) {
            if (!toRemove.contains(i)) {
                result.add(index2Trace.get(i));
            }
        }
        return result;
    }


    public static class Factory implements Tool.Factory<LogPatternAnalysisTool> {
        private Client client;

        private static LogPatternAnalysisTool.Factory INSTANCE;

        /**
         * Create or return the singleton factory instance
         */
        public static LogPatternAnalysisTool.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (LogPatternAnalysisTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new LogPatternAnalysisTool.Factory();
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
        public LogPatternAnalysisTool create(Map<String, Object> map) {

            return new LogPatternAnalysisTool(client);
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
        public String getDefaultVersion() {
            return null;
        }
    }
}
