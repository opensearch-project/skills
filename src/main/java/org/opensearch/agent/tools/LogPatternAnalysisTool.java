/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.tools.utils.ToolHelper.getPPLTransportActionListener;
import static org.opensearch.agent.tools.utils.clustering.HierarchicalAgglomerativeClustering.calculateCosineSimilarity;
import static org.opensearch.ml.common.CommonValue.TOOL_INPUT_SCHEMA_FIELD;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.json.JSONObject;
import org.opensearch.agent.tools.utils.clustering.ClusteringHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.utils.ToolUtils;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

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
 *           "result": """{"EXCEPTIONAL": {"traceId": "sequence"}}"""
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
    public static final String STRICT_FIELD = "strict";

    // Constants
    private static final String DEFAULT_DESCRIPTION =
        "This is a tool used to detect selection log patterns by the patterns command in PPL or to detect selection log sequences by the log clustering algorithm.";
    private static final double LOG_VECTORS_CLUSTERING_THRESHOLD = 0.5;
    private static final double LOG_PATTERN_THRESHOLD = 0.75;
    private static final double LOG_PATTERN_LIFT = 3;
    private static final String DEFAULT_TIME_FIELD = "@timestamp";

    public static final String DEFAULT_INPUT_SCHEMA =
        """
            {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Target OpenSearch index name containing log data (e.g., 'ss4o_logs-otel-2025.06.24')"
                    },
                    "timeField": {
                        "type": "string",
                        "description": "Date/time field in the index mapping used for time-based filtering (ISO 8601 format expected)"
                    },
                    "logFieldName": {
                        "type": "string",
                        "description": "Field containing raw log messages to analyze (e.g., 'body', 'message', 'log')"
                    },
                    "traceFieldName": {
                        "type": "string",
                        "description": "[OPTIONAL] Field for trace/correlation ID to enable sequence analysis (e.g., 'traceId', 'correlationId'). Leave empty for pattern-only analysis."
                    },
                    "baseTimeRangeStart": {
                        "type": "string",
                        "description": "Start time for baseline comparison period (ISO 8601 format, e.g., '2025-06-24T07:33:05Z')"
                    },
                    "baseTimeRangeEnd": {
                        "type": "string",
                        "description": "End time for baseline comparison period (ISO 8601 format, e.g., '2025-06-24T07:51:27Z')"
                    },
                    "selectionTimeRangeStart": {
                        "type": "string",
                        "description": "Start time for analysis target period (ISO 8601 format, e.g., '2025-06-24T07:50:26.999Z')"
                    },
                    "selectionTimeRangeEnd": {
                        "type": "string",
                        "description": "End time for analysis target period (ISO 8601 format, e.g., '2025-06-24T07:55:56Z')"
                    }
                },
                "required": [
                    "index",
                    "timeField",
                    "logFieldName",
                    "selectionTimeRangeStart",
                    "selectionTimeRangeEnd"
                ],
                "additionalProperties": false
            }
            """;

    public static final Map<String, Object> DEFAULT_ATTRIBUTES = Map.of(TOOL_INPUT_SCHEMA_FIELD, DEFAULT_INPUT_SCHEMA, STRICT_FIELD, false);

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
        Map<String, Double> patternWeightsMap) {
    }

    private record PatternDiffResult(String pattern, Double base, Double selection, Double lift) {
    }

    Comparator<PatternDiffResult> comparator = (d1, d2) -> {
        Double lift1 = Optional.ofNullable(d1.lift).orElse(Double.MIN_VALUE);
        Double lift2 = Optional.ofNullable(d2.lift).orElse(Double.MIN_VALUE);

        if (lift1.compareTo(lift2) == 0) {
            return Optional
                .ofNullable(d2.selection)
                .orElse(Double.MIN_VALUE)
                .compareTo(Optional.ofNullable(d1.selection).orElse(Double.MIN_VALUE));
        } else {
            return lift2.compareTo(lift1);
        }
    };

    private record PatternWithSamples(String pattern, double count, List<?> sampleLogs) {
    }

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
    private ClusteringHelper clusteringHelper;

    public LogPatternAnalysisTool(Client client) {
        this.client = client;
        this.clusteringHelper = new ClusteringHelper(LOG_VECTORS_CLUSTERING_THRESHOLD);
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
    public <T> void run(Map<String, String> originalParameters, ActionListener<T> listener) {
        try {
            Map<String, String> parameters = ToolUtils.extractInputParameters(originalParameters, DEFAULT_ATTRIBUTES);
            log.info("Starting log pattern analysis with parameters: {}", parameters.keySet());
            AnalysisParameters params = new AnalysisParameters(parameters);

            if (params.hasTraceField() && params.hasBaseTime()) {
                log.info("Performing log sequence analysis for index: {}", params.index);
                logSequenceAnalysis(params, listener);
            } else if (params.hasBaseTime()) {
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
        // Step 1: Analyze base time range
        analyzeBaseTimeRange(params, ActionListener.wrap(baseResult -> {
            log.info("Base time range analysis completed, found {} traces", baseResult.tracePatternMap.size());

            // Step 2: Analyze selection time range
            analyzeSelectionTimeRange(params, ActionListener.wrap(selectionResult -> {
                log.info("Selection time range analysis completed, found {} traces", selectionResult.tracePatternMap.size());

                // Step 3: Generate comparison result
                generateSequenceComparisonResult(baseResult, selectionResult, listener);
            }, listener::onFailure));
        }, this::handlePPLError));
    }

    private void analyzeBaseTimeRange(AnalysisParameters params, ActionListener<PatternAnalysisResult> listener) {
        String baseTimeRangeLogPatternPPL = buildLogPatternPPL(
            params.index,
            params.timeField,
            params.logFieldName,
            params.traceFieldName,
            params.baseTimeRangeStart,
            params.baseTimeRangeEnd
        );

        executePPL(baseTimeRangeLogPatternPPL, listener);
    }

    private void analyzeSelectionTimeRange(AnalysisParameters params, ActionListener<PatternAnalysisResult> listener) {
        String selectionTimeRangeLogPatternPPL = buildLogPatternPPL(
            params.index,
            params.timeField,
            params.logFieldName,
            params.traceFieldName,
            params.selectionTimeRangeStart,
            params.selectionTimeRangeEnd
        );

        executePPL(selectionTimeRangeLogPatternPPL, listener);
    }

    private void executePPL(String ppl, ActionListener<PatternAnalysisResult> listener) {
        Function<List<List<Object>>, PatternAnalysisResult> rowParser = dataRows -> {
            Map<String, Set<String>> tracePatternMap = new HashMap<>();
            Map<String, Set<String>> patternCountMap = new HashMap<>();
            Map<String, String> rawPatternCache = new HashMap<>();

            for (List<Object> row : dataRows) {
                if (row.size() < 2) {
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

            return new PatternAnalysisResult(tracePatternMap, patternCountMap, patternVectors);
        };

        executePPLAndParseResult(ppl, rowParser, listener);
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
                Locale.ROOT,
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

            // Step 4: Build vectors for base time range
            Map<String, double[]> baseVectorMap = buildVectorMap(
                baseResult.tracePatternMap,
                baseResult.patternWeightsMap,
                patternIndexMap,
                false
            );

            // Step 5: Cluster base vectors and find centroids
            List<String> baseRepresentative = this.clusteringHelper.clusterLogVectorsAndGetRepresentative(baseVectorMap);

            // Step 6: Build vectors for traceNeedToExamine time range
            Map<String, double[]> selectionVectorMap = buildVectorMap(
                selectionResult.tracePatternMap,
                selectionResult.patternWeightsMap,
                patternIndexMap,
                true,
                baseResult.patternCountMap,
                selectionResult.patternCountMap
            );

            // Step 7: Find traceNeedToExamine centroids
            List<String> selectionRepresentative = this.clusteringHelper.clusterLogVectorsAndGetRepresentative(selectionVectorMap);

            List<String> traceNeedToExamine = filterSelectionCentroids(
                baseRepresentative,
                selectionRepresentative,
                baseVectorMap,
                selectionVectorMap
            );

            log
                .info(
                    "Identified {} traceNeedToExamine centroids from {} candidates",
                    traceNeedToExamine.size(),
                    selectionRepresentative.size()
                );

            // Generate final result
            Map<String, Map<String, String>> result = buildFinalResult(
                baseRepresentative,
                traceNeedToExamine,
                baseResult.tracePatternMap,
                selectionResult.tracePatternMap
            );
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
        log.debug("vector dimension is {}", sortedPatterns.size());

        // pattern and its index in a vector
        Map<String, Integer> patternIndexMap = new HashMap<>();
        for (int i = 0; i < sortedPatterns.size(); i++) {
            patternIndexMap.put(sortedPatterns.get(i), i);
        }

        return patternIndexMap;
    }

    @SafeVarargs
    private Map<String, double[]> buildVectorMap(
        Map<String, Set<String>> tracePatternMap,
        Map<String, Double> patternWeightsMap,
        Map<String, Integer> patternIndexMap,
        boolean isSelection,
        Map<String, Set<String>>... additionalPatternMaps
    ) {
        Map<String, double[]> vectorMap = new HashMap<>();
        int dimension = patternIndexMap.size();

        for (Map.Entry<String, Set<String>> entry : tracePatternMap.entrySet()) {
            String traceId = entry.getKey();
            Set<String> patterns = entry.getValue();
            double[] vector = new double[dimension];

            for (String pattern : patterns) {
                Integer index = patternIndexMap.get(pattern);
                if (index != null) {
                    double baseValue = 0.5 * patternWeightsMap.getOrDefault(pattern, 0.0);

                    if (isSelection && additionalPatternMaps.length >= 2) {
                        // Add existence weight for selection patterns
                        Map<String, Set<String>> basePatterns = additionalPatternMaps[0];

                        int existenceWeight = basePatterns.containsKey(pattern) ? 0 : 1;
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
        executePPLAndParseResult(baseTimeRangeLogPatternPPL, dataRowsParser, ActionListener.wrap(basePatterns -> {
            try {
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
                executePPLAndParseResult(selectionTimeRangeLogPatternPPL, dataRowsParser, ActionListener.wrap(selectionPatterns -> {
                    try {
                        mergeSimilarPatterns(selectionPatterns);

                        log.debug("Selection patterns processed: {} patterns", selectionPatterns.size());

                        // Step 3: Calculate pattern differences
                        List<PatternDiffResult> patternDifferences = calculatePatternDifferences(basePatterns, selectionPatterns);

                        // Step 4: Sort the difference and get top 10
                        List<PatternDiffResult> topDiffs = Stream
                            .concat(
                                patternDifferences.stream().filter(diff -> !Objects.isNull(diff.lift)).sorted(comparator).limit(10),
                                patternDifferences.stream().filter(diff -> Objects.isNull(diff.lift)).sorted(comparator).limit(10)
                            )
                            .collect(Collectors.toList());

                        Map<String, Object> finalResult = new HashMap<>();
                        finalResult.put("patternMapDifference", topDiffs);

                        log.info("Pattern analysis completed: {} differences found", patternDifferences.size());
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
        Set<String> errorKeywords = Set
            .of(
                "error",
                "err",
                "exception",
                "failed",
                "failure",
                "timeout",
                "panic",
                "fatal",
                "critical",
                "severe",
                "abort",
                "aborted",
                "aborting",
                "crash",
                "crashed",
                "broken",
                "corrupt",
                "corrupted",
                "invalid",
                "malformed",
                "unprocessable",
                "denied",
                "forbidden",
                "unauthorized",
                "conflict",
                "deadlock",
                "overflow",
                "underflow",
                "throttled",
                "disk_full",
                "insufficient",
                "retrying",
                "backpressure",
                "degraded",
                "unexpected",
                "unusual",
                "missing",
                "stale",
                "expired",
                "mismatch",
                "violation"
            );

        String selectionTimeRangeLogPatternPPL = String
            .format(
                Locale.ROOT,
                "source=%s | where %s>'%s' and %s<'%s' | where match(%s, '%s') | patterns %s method=brain "
                    + "mode=aggregation max_sample_count=2"
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

        executePPLAndParseResult(selectionTimeRangeLogPatternPPL, dataRowsParser, ActionListener.wrap(logInsights -> {
            try {
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
                Locale.ROOT,
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

    private List<PatternDiffResult> calculatePatternDifferences(Map<String, Double> basePatterns, Map<String, Double> selectionPatterns) {
        List<PatternDiffResult> differences = new ArrayList<>();

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
                    differences.add(new PatternDiffResult(pattern, baseCount / baseTotal, selectionCount / selectionTotal, lift));
                }
            } else {
                // Pattern only exists in selection time range
                differences.add(new PatternDiffResult(pattern, 0.0, selectionCount / selectionTotal, null));
                log.debug("New selection pattern detected: {} (count: {})", pattern, selectionCount);
            }
        }

        return differences;
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
        union.addAll(set2);

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
        return pattern;
    }

    private <T> void executePPLAndParseResult(String ppl, Function<List<List<Object>>, T> rowParser, ActionListener<T> listener) {
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
                            Map<String, Object> pplResult = gson.fromJson(result, new TypeToken<Map<String, Object>>() {
                            }.getType());

                            Object datarowsObj = pplResult.get("datarows");
                            if (!(datarowsObj instanceof List)) {
                                throw new IllegalStateException("Invalid PPL response format: missing or invalid datarows");
                            }

                            @SuppressWarnings("unchecked")
                            List<List<Object>> dataRows = (List<List<Object>>) datarowsObj;
                            listener.onResponse(rowParser.apply(dataRows));
                        }
                    }, error -> {
                        String errorMessage = String.format(Locale.ROOT, "PPL execution failed for error: %s", error.getMessage());
                        listener.onFailure(new RuntimeException(errorMessage, error));
                    }))
                );
        } catch (Exception e) {
            String errorMessage = String.format(Locale.ROOT, "Failed to execute PPL query: %s", ppl);
            log.error(errorMessage, e);
            listener.onFailure(new RuntimeException(errorMessage, e));
        }
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
        public Map<String, Object> getDefaultAttributes() {
            return DEFAULT_ATTRIBUTES;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }
    }
}
