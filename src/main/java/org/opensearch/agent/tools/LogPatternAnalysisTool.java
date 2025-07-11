/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.tools.utils.ToolHelper.getPPLTransportActionListener;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.json.JSONObject;
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
 *     "normalTimeRangeStart": "2025-06-24T07:33:05Z",
 *     "normalTimeRangeEnd": "2025-06-24T07:51:27Z",
 *     "abnormalTimeRangeStart": "2025-06-24T07:50:26.999999999Z",
 *     "abnormalTimeRangeEnd": "2025-06-24T07:55:56Z"
 *   }
 * }
 * 3. Result: a list of abnormal traceId
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
    // the default description of this tool
    private static final String DEFAULT_DESCRIPTION =
        "This is a tool used to detect abnormal log patterns by the patterns command in PPL or to detect abnormal log sequences by the log clustering algorithm.";
    private static final double LOG_VECTORS_CLUSTERING_THRESHOLD = 0.6;
    private static final double LOG_PATTERN_THRESHOLD = 0.75;
    @Setter
    @Getter
    private String name = TYPE;
    // the description of this tool
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;

    // the version of this tool
    @Getter
    private String version;

    // the OpenSearch transport client
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
        return true;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        String index = parameters.getOrDefault("index", "");
        String timeFiled = parameters.getOrDefault("timeFiled", "");
        String logMessageFieldName = parameters.getOrDefault("logMessageFieldName", "");
        String traceFieldName = parameters.getOrDefault("traceFieldName", "");
        String normalTimeRangeStart = parameters.getOrDefault("normalTimeRangeStart", "");
        String normalTimeRangeEnd = parameters.getOrDefault("normalTimeRangeEnd", "");
        String abnormalTimeRangeStart = parameters.getOrDefault("abnormalTimeRangeStart", "");
        String abnormalTimeRangeEnd = parameters.getOrDefault("abnormalTimeRangeEnd", "");

        if (Strings.isEmpty(index)
            || Strings.isEmpty(timeFiled)
            || Strings.isEmpty(logMessageFieldName)
            || Strings.isEmpty(normalTimeRangeStart)
            || Strings.isEmpty(normalTimeRangeEnd)
            || Strings.isEmpty(abnormalTimeRangeStart)
            || Strings.isEmpty(abnormalTimeRangeEnd)) {
            listener
                .onFailure(
                    new IllegalArgumentException(
                        "Invalid parameters, please check the parameters of LogPatternAnalysisTool,"
                            + " index|timeFiled|logMessageFieldName|normalTimeRangeStart|normalTimeRangeEnd"
                            + "|abnormalTimeRangeStart|abnormalTimeRangeEnd are required!"
                    )
                );
            return;
        }

        // log pattern analysis
        if (Strings.isEmpty(traceFieldName)) {
            logPatternAnalysis(parameters, listener);
            return;
        }

        logSequenceAnalysis(parameters, listener);
    }

    private <T> void logSequenceAnalysis(Map<String, String> parameters, ActionListener<T> listener) {
        String index = parameters.getOrDefault("index", "");
        String timeFiled = parameters.getOrDefault("timeFiled", "");
        String logMessageFieldName = parameters.getOrDefault("logMessageFieldName", "");
        String traceFieldName = parameters.getOrDefault("traceFieldName", "");
        String normalTimeRangeStart = parameters.getOrDefault("normalTimeRangeStart", "");
        String normalTimeRangeEnd = parameters.getOrDefault("normalTimeRangeEnd", "");
        String abnormalTimeRangeStart = parameters.getOrDefault("abnormalTimeRangeStart", "");
        String abnormalTimeRangeEnd = parameters.getOrDefault("abnormalTimeRangeEnd", "");

        // Step 1. generate log patterns for normal time range
        String normalTimeRangeLogPatternPPL = ("source=%s | where %s!='' | where %s >'%s' and %s <'%s' | patterns "
            + "%s method=brain "
            + "variable_count_threshold=3 | fields %s, patterns_field, %s | sort %s")
            .formatted(
                index,
                traceFieldName,
                timeFiled,
                timeFiled,
                normalTimeRangeStart,
                normalTimeRangeEnd,
                logMessageFieldName,
                traceFieldName,
                timeFiled,
                timeFiled
            );
        // log ppl
        log.info("normalTimeRangeLogPatternPPL: {}", normalTimeRangeLogPatternPPL);
        executePPL(normalTimeRangeLogPatternPPL, ActionListener.wrap(normalTimeRangeResult -> {
            Map<String, Object> normalTimeRangePPLResult = gson.fromJson(normalTimeRangeResult, new TypeToken<Map<String, Object>>() {
            }.getType());
            List<List<Object>> normalTimeRangeDataRows = (List<List<Object>>) normalTimeRangePPLResult
                .getOrDefault("datarows", new ArrayList<>());
            // map traceId to its patterns
            Map<String, Set<String>> normalTimeRangeTraceIdPatternMap = new HashMap<>();
            // map pattern to its trace ids
            Map<String, Set<String>> normalTimeRangePatternCountMap = new HashMap<>();
            // map pattern to weight
            Map<String, Double> normalTimeRangePatternValue = new HashMap<>();

            // used for cache
            Map<String, String> rawPatternToSimplifiedPatternMap = new HashMap<>();
            for (List<Object> row : normalTimeRangeDataRows) {
                String traceId = (String) row.get(0);
                String rawPattern = (String) row.get(1);

                String simplifiedPattern = rawPatternToSimplifiedPatternMap.computeIfAbsent(rawPattern, this::postProcessPattern);

                normalTimeRangeTraceIdPatternMap.compute(traceId, (k, v) -> {
                    Set<String> patterns = v == null ? new LinkedHashSet<>() : v;
                    patterns.add(simplifiedPattern);
                    return patterns;
                });

                normalTimeRangePatternCountMap.compute(simplifiedPattern, (k, v) -> {
                    Set<String> traceIds = v == null ? new HashSet<>() : v;
                    traceIds.add(traceId);
                    return traceIds;
                });
            }

            int normalTimeRangeTraceCount = normalTimeRangeTraceIdPatternMap.size();
            for (Map.Entry<String, Set<String>> entry : normalTimeRangePatternCountMap.entrySet()) {
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    // IDF
                    double value = Math.log((double) normalTimeRangeTraceCount / entry.getValue().size());
                    // sigmoid
                    value = 1 / (1 + Math.exp(-value));
                    normalTimeRangePatternValue.put(entry.getKey(), value);
                } else {
                    normalTimeRangePatternValue.put(entry.getKey(), 0.0);
                }
            }

            // Step 2. generate log patterns for abnormal time range
            String abnormalTimeRangeLogPatternPPL = ("source=%s | where %s!='' | where %s>'%s' and %s <'%s' | "
                + "patterns %s method=brain "
                + "variable_count_threshold=3 | fields %s, patterns_field, %s | "
                + "sort %s")
                .formatted(
                    index,
                    traceFieldName,
                    timeFiled,
                    timeFiled,
                    abnormalTimeRangeStart,
                    abnormalTimeRangeEnd,
                    logMessageFieldName,
                    traceFieldName,
                    timeFiled,
                    timeFiled
                );
            // log abnormal ppl
            log.info("abnormalTimeRangeLogPatternPPL:{}", abnormalTimeRangeLogPatternPPL);

            Map<String, Set<String>> abnormalTimeRangeTraceIdPatternMap = new HashMap<>();
            Map<String, Set<String>> abnormalTimeRangePatternCountMap = new HashMap<>();
            Map<String, Double> abnormalTimeRangePatternValue = new HashMap<>();
            executePPL(abnormalTimeRangeLogPatternPPL, ActionListener.wrap(abnormalTimeRangeResult -> {
                Map<String, Object> abnormalTimeRangePPLResult = gson
                    .fromJson(abnormalTimeRangeResult, new TypeToken<Map<String, Object>>() {
                    }.getType());
                List<List<Object>> abnormalTimeRangeDataRows = (List<List<Object>>) abnormalTimeRangePPLResult
                    .getOrDefault("datarows", new ArrayList<>());
                for (List<Object> row : abnormalTimeRangeDataRows) {
                    String traceId = (String) row.get(0);
                    String rawPattern = (String) row.get(1);

                    String simplifiedPattern;
                    if (!rawPatternToSimplifiedPatternMap.containsKey(rawPattern)) {
                        simplifiedPattern = postProcessPattern(rawPattern);
                        // cache the raw pattern to simplified pattern map
                        rawPatternToSimplifiedPatternMap.put(rawPattern, simplifiedPattern);
                    } else {
                        simplifiedPattern = rawPatternToSimplifiedPatternMap.get(rawPattern);
                    }

                    abnormalTimeRangeTraceIdPatternMap.compute(traceId, (k, v) -> {
                        Set<String> patterns = v == null ? new LinkedHashSet<>() : v;
                        patterns.add(simplifiedPattern);
                        return patterns;
                    });

                    abnormalTimeRangePatternCountMap.compute(simplifiedPattern, (k, v) -> {
                        Set<String> traceIds = v == null ? new HashSet<>() : v;
                        traceIds.add(traceId);
                        return traceIds;
                    });

                }

                int abnormalTimeRangeTraceCount = abnormalTimeRangeTraceIdPatternMap.size();
                for (Map.Entry<String, Set<String>> entry : abnormalTimeRangePatternCountMap.entrySet()) {
                    if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                        double value = Math.log((double) abnormalTimeRangeTraceCount / entry.getValue().size());
                        value = 1 / (1 + Math.exp(-value));
                        abnormalTimeRangePatternValue.put(entry.getKey(), value);
                    } else {
                        abnormalTimeRangePatternValue.put(entry.getKey(), 0.0);
                    }
                }

                // Step 3. construct pattern index, take all patterns into consideration, and associate each pattern with a number as the
                // index in the vector
                Map<String, Integer> patternIndexMap = new HashMap<>();
                Set<String> patternSet = new HashSet<>(normalTimeRangePatternCountMap.keySet());
                patternSet.addAll(abnormalTimeRangePatternCountMap.keySet());
                List<String> patternList = new ArrayList<>(patternSet);
                Collections.sort(patternList);
                for (int i = 0; i < patternList.size(); i++) {
                    patternIndexMap.put(patternList.get(i), i);
                }

                log.info("pattern index:");
                for (Map.Entry<String, Integer> entry : patternIndexMap.entrySet()) {
                    log.debug(entry.getKey() + ": " + entry.getValue());
                }

                // Step 4. build vectors for normal time range
                Map<String, double[]> normalTimeRangeVectorMap = new HashMap<>();
                for (Map.Entry<String, Set<String>> entry : normalTimeRangeTraceIdPatternMap.entrySet()) {
                    double[] vector = new double[patternList.size()];
                    for (String pattern : entry.getValue()) {
                        vector[patternIndexMap.get(pattern)] = 0.5 * normalTimeRangePatternValue.get(pattern);
                    }
                    normalTimeRangeVectorMap.put(entry.getKey(), vector);
                }

                log.info("normalTimeRangeVectorMap:");
                for (Map.Entry<String, double[]> entry : normalTimeRangeVectorMap.entrySet()) {
                    log.debug(entry.getKey() + ": " + Arrays.toString(entry.getValue()));
                }

                // Step 5. cluster log vectors and find centroids for normal time range
                List<List<String>> normalClusters = clusterLogVectors(normalTimeRangeVectorMap);
                List<String> centroids = findCentroids(normalTimeRangeVectorMap, normalClusters);

                log.info("centroids:");
                log.info(centroids);

                Map<String, String> normalLogSequencesMap = new HashMap<>();
                for (String centroid : centroids) {
                    Set<String> set = normalTimeRangeTraceIdPatternMap.get(centroid);
                    normalLogSequencesMap.put(centroid, String.join(" -> ", set));
                }

                // Step 6. build vectors for abnormal time range
                Map<String, double[]> abnormalTimeRangeVectorMap = new HashMap<>();
                for (Map.Entry<String, Set<String>> entry : abnormalTimeRangeTraceIdPatternMap.entrySet()) {
                    double[] vector = new double[patternList.size()];
                    for (String pattern : entry.getValue()) {
                        int existenceWeight = abnormalTimeRangePatternCountMap.containsKey(pattern)
                            && !normalTimeRangePatternCountMap.containsKey(pattern) ? 1 : 0;
                        vector[patternIndexMap.get(pattern)] = 0.5 * abnormalTimeRangePatternValue.get(pattern) + 0.5 * existenceWeight;
                    }
                    abnormalTimeRangeVectorMap.put(entry.getKey(), vector);
                }

                log.info("abnormalTimeRangeVectorMap:");
                for (Map.Entry<String, double[]> entry : abnormalTimeRangeVectorMap.entrySet()) {
                    log.debug(entry.getKey() + ": " + Arrays.toString(entry.getValue()));
                }

                List<List<String>> abnormalCluster = clusterLogVectors(abnormalTimeRangeVectorMap);
                log.info("abnormalCluster: size={}", abnormalCluster.size());
                List<String> centriodsForSelection = findCentroids(abnormalTimeRangeVectorMap, abnormalCluster);

                List<String> abnormnalCentroids = new ArrayList<>();

                for (String selection : centriodsForSelection) {
                    boolean abnormal = true;
                    for (String centroid : centroids) {
                        double[] vector1 = normalTimeRangeVectorMap.get(centroid);
                        double[] vector2 = abnormalTimeRangeVectorMap.get(selection);
                        if (cosineSimilarity(vector1, vector2) > LOG_VECTORS_CLUSTERING_THRESHOLD) {
                            abnormal = false;
                            break;
                        }
                    }
                    if (abnormal) {
                        abnormnalCentroids.add(selection);
                    }
                }

                log.info("abnormalClustersCentroids:");
                log.info(abnormnalCentroids);

                // get actual log sequence
                Map<String, String> abnormalLogSequenceMap = new HashMap<>();
                for (String traceId : abnormnalCentroids) {
                    Set<String> set = abnormalTimeRangeTraceIdPatternMap.get(traceId);
                    abnormalLogSequenceMap.put(traceId, String.join(" -> ", set));
                }
                Map<String, Map<String, String>> result = new HashMap<>();
                result.put("NORMAL", normalLogSequencesMap);
                result.put("EXCEPTIONAL", abnormalLogSequenceMap);
                listener.onResponse((T) gson.toJson(result));

            }, listener::onFailure));
        }, e -> {
            log.error("failed to execute ppl: " + e);
            if (e.toString().contains("IndexNotFoundException")) {
                listener.onFailure(new IllegalArgumentException(e));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private <T> void logPatternAnalysis(Map<String, String> parameters, ActionListener<T> listener) {
        String index = parameters.getOrDefault("index", "");
        String logFieldName = parameters.getOrDefault("logFieldName", "");
        String normalTimeRangeStart = parameters.getOrDefault("normalTimeRangeStart", "");
        String normalTimeRangeEnd = parameters.getOrDefault("normalTimeRangeEnd", "");
        String abnormalTimeRangeStart = parameters.getOrDefault("abnormalTimeRangeStart", "");
        String abnormalTimeRangeEnd = parameters.getOrDefault("abnormalTimeRangeEnd", "");

        // Step 1. generate log patterns for normal time range
        String normalTimeRangeLogPatternPPL = ("source=%s | where time>'%s' and time <'%s' | patterns %s method=brain "
            + "variable_count_threshold=3 | fields patterns_field | stats "
            + "count() as cnt by patterns_field").formatted(index, normalTimeRangeStart, normalTimeRangeEnd, logFieldName);

        executePPL(normalTimeRangeLogPatternPPL, ActionListener.wrap((response) -> {
            final Map<String, Double> patternMapBase = parseLogPatterns(response);

            mergeSimilarPatterns(patternMapBase);

            double baseTotal = patternMapBase.values().stream().reduce(0d, Double::sum);

            String selectionTimeRangeLogPatternPPL = ("source=%s | where time>'%s' and time <'%s' | patterns %s "
                + "method=brain "
                + "variable_count_threshold=3 | fields patterns_field | stats "
                + "count() as cnt by patterns_field").formatted(index, abnormalTimeRangeStart, abnormalTimeRangeEnd, logFieldName);

            executePPL(selectionTimeRangeLogPatternPPL, ActionListener.wrap((selectionResponse) -> {
                final Map<String, Double> patternMapSelection = parseLogPatterns(selectionResponse);
                mergeSimilarPatterns(patternMapSelection);

                double total = patternMapSelection.values().stream().reduce(0d, Double::sum);
                // calculate the difference between the two maps
                Map<String, Double> patternMapDifference = new HashMap<>();

                for (Map.Entry<String, Double> entry : patternMapSelection.entrySet()) {
                    String pattern = entry.getKey();
                    if (patternMapBase.containsKey(pattern)) {
                        Double cnt = patternMapBase.get(pattern);
                        Double selectionCnt = patternMapSelection.get(pattern);
                        double lift = (selectionCnt / total) / (cnt / baseTotal);
                        if (lift < 1) {
                            lift = 1 / lift;
                        }
                        if (lift > 2) {
                            log.info("pattern: {}, cnt: {}, selectionCnt: {}, lift: {}", pattern, cnt, selectionCnt, lift);
                            // patternMapDifference.put(pattern, "base: %s, selection: %s, lift: %s".formatted(cnt,
                            // selectionCnt, lift));
                        }
                    } else {
                        patternMapDifference.put(pattern, entry.getValue());
                    }

                }

                Map<String, Object> finalResult = new HashMap<>();
                finalResult.put("patternMapDifference", patternMapDifference);
                finalResult.put("patternMapSelection", patternMapSelection);
                finalResult.put("patternMapBase", patternMapBase);
                listener.onResponse((T) gson.toJson(finalResult));

            }, e -> { listener.onFailure(e); }));

        }, e -> { listener.onFailure(e); }));
    }

    private Map<String, Double> parseLogPatterns(String response) {
        Map<String, Object> normalTimeRangePPLResult = gson.fromJson(response, new TypeToken<Map<String, Object>>() {
        }.getType());
        List<List<Object>> normalTimeRangeDataRows = (List<List<Object>>) normalTimeRangePPLResult
            .getOrDefault("datarows", new ArrayList<>());

        return normalTimeRangeDataRows
            .stream()
            .collect(HashMap::new, (map, row) -> map.put((String) row.get(1), (Double) row.get(0)), HashMap::putAll);
    }

    private boolean isSimilar(String pattern, Collection<String> baselinePatterns) {
        for (String basePattern : baselinePatterns) {
            if (jacCardSimilarity(pattern, basePattern) > LOG_PATTERN_THRESHOLD) {
                return true;
            }
        }
        return false;
    }

    private double jacCardSimilarity(String pattern1, String pattern2) {
        Set<String> set1 = new HashSet<>(Arrays.asList(pattern1.split("\\s+")));
        Set<String> set2 = new HashSet<>(Arrays.asList(pattern2.split("\\s+")));

        // calculate intersection and union of set1 and set2
        Set<String> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);

        Set<String> union = new HashSet<>(set1);
        set1.addAll(set2);

        return intersection.size() * 1.0d / union.size();
    }

    private void mergeSimilarPatterns(Map<String, Double> patternMap) {
        List<String> patterns = new ArrayList<>(patternMap.keySet());
        patterns.sort(String::compareTo);
        List<String> removed = new ArrayList<>();
        for (int i = 0; i < patterns.size(); i++) {
            if (removed.contains(patterns.get(i))) {
                continue;
            }

            for (int j = i + 1; j < patterns.size(); j++) {
                String pattern1 = patterns.get(i);
                String pattern2 = patterns.get(j);
                if (jacCardSimilarity(pattern1, pattern2) > LOG_PATTERN_THRESHOLD) {
                    // merge pattern2 into pattern1
                    patternMap.put(pattern1, patternMap.getOrDefault(pattern1, 0d) + patternMap.getOrDefault(pattern2, 0d));
                    removed.add(pattern2);
                    patternMap.remove(pattern2);
                }
            }
        }

        // loop patternMap and merge patterns with similar patterns
        Map<String, String> toReplaced = patternMap.entrySet().stream().filter(entry -> {
            String newPattern = postProcessPattern(entry.getKey());
            return !newPattern.equals(entry.getKey());
        }).collect(Collectors.toMap(Map.Entry::getKey, entry -> postProcessPattern(entry.getKey())));

        toReplaced.forEach((key, newPattern) -> {
            double cnt = patternMap.remove(key);
            patternMap.merge(newPattern, cnt, Double::sum);
        });
    }

    private String postProcessPattern(String pattern) {
        if (pattern.endsWith("]")) {
            pattern = pattern.substring(0, pattern.length() - 1);
        }
        // replace all the repeated <*> with only one <*>
        pattern = pattern.replaceAll("(<\\*>)(\\s+<\\*>)+", "<*>");
        // replace all the other substrings into one
        pattern = mergeRepeatedSubstrings(pattern);

        return pattern;
    }

    private String mergeRepeatedSubstrings(String input) {
        List<String> tokens = new ArrayList<>(Arrays.asList(input.split("\\s+")));
        List<String> result = new ArrayList<>();
        int i = 0;
        while (i < tokens.size()) {
            int maxSeqLen = 1;
            int maxRepeatCount = 1;

            int maxPossibleSeqLen = (tokens.size() - i) / 2;
            for (int seqLen = 1; seqLen <= maxPossibleSeqLen; seqLen++) {
                int repeatCount = 1;
                while (true) {
                    int start1 = i;
                    int start2 = i + repeatCount * seqLen;
                    if (start2 + seqLen > tokens.size())
                        break;

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

            // Add the sequence once
            for (int k = 0; k < maxSeqLen; k++) {
                result.add(tokens.get(i + k));
            }
            // Skip repeated sequences
            i += maxSeqLen * maxRepeatCount;
        }
        return String.join(" ", result);
    }

    private void executePPL(String ppl, ActionListener<String> listener) {
        JSONObject jsonContent = new JSONObject(ImmutableMap.of("query", ppl));
        PPLQueryRequest pplQueryRequest = new PPLQueryRequest(ppl, jsonContent, null, "jdbc");
        TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest(pplQueryRequest);
        client
            .execute(
                PPLQueryAction.INSTANCE,
                transportPPLQueryRequest,
                getPPLTransportActionListener(
                    ActionListener.wrap(transportPPLQueryResponse -> listener.onResponse(transportPPLQueryResponse.getResult()), e -> {
                        String pplError = "execute ppl:" + ppl + ", get error: " + e.getMessage();
                        Exception exception = new Exception(pplError, e);
                        listener.onFailure(exception);
                    })
                )
            );
    }

    private List<String> findCentroids(Map<String, double[]> logVectors, List<List<String>> clusters) {
        List<String> centroids = new ArrayList<>();
        for (List<String> cluster : clusters) {
            int traceCount = cluster.size();
            if (traceCount == 1) {
                centroids.add(cluster.getFirst());
                continue;
            }
            Map<String, Double> scores = new HashMap<>();
            // cache similarity result
            Map<String, Double> tempScores = new HashMap<>();
            for (String traceId : cluster) {
                double[] vector = logVectors.get(traceId);
                double score = 0;
                for (String otherTraceId : cluster) {
                    if (traceId.equals(otherTraceId)) {
                        continue;
                    }
                    if (tempScores.containsKey(String.format("%s-%s", traceId, otherTraceId))) {
                        score = tempScores.get(String.format("%s-%s", traceId, otherTraceId));
                        continue;
                    }
                    double[] otherVector = logVectors.get(otherTraceId);
                    score += 1 - cosineSimilarity(vector, otherVector);
                    tempScores.put(String.format("%s-%s", otherTraceId, traceId), score);
                }
                score /= traceCount;
                scores.put(traceId, score);
            }
            String centroid = scores
                .entrySet()
                .stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(scores.keySet().iterator().next());
            centroids.add(centroid);
        }
        return centroids;
    }

    private List<List<String>> clusterLogVectors(Map<String, double[]> logVectors) {
        List<List<String>> clusters = new ArrayList<>();

        for (Map.Entry<String, double[]> entry : logVectors.entrySet()) {
            String traceId = entry.getKey();
            double[] vector = entry.getValue();
            boolean placed = false;

            for (List<String> cluster : clusters) {
                if (cluster
                    .stream()
                    .anyMatch(member -> cosineSimilarity(vector, logVectors.get(member)) >= LOG_VECTORS_CLUSTERING_THRESHOLD)) {
                    cluster.add(traceId);
                    placed = true;
                    break;
                }
            }

            if (!placed) {
                clusters.add(new ArrayList<>(List.of(traceId)));
            }
        }

        return clusters;
    }

    private double cosineSimilarity(double[] vectorA, double[] vectorB) {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < vectorA.length; i++) {
            dotProduct += vectorA[i] * vectorB[i];
            normA += vectorA[i] * vectorA[i];
            normB += vectorB[i] * vectorB[i];
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
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
