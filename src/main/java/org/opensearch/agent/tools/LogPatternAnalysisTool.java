/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.tools.utils.ToolHelper.getPPLTransportActionListener;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private static final double LOG_VECTORS_CLUSTERING_THRESHOLD = 0.5;
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
        String logFieldName = parameters.getOrDefault("logFieldName", "");
        String traceFieldName = parameters.getOrDefault("traceFieldName", "");
        String normalTimeRangeStart = parameters.getOrDefault("normalTimeRangeStart", "");
        String normalTimeRangeEnd = parameters.getOrDefault("normalTimeRangeEnd", "");
        String abnormalTimeRangeStart = parameters.getOrDefault("abnormalTimeRangeStart", "");
        String abnormalTimeRangeEnd = parameters.getOrDefault("abnormalTimeRangeEnd", "");

        if (Strings.isEmpty(index)
            || Strings.isEmpty(logFieldName)
            || Strings.isEmpty(traceFieldName)
            || Strings.isEmpty(normalTimeRangeStart)
            || Strings.isEmpty(normalTimeRangeEnd)
            || Strings.isEmpty(abnormalTimeRangeStart)
            || Strings.isEmpty(abnormalTimeRangeEnd)) {
            throw new IllegalArgumentException(
                "Invalid parameters, please check the parameters of LogPatternAnalysisTool,"
                    + " index|logFieldName|traceFieldName|normalTimeRangeStart|normalTimeRangeEnd|abnormalTimeRangeStart|abnormalTimeRangeEnd are required!"
            );
        }

        // Step 1. generate log patterns for normal time range
        String normalTimeRangeLogPatternPPL = ("source=%s | where %s!='' | where time>'%s' and time <'%s' | patterns %s method=brain "
            + "variable_count_threshold=2 | fields %s, patterns_field | sort time")
            .formatted(index, traceFieldName, normalTimeRangeStart, normalTimeRangeEnd, logFieldName, traceFieldName);
        // log ppl
        log.info("normalTimeRangeLogPatternPPL: {}", normalTimeRangeLogPatternPPL);
        executePPL(normalTimeRangeLogPatternPPL, ActionListener.wrap(normalTimeRangeResult -> {
            Map<String, Object> normalTimeRangePPLResult = gson.fromJson(normalTimeRangeResult, new TypeToken<Map<String, Object>>() {
            }.getType());
            List<List<Object>> normalTimeRangeDataRows = (List<List<Object>>) normalTimeRangePPLResult
                .getOrDefault("datarows", new ArrayList<>());
            // map traceId to its patterns
            Map<String, Set<String>> normalTimeRangeTraceIdPatternMap = new HashMap<>();
            // map pattern to its count
            Map<String, Integer> normalTimeRangePatternCountMap = new HashMap<>();
            // map pattern to weight
            Map<String, Double> normalTimeRangePatternValue = new HashMap<>();

            // used for cache
            Map<String, String> rawPatternToSimplifiedPatternMap = new HashMap<>();
            for (List<Object> row : normalTimeRangeDataRows) {
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

                if (normalTimeRangeTraceIdPatternMap.containsKey(traceId)) {
                    Set<String> newPatterns = new HashSet<>(normalTimeRangeTraceIdPatternMap.get(traceId));
                    newPatterns.add(simplifiedPattern);
                    normalTimeRangeTraceIdPatternMap.put(traceId, newPatterns);
                } else {
                    normalTimeRangeTraceIdPatternMap.put(traceId, Set.of(simplifiedPattern));
                }

                normalTimeRangePatternCountMap
                    .put(simplifiedPattern, normalTimeRangePatternCountMap.getOrDefault(simplifiedPattern, 0) + 1);
            }

            int normalTimeRangeTraceCount = normalTimeRangeTraceIdPatternMap.size();
            for (Map.Entry<String, Integer> entry : normalTimeRangePatternCountMap.entrySet()) {
                if (entry.getValue() != 0) {
                    double value = Math.log((double) normalTimeRangeTraceCount / entry.getValue());
                    value = 1 / (1 + Math.exp(-value));
                    normalTimeRangePatternValue.put(entry.getKey(), value);
                } else {
                    normalTimeRangePatternValue.put(entry.getKey(), 0.0);
                }
            }

            log.info("normalTimeRangeTraceIdPatternMap");
            for (Map.Entry<String, Set<String>> entry : normalTimeRangeTraceIdPatternMap.entrySet()) {
                 log.debug(entry.getKey() + ": " + entry.getValue().toString());
            }

            log.info("normalTimeRangePatternValue:");
            for (Map.Entry<String, Double> entry : normalTimeRangePatternValue.entrySet()) {
                 log.debug(entry.getKey() + ": " + entry.getValue());
            }

            // Step 2. generate log patterns for abnormal time range
            String abnormalTimeRangeLogPatternPPL = ("source=%s | where %s!='' | where time>'%s' and time <'%s' | patterns %s method=brain "
                + "variable_count_threshold=2 | fields %s, patterns_field | sort time")
                .formatted(index, traceFieldName, abnormalTimeRangeStart, abnormalTimeRangeEnd, logFieldName, traceFieldName);
            // log abnormal ppl
            log.info("abnormalTimeRangeLogPatternPPL:{}", abnormalTimeRangeLogPatternPPL);
            Map<String, Set<String>> abnormalTimeRangeTraceIdPatternMap = new HashMap<>();
            Map<String, Integer> abnormalTimeRangePatternCountMap = new HashMap<>();
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

                    if (abnormalTimeRangeTraceIdPatternMap.containsKey(traceId)) {
                        Set<String> newPatterns = new HashSet<>(abnormalTimeRangeTraceIdPatternMap.get(traceId));
                        newPatterns.add(simplifiedPattern);
                        abnormalTimeRangeTraceIdPatternMap.put(traceId, newPatterns);
                    } else {
                        abnormalTimeRangeTraceIdPatternMap.put(traceId, Set.of(simplifiedPattern));
                    }

                    abnormalTimeRangePatternCountMap
                        .put(simplifiedPattern, abnormalTimeRangePatternCountMap.getOrDefault(simplifiedPattern, 0) + 1);
                }

                log.info("abnormalTimeRangeTraceIdPatternMap");
                for (Map.Entry<String, Set<String>> entry : abnormalTimeRangeTraceIdPatternMap.entrySet()) {
                    log.debug(entry.getKey() + ": " + entry.getValue().toString());
                }

                int abnormalTimeRangeTraceCount = abnormalTimeRangeTraceIdPatternMap.size();
                for (Map.Entry<String, Integer> entry : abnormalTimeRangePatternCountMap.entrySet()) {
                    if (entry.getValue() != 0) {
                        double value = Math.log((double) abnormalTimeRangeTraceCount / entry.getValue());
                        value = 1 / (1 + Math.exp(-value));
                        abnormalTimeRangePatternValue.put(entry.getKey(), value);
                    } else {
                        abnormalTimeRangePatternValue.put(entry.getKey(), 0.0);
                    }
                }

                log.info("abnormalTimeRangePatternValue:");
                for (Map.Entry<String, Double> entry : abnormalTimeRangePatternValue.entrySet()) {
                    log.debug(entry.getKey() + ": " + entry.getValue());
                }

                // Step 3. construct pattern index, take all patterns into consideration, and associate each pattern with a number as the
                // index in the vector
                Map<String, Integer> patternIndex = new HashMap<>();
                Set<String> patternSet = new HashSet<>(normalTimeRangePatternCountMap.keySet());
                patternSet.addAll(abnormalTimeRangePatternCountMap.keySet());
                List<String> patternList = new ArrayList<>(patternSet);
                Collections.sort(patternList);
                for (int i = 0; i < patternList.size(); i++) {
                    patternIndex.put(patternList.get(i), i);
                }

                log.info("pattern index:");
                for (Map.Entry<String, Integer> entry : patternIndex.entrySet()) {
                    log.debug(entry.getKey() + ": " + entry.getValue());
                }

                // Step 4. build vectors for normal time range
                Map<String, double[]> normalTimeRangeVectorMap = new HashMap<>();
                for (Map.Entry<String, Set<String>> entry : normalTimeRangeTraceIdPatternMap.entrySet()) {
                    double[] vector = new double[patternList.size()];
                    for (String pattern : entry.getValue()) {
                        vector[patternIndex.get(pattern)] = 0.5 * normalTimeRangePatternValue.get(pattern);
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
                        vector[patternIndex.get(pattern)] = 0.5 * abnormalTimeRangePatternValue.get(pattern) + 0.5 * existenceWeight;
                    }
                    abnormalTimeRangeVectorMap.put(entry.getKey(), vector);
                }

                log.info("abnormalTimeRangeVectorMap:");
                for (Map.Entry<String, double[]> entry : abnormalTimeRangeVectorMap.entrySet()) {
                    log.debug(entry.getKey() + ": " + Arrays.toString(entry.getValue()));
                }

                // Step 7. find all non-existent traces in abnormal time range
                Set<String> abnormalTraces = findAbnormalTraces(centroids, normalTimeRangeVectorMap, abnormalTimeRangeVectorMap);
                Map<String, double[]> abnormalTracesVectorMap = new HashMap<>();
                for (Map.Entry<String, double[]> entry : abnormalTimeRangeVectorMap.entrySet()) {
                    if (abnormalTraces.contains(entry.getKey())) {
                        abnormalTracesVectorMap.put(entry.getKey(), entry.getValue());
                    }
                }

                log.info("abnormalTraces:");
                log.info(abnormalTraces);

                // Step 7. cluster the non-existent trances and find centroids in abnormal time range
                List<List<String>> abnormalClusters = clusterLogVectors(abnormalTracesVectorMap);
                List<String> abnormalClustersCentroids = findCentroids(abnormalTracesVectorMap, abnormalClusters);

                log.info("abnormalClusters:");
                log.info(abnormalClusters);

                log.info("abnormalClustersCentroids:");
                log.info(abnormalClustersCentroids);

                // get actual log sequence
                Map<String, String> abnormalLogSequenceMap = new HashMap<>();
                for (String traceId : abnormalClustersCentroids) {
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

    private Set<String> findAbnormalTraces(
        List<String> normalTimeRangeLogClusterCentroids,
        Map<String, double[]> normalTimeRangeVectorMap,
        Map<String, double[]> abnormalTimeRangeVectorMap
    ) {
        Set<String> abnormalTraces = new HashSet<>();
        for (Map.Entry<String, double[]> entry : abnormalTimeRangeVectorMap.entrySet()) {
            String traceId = entry.getKey();
            double[] vector = entry.getValue();
            boolean found = false;
            for (String centroid : normalTimeRangeLogClusterCentroids) {
                double[] centroidVector = normalTimeRangeVectorMap.get(centroid);
                double similarity = cosineSimilarity(vector, centroidVector);
                if (similarity >= LOG_VECTORS_CLUSTERING_THRESHOLD) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                abnormalTraces.add(traceId);
            }
        }

        return abnormalTraces;
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
