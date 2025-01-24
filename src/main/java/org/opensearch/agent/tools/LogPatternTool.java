/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.agent.tools.utils.BrainLogParser;
import org.opensearch.agent.tools.utils.ToolHelper;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.logging.LoggerMessageFormat;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.reflect.TypeToken;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * This tool supports generating log patterns on the input dsl and index. It's implemented by
 * several steps:
 * 1. Retrival [[${DOC_SIZE_FIELD}]] logs from index by either dsl or ppl query
 * 2. Extract patterns for input logs: If users provide parameter [[${PATTERN_FIELD}]], use it as the pattern
 *      field; Otherwise, find the string field with the longest length on the first log.
 * 3. Group logs by their extracted patterns.
 * 4. Find top N patterns with the largest sample log size.
 * 5. For each found top N patterns, return [[${SAMPLE_LOG_SIZE}]] sample logs.
 */
@Log4j2
@Getter
@Setter
@ToolAnnotation(LogPatternTool.TYPE)
public class LogPatternTool extends AbstractRetrieverTool {
    public static final String TYPE = "LogPatternTool";

    public static final String DEFAULT_DESCRIPTION = "Log Pattern Tool";
    public static final String TOP_N_PATTERN = "top_n_pattern";
    public static final String SAMPLE_LOG_SIZE = "sample_log_size";
    public static final String PATTERN_FIELD = "pattern_field";
    public static final String PPL_FIELD = "ppl";
    public static final String VARIABLE_COUNT_THRESHOLD = "variable_count_threshold";
    public static final int LOG_PATTERN_DEFAULT_DOC_SIZE = 1000;
    public static final int DEFAULT_TOP_N_PATTERN = 3;
    public static final int DEFAULT_SAMPLE_LOG_SIZE = 20;
    public static final int DEFAULT_VARIABLE_COUNT_THRESHOLD = 5;

    private static final float DEFAULT_THRESHOLD_PERCENTAGE = 0.3f;
    private static final String PPL_SCHEMA_NAME = "name";

    private String name = TYPE;
    private int topNPattern;
    private int sampleLogSize;
    private BrainLogParser logParser;

    @Builder
    public LogPatternTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        int docSize,
        int topNPattern,
        int sampleLogSize,
        int variableCountThreshold
    ) {
        super(client, xContentRegistry, null, null, docSize);
        checkPositive(topNPattern, TOP_N_PATTERN);
        checkPositive(sampleLogSize, SAMPLE_LOG_SIZE);
        checkPositive(variableCountThreshold, VARIABLE_COUNT_THRESHOLD);
        this.topNPattern = topNPattern;
        this.sampleLogSize = sampleLogSize;
        this.logParser = new BrainLogParser(variableCountThreshold, DEFAULT_THRESHOLD_PERCENTAGE);
    }

    @Override
    protected String getQueryBody(String queryText) {
        return removeDSLAggregations(queryText);
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        String dsl = parameters.get(INPUT_FIELD);
        String ppl = parameters.get(PPL_FIELD);
        if (!StringUtils.isBlank(dsl)) {
            SearchRequest searchRequest;
            try {
                searchRequest = buildSearchRequest(parameters);
            } catch (Exception e) {
                log.error("Failed to build search request.", e);
                listener.onFailure(e);
                return;
            }

            ActionListener<SearchResponse> actionListener = ActionListener.wrap(r -> {
                SearchHit[] hits = r.getHits().getHits();

                if (!CollectionUtils.isEmpty(hits)) {
                    Map<String, Object> firstLogSource = hits[0].getSourceAsMap();

                    Function<String, List<String>> logMessagesProvider = (String patternField) -> Arrays.stream(hits).map(hit -> {
                        Map<String, Object> source = hit.getSourceAsMap();
                        return (String) source.getOrDefault(patternField, "");
                    }).collect(Collectors.toList());

                    onResponseSortedLogPatterns(parameters, listener, firstLogSource, logMessagesProvider);
                } else {
                    listener.onResponse((T) "Can not get any match from search result.");
                }
            }, e -> {
                log.error("Failed to search index.", e);
                listener.onFailure(e);
            });
            client.search(searchRequest, actionListener);
        } else if (!StringUtils.isBlank(ppl)) {
            String prunedPPL = removePPLAggregations(ppl);
            PPLQueryRequest pplQueryRequest = new PPLQueryRequest(prunedPPL, null, null, "jdbc");
            TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest(pplQueryRequest);

            ActionListener<TransportPPLQueryResponse> actionListener = ActionListener.wrap(r -> {
                String results = r.getResult();
                Map<String, Object> pplResult = gson.fromJson(results, new TypeToken<Map<String, Object>>() {
                }.getType());
                List<Map<String, String>> schema = (List<Map<String, String>>) pplResult.getOrDefault("schema", new ArrayList<>());
                List<List<Object>> dataRows = (List<List<Object>>) pplResult.getOrDefault("datarows", new ArrayList<>());
                List<Object> firstDataRow = dataRows.isEmpty() ? new ArrayList<>() : dataRows.get(0);
                if (!firstDataRow.isEmpty()) {
                    Map<String, Object> firstLogSource = new HashMap<>();
                    IntStream
                        .range(0, schema.size())
                        .boxed()
                        .filter(i -> schema.get(i) != null && !StringUtils.isBlank(schema.get(i).get(PPL_SCHEMA_NAME)))
                        .forEach(i -> firstLogSource.put(schema.get(i).get(PPL_SCHEMA_NAME), firstDataRow.get(i)));

                    Function<String, List<String>> logMessagesProvider = (String patternField) -> IntStream
                        .range(0, schema.size())
                        .boxed()
                        .filter(i -> schema.get(i) != null && patternField.equals(schema.get(i).get(PPL_SCHEMA_NAME)))
                        .findFirst()
                        .map(fieldIndex -> dataRows.stream().map(dataRow -> (String) dataRow.get(fieldIndex)).collect(Collectors.toList()))
                        .orElseGet(ArrayList::new);

                    onResponseSortedLogPatterns(parameters, listener, firstLogSource, logMessagesProvider);
                } else {
                    listener.onResponse((T) "Can not get any data row from ppl response.");
                }
            }, e -> {
                log.error("Failed to query ppl.", e);
                listener.onFailure(e);
            });
            client.execute(PPLQueryAction.INSTANCE, transportPPLQueryRequest, ToolHelper.getPPLTransportActionListener(actionListener));
        } else {
            Exception e = new IllegalArgumentException("Both DSL and PPL input is null or empty, can not process it.");
            log.error("Failed to find searchable query.", e);
            listener.onFailure(e);
        }
    }

    /**
     * Find the longest field in the sample log source. This function imitates the same logic of
     * Observability log pattern feature here:
     * <a href="https://github.com/opensearch-project/dashboards-observability/blob/279463ad0c8e00780bf57f85e6d2bf30e8c7c93d/public/components/event_analytics/hooks/use_fetch_patterns.ts#L137C2-L164C6">setDefaultPatternsField</a>
     * @param sampleLogSource sample log source map
     * @return the longest field name
     */
    @VisibleForTesting
    static String findLongestField(Map<String, Object> sampleLogSource) {
        String longestField = null;
        int maxLength = 0;

        for (Map.Entry<String, Object> entry : sampleLogSource.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                String stringValue = (String) value;
                int length = stringValue.length();
                if (length > maxLength) {
                    maxLength = length;
                    longestField = entry.getKey();
                }
            }
        }
        return longestField;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        // LogPatternTool needs to pass dsl input with index or ppl as parameter in runtime.
        return parameters != null
            && !parameters.isEmpty()
            && ((!StringUtils.isBlank(parameters.get(INPUT_FIELD)) && !StringUtils.isBlank(parameters.get(INDEX_FIELD)))
                || !StringUtils.isBlank(parameters.get(PPL_FIELD)));
    }

    private <T> void onResponseSortedLogPatterns(
        Map<String, String> parameters,
        ActionListener<T> listener,
        Map<String, Object> firstLogSource,
        Function<String, List<String>> logMessagesProvider
    ) throws PrivilegedActionException {
        String patternField = parameters.getOrDefault(PATTERN_FIELD, findLongestField(firstLogSource));
        validatePatternFieldAndFirstLogSource(parameters, patternField, firstLogSource);
        List<String> logMessages = logMessagesProvider.apply(patternField);
        List<Map<String, Object>> sortedEntries = getTopNLogPatterns(parameters, logMessages);

        listener.onResponse((T) AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(sortedEntries)));
    }

    private List<Map<String, Object>> getTopNLogPatterns(Map<String, String> parameters, List<String> logMessages) {
        int topNPattern = parameters.containsKey(TOP_N_PATTERN) ? getPositiveInteger(parameters, TOP_N_PATTERN) : this.topNPattern;
        int sampleLogSize = parameters.containsKey(SAMPLE_LOG_SIZE) ? getPositiveInteger(parameters, SAMPLE_LOG_SIZE) : this.sampleLogSize;

        Map<String, List<String>> logPatternMap = logParser.parseAllLogPatterns(logMessages);

        return logPatternMap
            .entrySet()
            .stream()
            .sorted(Comparator.comparingInt(entry -> -entry.getValue().size()))
            .limit(topNPattern)
            .map(
                entry -> Map
                    .of(
                        "total count",
                        entry.getValue().size(),
                        "pattern",
                        entry.getKey(),
                        "sample logs",
                        entry
                            .getValue()
                            .subList(0, Math.min(entry.getValue().size(), sampleLogSize))
                            .stream()
                            .map(logId -> logMessages.get(Integer.parseInt(logId)))
                            .collect(Collectors.toList())
                    )
            )
            .collect(Collectors.toList());
    }

    private void validatePatternFieldAndFirstLogSource(
        Map<String, String> parameters,
        String patternField,
        Map<String, Object> firstLogSource
    ) {
        if (patternField == null) {
            throw new IllegalArgumentException("Pattern field is not set and this index doesn't contain any string field");
        } else if (!firstLogSource.containsKey(patternField)) {
            throw new IllegalArgumentException(
                LoggerMessageFormat
                    .format(
                        null,
                        "Invalid parameter pattern_field: index {} does not have a field named {}",
                        parameters.getOrDefault(INDEX_FIELD, index),
                        patternField
                    )
            );
        } else if (!(firstLogSource.get(patternField) instanceof String)) {
            throw new IllegalArgumentException(
                LoggerMessageFormat
                    .format(
                        null,
                        "Invalid parameter pattern_field: pattern field {} in index {} is not type of String",
                        patternField,
                        parameters.getOrDefault(INDEX_FIELD, index)
                    )
            );
        }
    }

    private String removeDSLAggregations(String dsl) {
        JSONObject dslObj = new JSONObject(dsl);
        // DSL request is a json blob. Aggregations usually have keys 'aggs' or 'aggregations'
        dslObj.remove("aggs");
        dslObj.remove("aggregations");
        return dslObj.toString();
    }

    private String removePPLAggregations(String ppl) {
        String normPPL = ppl.replaceAll("\\s+", " ");
        /*
         * Remove all following query starting with stats as they rely on aggregation results.
         * We don't convert ppl string to lower case or upper case and directly use converted ppl
         * because some enum parameters of functions are case-sensitive.
         * i.e. TIMESTAMPADD(DAY, -1, '2025-01-01 00:00:00') is different from TIMESTAMPADD(day, -1, '2025-01-01 00:00:00')
         * The latter one is not parsed well by PPLService.
         */
        int idx = normPPL.toUpperCase(Locale.ROOT).indexOf("| STATS");
        return idx != -1 ? normPPL.substring(0, idx).trim() : ppl;
    }

    private static int getPositiveInteger(Map<String, ?> params, String paramName) {
        int value = getInteger(params, paramName);
        checkPositive(value, paramName);
        return value;
    }

    private static int getInteger(Map<String, ?> params, String paramName) {
        int value;
        try {
            value = Integer.parseInt((String) params.get(paramName));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                LoggerMessageFormat.format("Invalid value {} for parameter {}, it should be a number", params.get(paramName), paramName)
            );
        }
        return value;
    }

    private static void checkPositive(int value, String paramName) {
        if (value <= 0) {
            throw new IllegalArgumentException(
                LoggerMessageFormat.format("Invalid value {} for parameter {}, it should be positive", value, paramName)
            );
        }
    }

    public static class Factory extends AbstractRetrieverTool.Factory<LogPatternTool> {
        private static LogPatternTool.Factory INSTANCE;

        public static LogPatternTool.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (LogPatternTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new LogPatternTool.Factory();
                return INSTANCE;
            }
        }

        @Override
        public LogPatternTool create(Map<String, Object> params) {
            int docSize = params.containsKey(DOC_SIZE_FIELD) ? getPositiveInteger(params, DOC_SIZE_FIELD) : LOG_PATTERN_DEFAULT_DOC_SIZE;
            int topNPattern = params.containsKey(TOP_N_PATTERN) ? getPositiveInteger(params, TOP_N_PATTERN) : DEFAULT_TOP_N_PATTERN;
            int sampleLogSize = params.containsKey(SAMPLE_LOG_SIZE) ? getPositiveInteger(params, SAMPLE_LOG_SIZE) : DEFAULT_SAMPLE_LOG_SIZE;
            int variableCountThreshold = params.containsKey(VARIABLE_COUNT_THRESHOLD)
                ? getPositiveInteger(params, VARIABLE_COUNT_THRESHOLD)
                : DEFAULT_VARIABLE_COUNT_THRESHOLD;
            return LogPatternTool
                .builder()
                .client(client)
                .xContentRegistry(xContentRegistry)
                .docSize(docSize)
                .topNPattern(topNPattern)
                .sampleLogSize(sampleLogSize)
                .variableCountThreshold(variableCountThreshold)
                .build();
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
