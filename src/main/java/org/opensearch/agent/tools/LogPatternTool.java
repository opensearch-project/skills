/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.logging.LoggerMessageFormat;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.search.SearchHit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * This tool supports generating log patterns on the input dsl and index. It's implemented by
 * several steps:
 * 1. Retrival [[${DOC_SIZE_FIELD}]] logs from index
 * 2. Extract patterns for each retrieved log
 *  2.1 Find Pattern Field: If users provide parameter [[${PATTERN_FIELD}]], use it as the pattern
 *      field; Otherwise, find the string field with the longest length on the first log.
 *  2.2 Extract Pattern: If users provide parameter [[${PATTERN}]], compile it as a pattern;
 *      Otherwise, use [[${DEFAULT_IGNORED_CHARS}]]. It will remove all chars matching the pattern.
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

    public static String DEFAULT_DESCRIPTION = "Log Pattern Tool";
    public static final String TOP_N_PATTERN = "top_n_pattern";
    public static final String SAMPLE_LOG_SIZE = "sample_log_size";
    public static final String PATTERN_FIELD = "pattern_field";
    public static final String PATTERN = "pattern";
    public static final int LOG_PATTERN_DEFAULT_DOC_SIZE = 1000;
    public static final int DEFAULT_TOP_N_PATTERN = 3;
    public static final int DEFAULT_SAMPLE_LOG_SIZE = 20;
    private static final ImmutableSet<Character> DEFAULT_IGNORED_CHARS = ImmutableSet
        .copyOf("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".chars().mapToObj(c -> (char) c).toArray(Character[]::new));

    private String name = TYPE;
    private int topNPattern;
    private int sampleLogSize;
    @EqualsAndHashCode.Exclude
    private Pattern pattern;

    @Builder
    public LogPatternTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        int docSize,
        int topNPattern,
        int sampleLogSize,
        String patternStr
    ) {
        super(client, xContentRegistry, null, null, docSize);
        checkPositive(topNPattern, TOP_N_PATTERN);
        checkPositive(sampleLogSize, SAMPLE_LOG_SIZE);
        this.topNPattern = topNPattern;
        this.sampleLogSize = sampleLogSize;
        if (patternStr != null)
            this.pattern = Pattern.compile(patternStr);
    }

    @Override
    protected String getQueryBody(String queryText) {
        return queryText;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        int topNPattern = parameters.containsKey(TOP_N_PATTERN) ? getPositiveInteger(parameters, TOP_N_PATTERN) : this.topNPattern;
        int sampleLogSize = parameters.containsKey(SAMPLE_LOG_SIZE) ? getPositiveInteger(parameters, SAMPLE_LOG_SIZE) : this.sampleLogSize;
        Pattern pattern = parameters.containsKey(PATTERN) ? Pattern.compile(parameters.get(PATTERN)) : this.pattern;

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

            if (hits != null && hits.length > 0) {
                String patternField = parameters.containsKey(PATTERN_FIELD)
                    ? parameters.get(PATTERN_FIELD)
                    : findLongestField(hits[0].getSourceAsMap());
                if (patternField == null) {
                    listener.onResponse((T) "Pattern field is not set and this index doesn't contain any string field");
                }
                Map<String, List<Map<String, Object>>> patternGroups = new HashMap<>();
                for (SearchHit hit : hits) {
                    Map<String, Object> source = hit.getSourceAsMap();
                    String patternValue = extractPattern((String) source.getOrDefault(patternField, ""), pattern);
                    List<Map<String, Object>> group = patternGroups.computeIfAbsent(patternValue, k -> new ArrayList<>());
                    group.add(source);
                }
                List<Map<String, Object>> sortedEntries = patternGroups
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
                                entry.getValue().subList(0, Math.min(entry.getValue().size(), sampleLogSize))
                            )
                    )
                    .toList();

                listener
                    .onResponse((T) AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(sortedEntries)));
            } else {
                listener.onResponse((T) "Can not get any match from search result.");
            }
        }, e -> {
            log.error("Failed to search index.", e);
            listener.onFailure(e);
        });
        client.search(searchRequest, actionListener);
    }

    /**
     * Extract a pattern from the value of a field by removing chars in the pattern. This function
     * imitates the same logic of Observability log pattern feature here:
     * <a href="https://github.com/opensearch-project/sql/blob/4303a2ab755d53903094dd94a5100572677a27a1/core/src/main/java/org/opensearch/sql/expression/parse/PatternsExpression.java#L53">parseValue</a>
     * @param rawString string value of the field to generate a pattern
     * @param pattern @Nullable the specified pattern to remove, use DEFAULT_IGNORED_CHARS if null
     * @return the generated pattern value
     */
    @VisibleForTesting
    static String extractPattern(String rawString, Pattern pattern) {
        if (pattern != null)
            return pattern.matcher(rawString).replaceAll("");
        char[] chars = rawString.toCharArray();
        int pos = 0;
        for (int i = 0; i < chars.length; i++) {
            if (!DEFAULT_IGNORED_CHARS.contains(chars[i])) {
                chars[pos++] = chars[i];
            }
        }
        return new String(chars, 0, pos);
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
        // LogPatternTool needs to pass index and input as parameter in runtime.
        return super.validate(parameters) && parameters.containsKey(INDEX_FIELD) && !StringUtils.isBlank(parameters.get(INDEX_FIELD));
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
        if (value < 0) {
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
            int docSize = params.containsKey(DOC_SIZE_FIELD) ? getInteger(params, DOC_SIZE_FIELD) : LOG_PATTERN_DEFAULT_DOC_SIZE;
            int topNPattern = params.containsKey(TOP_N_PATTERN) ? getInteger(params, TOP_N_PATTERN) : DEFAULT_TOP_N_PATTERN;
            int sampleLogSize = params.containsKey(SAMPLE_LOG_SIZE) ? getInteger(params, SAMPLE_LOG_SIZE) : DEFAULT_SAMPLE_LOG_SIZE;
            String patternStr = params.containsKey(PATTERN) ? (String) params.get(PATTERN) : null;
            return LogPatternTool
                .builder()
                .client(client)
                .xContentRegistry(xContentRegistry)
                .docSize(docSize)
                .topNPattern(topNPattern)
                .sampleLogSize(sampleLogSize)
                .patternStr(patternStr)
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
