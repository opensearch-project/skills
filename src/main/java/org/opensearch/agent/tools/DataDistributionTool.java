/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.CommonValue.TOOL_INPUT_SCHEMA_FIELD;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.math.NumberUtils;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.agent.tools.utils.PPLExecuteHelper;
import org.opensearch.agent.tools.utils.ToolHelper;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.utils.ToolUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import com.google.gson.reflect.TypeToken;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Usage:
 * 1. Register agent:
 * POST /_plugins/_ml/agents/_register
 * {
 *   "name": "DataDistribution",
 *   "type": "flow",
 *   "tools": [
 *     {
 *       "name": "data_distribution_tool",
 *       "type": "DataDistributionTool",
 *       "parameters": {
 *       }
 *     }
 *   ]
 * }
 * 2. Execute agent:
 * POST /_plugins/_ml/agents/{agent_id}/_execute
 * {
 *   "parameters": {
 *     "index": "logs-2025.01.15",
 *     "timeField": "@timestamp",
 *     "selectionTimeRangeStart": "2025-01-15 10:00:00",
 *     "selectionTimeRangeEnd": "2025-01-15 11:00:00",
 *     "baselineTimeRangeStart": "2025-01-15 08:00:00",
 *     "baselineTimeRangeEnd": "2025-01-15 09:00:00",
 *     "size": 1000,
 *     "queryType": "dsl",
 *     "filter": ["{'term': {'status': 'error'}}", "{'range': {'response_time': {'gte': 100}}}"],
 *     "dsl": "{\"bool\": {\"must\": [{\"term\": {\"status\": \"error\"}}]}}",
 *     "ppl": "source index where a=0"
 *   }
 * }
 * 3. Result: analysis of data distribution patterns
 * {
 *   "comparisonAnalysis": [
 *     {
 *       "field": "status",
 *       "divergence": 0.2,
 *       "topChanges": [
 *         {
 *           "value": "error",
 *           "selectionPercentage": 0.3,
 *           "baselinePercentage": 0.1
 *         },
 *         {
 *           "value": "success",
 *           "selectionPercentage": 0.7,
 *           "baselinePercentage": 0.9
 *         }
 *       ]
 *     }
 *   ]
 * }
 */
@Log4j2
@Setter
@Getter
@ToolAnnotation(DataDistributionTool.TYPE)
public class DataDistributionTool implements Tool {
    public static final String TYPE = "DataDistributionTool";
    public static final String STRICT_FIELD = "strict";

    private static final String DEFAULT_DESCRIPTION =
        "This tool analyzes data distribution differences between time ranges or provides single dataset insights.";
    private static final String DEFAULT_TIME_FIELD = "@timestamp";

    private static final String PARAM_INDEX = "index";
    private static final String PARAM_TIME_FIELD = "timeField";
    private static final String PARAM_SELECTION_TIME_RANGE_START = "selectionTimeRangeStart";
    private static final String PARAM_SELECTION_TIME_RANGE_END = "selectionTimeRangeEnd";
    private static final String PARAM_BASELINE_TIME_RANGE_START = "baselineTimeRangeStart";
    private static final String PARAM_BASELINE_TIME_RANGE_END = "baselineTimeRangeEnd";
    private static final String PARAM_SIZE = "size";
    private static final String PARAM_QUERY_TYPE = "queryType";
    private static final String PARAM_FILTER = "filter";
    private static final String PARAM_DSL = "dsl";
    private static final String QUERY_TYPE_PPL = "ppl";
    private static final String QUERY_TYPE_DSL = "dsl";
    private static final String DEFAULT_SIZE = "1000";
    private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    private static final Set<String> USEFUL_FIELD_TYPES = Set
        .of("keyword", "boolean", "text", "byte", "short", "integer", "long", "float", "double", "half_float", "scaled_float");
    private static final Set<String> NUMBER_FIELD_TYPES = Set
        .of("byte", "short", "integer", "long", "float", "double", "half_float", "scaled_float");

    private static final int DEFAULT_COMPARISON_RESULT_LIMIT = 10;
    private static final int DEFAULT_SINGLE_ANALYSIS_RESULT_LIMIT = 30;
    private static final int MIN_CARDINALITY_DIVISOR = 4;
    private static final int MIN_CARDINALITY_BASE = 5;
    private static final int ID_FIELD_MAX_CARDINALITY = 30;
    private static final int DATA_FIELD_MAX_CARDINALITY = 10;
    private static final int DATA_FIELD_CARDINALITY_DIVISOR = 2;
    private static final int NUMERIC_GROUPING_THRESHOLD = 10;
    private static final double PERCENTAGE_MULTIPLIER = 100.0;
    private static final int TOP_CHANGES_LIMIT = 10;
    private static final int MAX_SIZE_LIMIT = 10000;

    public static final String DEFAULT_INPUT_SCHEMA = """
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
                    "description": "Start time for analysis period"
                },
                "selectionTimeRangeEnd": {
                    "type": "string",
                    "description": "End time for analysis period"
                },
                "baselineTimeRangeStart": {
                    "type": "string",
                    "description": "Start time for baseline period (optional)"
                },
                "baselineTimeRangeEnd": {
                    "type": "string",
                    "description": "End time for baseline period (optional)"
                },
                "size": {
                    "type": "integer",
                    "description": "Maximum number of documents to analyze (default: 1000)"
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
                    "description": "Additional DSL query conditions for filtering (optional)"
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
            "required": ["index", "selectionTimeRangeStart", "selectionTimeRangeEnd"],
            "additionalProperties": false
        }
        """;

    public static final Map<String, Object> DEFAULT_ATTRIBUTES = Map.of(TOOL_INPUT_SCHEMA_FIELD, DEFAULT_INPUT_SCHEMA, STRICT_FIELD, false);

    /**
     * Parameter class to hold analysis parameters with validation
     */
    private static class AnalysisParameters {
        final String index;
        final String timeField;
        final String selectionTimeRangeStart;
        final String selectionTimeRangeEnd;
        final String baselineTimeRangeStart;
        final String baselineTimeRangeEnd;
        final int size;
        final String queryType;
        final List<String> filter;
        final String dsl;
        final String ppl;

        /**
         * Constructs analysis parameters from input map with default values
         *
         * @param parameters Input parameter map from user request
         */
        AnalysisParameters(Map<String, String> parameters) {
            this.index = parameters.getOrDefault(PARAM_INDEX, "");
            this.timeField = parameters.getOrDefault(PARAM_TIME_FIELD, DEFAULT_TIME_FIELD);
            this.selectionTimeRangeStart = parameters.getOrDefault(PARAM_SELECTION_TIME_RANGE_START, "");
            this.selectionTimeRangeEnd = parameters.getOrDefault(PARAM_SELECTION_TIME_RANGE_END, "");
            this.baselineTimeRangeStart = parameters.getOrDefault(PARAM_BASELINE_TIME_RANGE_START, "");
            this.baselineTimeRangeEnd = parameters.getOrDefault(PARAM_BASELINE_TIME_RANGE_END, "");

            try {
                this.size = Integer.parseInt(parameters.getOrDefault(PARAM_SIZE, DEFAULT_SIZE));
                if (this.size > MAX_SIZE_LIMIT) {
                    throw new IllegalArgumentException("Size parameter exceeds maximum limit of " + MAX_SIZE_LIMIT + ", got: " + this.size);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid 'size' parameter: must be a valid integer, got '" + parameters.get(PARAM_SIZE) + "'"
                );
            }

            this.queryType = parameters.getOrDefault(PARAM_QUERY_TYPE, QUERY_TYPE_DSL);

            String filterParam = parameters.getOrDefault(PARAM_FILTER, "");
            if (Strings.isEmpty(filterParam)) {
                this.filter = List.of();
            } else {
                try {
                    this.filter = Arrays.asList(gson.fromJson(filterParam, String[].class));
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                        "Invalid 'filter' parameter: must be a valid JSON array of strings, got '"
                            + filterParam
                            + "'. Example: [\"{'term': {'status': 'error'}}\", \"{'range': {'level': {'gte': 3}}}\"]"
                    );
                }
            }

            this.dsl = parameters.getOrDefault(PARAM_DSL, "");
            this.ppl = parameters.getOrDefault(QUERY_TYPE_PPL, "");
        }

        /**
         * Validates required parameters are present
         *
         * @throws IllegalArgumentException if required parameters are missing
         */
        void validate() {
            List<String> missingParams = new ArrayList<>();
            if (Strings.isEmpty(index))
                missingParams.add(PARAM_INDEX);
            if (Strings.isEmpty(selectionTimeRangeStart))
                missingParams.add(PARAM_SELECTION_TIME_RANGE_START);
            if (Strings.isEmpty(selectionTimeRangeEnd))
                missingParams.add(PARAM_SELECTION_TIME_RANGE_END);
            if (Strings.isEmpty(timeField))
                missingParams.add(PARAM_TIME_FIELD);
            if (!missingParams.isEmpty()) {
                throw new IllegalArgumentException("Missing required parameters: " + String.join(", ", missingParams));
            }
        }

        /**
         * Checks if baseline time range is provided for comparison analysis
         *
         * @return true if both baseline start and end times are provided
         */
        boolean hasBaselineTime() {
            return !Strings.isEmpty(baselineTimeRangeStart) && !Strings.isEmpty(baselineTimeRangeEnd);
        }
    }

    /**
     * Result class for data distribution analysis
     */
    private record SummaryDataItem(String field, double divergence, List<ChangeItem> topChanges) {
    }

    /**
     * Individual change item for field values
     */
    private record ChangeItem(String value, double selectionPercentage, Double baselinePercentage) {
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

    /**
     * Constructs a DataDistributionTool with the given OpenSearch client
     *
     * @param client The OpenSearch client for executing queries
     */
    public DataDistributionTool(Client client) {
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
            log.error("Failed to validate the data distribution analysis parameter: {}", e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Executes data distribution analysis based on provided parameters.
     * Supports both single dataset analysis and comparative analysis between time periods.
     *
     * @param <T> The response type
     * @param originalParameters Input parameters for analysis
     * @param listener Action listener for handling results or failures
     */
    @Override
    public <T> void run(Map<String, String> originalParameters, ActionListener<T> listener) {
        try {
            Map<String, String> parameters = ToolUtils.extractInputParameters(originalParameters, DEFAULT_ATTRIBUTES);
            log.debug("Starting data distribution analysis with parameters: {}", parameters.keySet());
            AnalysisParameters params = new AnalysisParameters(parameters);

            if (QUERY_TYPE_PPL.equals(params.queryType)) {
                executePPLAnalysis(params, listener);
            } else {
                executeDSLAnalysis(params, listener);
            }
        } catch (IllegalArgumentException e) {
            log.error("Invalid parameters for DataDistributionTool: {}", e.getMessage());
            listener.onFailure(e);
        } catch (Exception e) {
            log.error("Unexpected error in DataDistributionTool", e);
            listener.onFailure(e);
        }
    }

    /**
     * Executes analysis using PPL (Piped Processing Language) queries
     *
     * @param <T> The response type
     * @param params Analysis parameters containing query details
     * @param listener Action listener for handling results
     */
    private <T> void executePPLAnalysis(AnalysisParameters params, ActionListener<T> listener) {
        if (params.hasBaselineTime()) {
            fetchPPLComparisonData(params, listener);
        } else {
            String pplQuery = buildPPLQuery(
                params.index,
                params.timeField,
                params.selectionTimeRangeStart,
                params.selectionTimeRangeEnd,
                params.size,
                params.ppl
            );

            Function<Map<String, Object>, List<Map<String, Object>>> pplResultParser = pplResult -> {
                Object datarowsObj = pplResult.get("datarows");
                Object schemaObj = pplResult.get("schema");

                if (!(datarowsObj instanceof List) || !(schemaObj instanceof List)) {
                    return List.of();
                }

                @SuppressWarnings("unchecked")
                List<List<Object>> dataRows = (List<List<Object>>) datarowsObj;
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> schema = (List<Map<String, Object>>) schemaObj;

                List<Map<String, Object>> result = new ArrayList<>();
                for (List<Object> row : dataRows) {
                    Map<String, Object> doc = new HashMap<>();
                    for (int i = 0; i < Math.min(row.size(), schema.size()); i++) {
                        String columnName = (String) schema.get(i).get("name");
                        if (columnName != null) {
                            doc.put(columnName, row.get(i));
                        }
                    }
                    result.add(doc);
                }
                return result;
            };

            PPLExecuteHelper.executePPLAndParseResult(client, pplQuery, pplResultParser, ActionListener.wrap(data -> {
                try {
                    analyzeSingleDataset(data, params.index, ActionListener.wrap(result -> {
                        listener.onResponse((T) gson.toJson(Map.of("singleAnalysis", result)));
                    }, listener::onFailure));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, listener::onFailure));
        }
    }

    /**
     * Executes analysis using DSL (Domain Specific Language) queries
     *
     * @param <T> The response type
     * @param params Analysis parameters containing query details
     * @param listener Action listener for handling results
     */
    private <T> void executeDSLAnalysis(AnalysisParameters params, ActionListener<T> listener) {
        if (params.hasBaselineTime()) {
            fetchComparisonData(params, listener);
        } else {
            getSingleDataDistribution(params, listener);
        }
    }

    /**
     * Fetches data for both selection and baseline time ranges for comparison analysis
     *
     * @param <T> The response type
     * @param params Analysis parameters containing time ranges
     * @param listener Action listener for handling comparison results
     */
    private <T> void fetchComparisonData(AnalysisParameters params, ActionListener<T> listener) {
        fetchIndexData(params.selectionTimeRangeStart, params.selectionTimeRangeEnd, params, ActionListener.wrap(selectionData -> {
            fetchIndexData(params.baselineTimeRangeStart, params.baselineTimeRangeEnd, params, ActionListener.wrap(baselineData -> {
                try {
                    if (selectionData.isEmpty()) {
                        throw new IllegalStateException("No data found for selection time range");
                    }
                    if (baselineData.isEmpty()) {
                        throw new IllegalStateException("No data found for baseline time range");
                    }
                    getComparisonDataDistribution(selectionData, baselineData, params.index, ActionListener.wrap(result -> {
                        listener.onResponse((T) gson.toJson(Map.of("comparisonAnalysis", result)));
                    }, listener::onFailure));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, listener::onFailure));
        }, listener::onFailure));
    }

    /**
     * Performs single dataset distribution analysis for the selection time range
     *
     * @param <T> The response type
     * @param params Analysis parameters containing selection time range
     * @param listener Action listener for handling single analysis results
     */
    private <T> void getSingleDataDistribution(AnalysisParameters params, ActionListener<T> listener) {
        fetchIndexData(params.selectionTimeRangeStart, params.selectionTimeRangeEnd, params, ActionListener.wrap(data -> {
            try {
                if (data.isEmpty()) {
                    throw new IllegalStateException("No data found for selection time range");
                }
                analyzeSingleDataset(data, params.index, ActionListener.wrap(result -> {
                    listener.onResponse((T) gson.toJson(Map.of("singleAnalysis", result)));
                }, listener::onFailure));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    /**
     * Formats time string to ISO 8601 format for OpenSearch compatibility
     *
     * @param timeString Input time string
     * @return Formatted time string in ISO 8601 format
     * @throws DateTimeParseException if time string cannot be parsed
     */
    private String formatTimeString(String timeString) throws DateTimeParseException {
        log.debug("Attempting to parse time string: {}", timeString);

        // Try parsing with zone first
        try {
            if (timeString.endsWith("Z")) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'", Locale.ROOT);
                ZonedDateTime dateTime = ZonedDateTime.parse(timeString, formatter.withZone(ZoneOffset.UTC));
                return dateTime.format(DateTimeFormatter.ISO_INSTANT);
            }
        } catch (DateTimeParseException e) {
            log.debug("Failed to parse as UTC time: {}", e.getMessage());
        }

        // Try parsing as local time without zone
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT_PATTERN, Locale.ROOT);
            LocalDateTime localDateTime = LocalDateTime.parse(timeString, formatter);
            ZonedDateTime zonedDateTime = localDateTime.atOffset(ZoneOffset.UTC).toZonedDateTime();
            return zonedDateTime.format(DateTimeFormatter.ISO_INSTANT);
        } catch (DateTimeParseException e) {
            log.debug("Failed to parse as local time: {}", e.getMessage());
        }

        // Try ISO format
        try {
            ZonedDateTime dateTime = ZonedDateTime.parse(timeString);
            return dateTime.format(DateTimeFormatter.ISO_INSTANT);
        } catch (DateTimeParseException e) {
            log.debug("Failed to parse as ISO format: {}", e.getMessage());
        }

        throw new DateTimeParseException("Unable to parse time string: " + timeString, timeString, 0);
    }

    /**
     * Fetches data from the specified index within the given time range
     *
     * @param startTime Start time for data retrieval
     * @param endTime End time for data retrieval
     * @param params Analysis parameters containing index and field information
     * @param listener Action listener for handling retrieved data
     */
    private void fetchIndexData(
        String startTime,
        String endTime,
        AnalysisParameters params,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        try {
            String formattedStartTime = formatTimeString(startTime);
            String formattedEndTime = formatTimeString(endTime);
            BoolQueryBuilder query;

            // Use raw DSL query if provided
            if (!Strings.isEmpty(params.dsl)) {
                try {
                    Map<String, Object> dslMap = gson.fromJson(params.dsl, new TypeToken<Map<String, Object>>() {
                    }.getType());
                    query = QueryBuilders.boolQuery();

                    // Handle DSL query structure - check if it has "query" wrapper
                    if (dslMap.containsKey("query")) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> queryMap = (Map<String, Object>) dslMap.get("query");
                        log.debug("Processing DSL query with wrapper: {}", queryMap);

                        // Build the DSL query directly into the main query
                        buildQueryFromMap(queryMap, query);

                        // Add time range filter
                        query.filter(new RangeQueryBuilder(params.timeField).gte(formattedStartTime).lte(formattedEndTime));
                    } else {
                        log.debug("Processing DSL query without wrapper: {}", dslMap);
                        buildQueryFromMap(dslMap, query);
                        // Add time range filter to the raw DSL query
                        query.filter(new RangeQueryBuilder(params.timeField).gte(formattedStartTime).lte(formattedEndTime));
                    }

                    log.debug("Final DSL query: {}", query.toString());
                } catch (Exception e) {
                    log.warn("Failed to parse raw DSL query: {}, falling back to time range only", params.dsl, e);
                    query = QueryBuilders
                        .boolQuery()
                        .filter(new RangeQueryBuilder(params.timeField).gte(formattedStartTime).lte(formattedEndTime));
                }
            } else {
                query = QueryBuilders
                    .boolQuery()
                    .filter(new RangeQueryBuilder(params.timeField).gte(formattedStartTime).lte(formattedEndTime));

                // Add additional filters if provided
                if (!params.filter.isEmpty()) {
                    for (String filterStr : params.filter) {
                        try {
                            Map<String, Object> filterMap = gson.fromJson(filterStr, new TypeToken<Map<String, Object>>() {
                            }.getType());
                            BoolQueryBuilder filterQuery = QueryBuilders.boolQuery();
                            buildQueryFromMap(filterMap, filterQuery);
                            query.must(filterQuery);
                        } catch (Exception e) {
                            log.warn("Failed to parse filter parameter: {}", filterStr, e);
                        }
                    }
                }
            }

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(query).size(params.size);

            SearchRequest request = new SearchRequest(params.index).source(sourceBuilder);

            client.search(request, ActionListener.wrap(response -> {
                List<Map<String, Object>> data = Arrays
                    .stream(response.getHits().getHits())
                    .map(SearchHit::getSourceAsMap)
                    .collect(Collectors.toList());
                listener.onResponse(data);
            }, listener::onFailure));
        } catch (Exception e) {
            log.error("Failed to format time strings: {}", e.getMessage());
            listener.onFailure(new IllegalArgumentException("Invalid time format: " + e.getMessage(), e));
        }
    }

    /**
     * Fetches data for both selection and baseline time ranges using PPL for comparison analysis
     *
     * @param <T> The response type
     * @param params Analysis parameters containing time ranges
     * @param listener Action listener for handling comparison results
     */
    private <T> void fetchPPLComparisonData(AnalysisParameters params, ActionListener<T> listener) {
        String selectionQuery = buildPPLQuery(
            params.index,
            params.timeField,
            params.selectionTimeRangeStart,
            params.selectionTimeRangeEnd,
            params.size,
            params.ppl
        );
        String baselineQuery = buildPPLQuery(
            params.index,
            params.timeField,
            params.baselineTimeRangeStart,
            params.baselineTimeRangeEnd,
            params.size,
            params.ppl
        );

        Function<Map<String, Object>, List<Map<String, Object>>> pplResultParser = pplResult -> {
            Object datarowsObj = pplResult.get("datarows");
            Object schemaObj = pplResult.get("schema");

            if (!(datarowsObj instanceof List) || !(schemaObj instanceof List)) {
                return List.of();
            }

            @SuppressWarnings("unchecked")
            List<List<Object>> dataRows = (List<List<Object>>) datarowsObj;
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> schema = (List<Map<String, Object>>) schemaObj;

            List<Map<String, Object>> result = new ArrayList<>();
            for (List<Object> row : dataRows) {
                Map<String, Object> doc = new HashMap<>();
                for (int i = 0; i < Math.min(row.size(), schema.size()); i++) {
                    String columnName = (String) schema.get(i).get("name");
                    if (columnName != null) {
                        doc.put(columnName, row.get(i));
                    }
                }
                result.add(doc);
            }
            return result;
        };

        PPLExecuteHelper.executePPLAndParseResult(client, selectionQuery, pplResultParser, ActionListener.wrap(selectionData -> {
            PPLExecuteHelper.executePPLAndParseResult(client, baselineQuery, pplResultParser, ActionListener.wrap(baselineData -> {
                try {
                    if (selectionData.isEmpty()) {
                        throw new IllegalStateException("No data found for selection time range");
                    }
                    if (baselineData.isEmpty()) {
                        throw new IllegalStateException("No data found for baseline time range");
                    }
                    getComparisonDataDistribution(selectionData, baselineData, params.index, ActionListener.wrap(result -> {
                        listener.onResponse((T) gson.toJson(Map.of("comparisonAnalysis", result)));
                    }, listener::onFailure));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, listener::onFailure));
        }, listener::onFailure));
    }

    /**
     * Converts time string to PPL format (yyyy-MM-dd HH:mm:ss)
     *
     * @param timeString Input time string
     * @return Formatted time string for PPL
     */
    private String formatTimeForPPL(String timeString) {
        try {
            // Parse ISO format and convert to PPL format
            ZonedDateTime dateTime = ZonedDateTime.parse(timeString);
            return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT));
        } catch (DateTimeParseException e) {
            // Try parsing as local time without zone
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT_PATTERN, Locale.ROOT);
                LocalDateTime localDateTime = LocalDateTime.parse(timeString, formatter);
                return localDateTime.format(formatter);
            } catch (DateTimeParseException e2) {
                // Return original if parsing fails
                return timeString;
            }
        }
    }

    /**
     * Adds time range filter to PPL query, similar to JavaScript version
     *
     * @param query PPL query string (can be empty)
     * @param startTime Start time for filtering
     * @param endTime End time for filtering
     * @param timeField Time field name
     * @return PPL query with time range filter added
     */
    private String getPPLQueryWithTimeRange(String query, String startTime, String endTime, String timeField) {
        if (Strings.isEmpty(query)) {
            throw new IllegalArgumentException("PPL query cannot be empty");
        }
        if (Strings.isEmpty(timeField)) {
            return query;
        }

        String formattedStartTime = formatTimeForPPL(startTime);
        String formattedEndTime = formatTimeForPPL(endTime);
        String timePredicate = String
            .format(Locale.ROOT, "`%s` >= '%s' AND `%s` <= '%s'", timeField, formattedStartTime, timeField, formattedEndTime);

        String[] commands = query.split("\\|");
        for (int i = 0; i < commands.length; i++) {
            String cmd = commands[i].trim();
            if (cmd.regionMatches(true, 0, "where", 0, 5)) {
                commands[i] = cmd + " AND " + timePredicate;
                return String.join(" | ", Arrays.stream(commands).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList()));
            }
        }

        // No WHERE found: insert after first command
        List<String> commandList = new ArrayList<>();
        commandList.add(commands[0].trim());
        commandList.add("WHERE " + timePredicate);
        for (int i = 1; i < commands.length; i++) {
            String cmd = commands[i].trim();
            if (!cmd.isEmpty()) {
                commandList.add(cmd);
            }
        }
        return String.join(" | ", commandList);
    }

    /**
     * Builds PPL query string for data retrieval within specified time range
     *
     * @param index Index name
     * @param timeField Time field name
     * @param startTime Start time for query
     * @param endTime End time for query
     * @param size Maximum number of documents
     * @param customPpl Custom PPL statement (optional)
     * @return Formatted PPL query string
     */
    private String buildPPLQuery(String index, String timeField, String startTime, String endTime, int size, String customPpl) {
        String baseQuery;

        if (!Strings.isEmpty(customPpl)) {
            baseQuery = getPPLQueryWithTimeRange(customPpl, startTime, endTime, timeField);
        } else {
            baseQuery = getPPLQueryWithTimeRange(String.format(Locale.ROOT, "source=%s", index), startTime, endTime, timeField);
        }

        return baseQuery + String.format(Locale.ROOT, " | head %d", size);
    }

    /**
     * Analyzes and compares data distributions between selection and baseline datasets
     *
     * @param selectionData Data from the selection time period
     * @param baselineData Data from the baseline time period
     * @param index Index name for field mapping retrieval
     * @param listener Action listener for handling comparison results
     */
    private void getComparisonDataDistribution(
        List<Map<String, Object>> selectionData,
        List<Map<String, Object>> baselineData,
        String index,
        ActionListener<List<SummaryDataItem>> listener
    ) {
        getFieldTypes(index, ActionListener.wrap(fieldTypes -> {
            try {
                List<String> usefulFields = getUsefulFields(selectionData, fieldTypes);
                Set<String> numberFields = getNumberFields(fieldTypes);
                List<FieldAnalysis> analyses = new ArrayList<>();

                for (String field : usefulFields) {
                    Map<String, Double> selectionDist = calculateFieldDistribution(selectionData, field);
                    Map<String, Double> baselineDist = calculateFieldDistribution(baselineData, field);

                    if (numberFields.contains(field)) {
                        GroupedDistributions grouped = groupNumericKeys(selectionDist, baselineDist);
                        selectionDist = grouped.groupedSelectionDist();
                        baselineDist = grouped.groupedBaselineDist();
                    }

                    double divergence = calculateMaxDifference(selectionDist, baselineDist);
                    analyses.add(new FieldAnalysis(field, divergence, selectionDist, baselineDist));
                }

                analyses.sort(Comparator.comparingDouble((FieldAnalysis a) -> a.divergence).reversed());
                listener.onResponse(formatComparisonSummary(analyses, DEFAULT_COMPARISON_RESULT_LIMIT));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    /**
     * Analyzes distribution patterns within a single dataset
     *
     * @param data Dataset to analyze
     * @param index Index name for field mapping retrieval
     * @param listener Action listener for handling single analysis results
     */
    private void analyzeSingleDataset(List<Map<String, Object>> data, String index, ActionListener<List<SummaryDataItem>> listener) {
        getFieldTypes(index, ActionListener.wrap(fieldTypes -> {
            try {
                List<String> usefulFields = getUsefulFields(data, fieldTypes);
                Set<String> numberFields = getNumberFields(fieldTypes);
                List<FieldAnalysis> analyses = new ArrayList<>();

                for (String field : usefulFields) {
                    Map<String, Double> selectionDist = calculateFieldDistribution(data, field);
                    Map<String, Double> baselineDist = new HashMap<>();

                    if (numberFields.contains(field)) {
                        GroupedDistributions grouped = groupNumericKeys(selectionDist, baselineDist);
                        selectionDist = grouped.groupedSelectionDist();
                    }

                    double divergence = calculateMaxDifference(selectionDist, baselineDist);
                    analyses.add(new FieldAnalysis(field, divergence, selectionDist, baselineDist));
                }

                analyses.sort(Comparator.comparingDouble((FieldAnalysis a) -> a.divergence).reversed());
                listener.onResponse(formatComparisonSummary(analyses, DEFAULT_SINGLE_ANALYSIS_RESULT_LIMIT));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    /**
     * Internal record for field analysis results
     */
    private record FieldAnalysis(String field, double divergence, Map<String, Double> selectionDist, Map<String, Double> baselineDist) {
    }

    /**
     * Record for grouped numeric distributions
     */
    private record GroupedDistributions(Map<String, Double> groupedSelectionDist, Map<String, Double> groupedBaselineDist) {
    }

    /**
     * Gets field type mappings from index
     *
     * @param index Index name for mapping retrieval
     * @param listener Action listener for handling field types result
     */
    private void getFieldTypes(String index, ActionListener<Map<String, String>> listener) {
        try {
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            client.admin().indices().getMappings(getMappingsRequest, ActionListener.wrap(response -> {
                try {
                    Map<String, MappingMetadata> mappings = response.getMappings();
                    if (mappings.isEmpty()) {
                        listener.onResponse(Map.of());
                        return;
                    }

                    MappingMetadata mappingMetadata = mappings.values().iterator().next();
                    Map<String, Object> mappingSource = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
                    if (mappingSource == null) {
                        listener.onResponse(Map.of());
                        return;
                    }

                    Map<String, String> fieldsToType = new HashMap<>();
                    ToolHelper.extractFieldNamesTypes(mappingSource, fieldsToType, "", true);
                    listener.onResponse(fieldsToType);
                } catch (Exception e) {
                    log.error("Failed to process field types for index: {}", index, e);
                    listener.onResponse(Map.of());
                }
            }, e -> {
                log.error("Failed to get field types for index: {}", index, e);
                listener.onResponse(Map.of());
            }));
        } catch (Exception e) {
            log.error("Failed to create getMappings request for index: {}", index, e);
            listener.onResponse(Map.of());
        }
    }

    /**
     * Identifies useful fields for analysis based on index mapping and data characteristics
     *
     * @param data Sample data for cardinality analysis
     * @param fieldTypes Map of field names to their types
     * @return List of field names suitable for distribution analysis
     */
    private List<String> getUsefulFields(List<Map<String, Object>> data, Map<String, String> fieldTypes) {
        if (fieldTypes.isEmpty()) {
            log.warn("No field types available, using data-based field detection");
            return getFieldsFromData(data);
        }

        Set<String> keywordFields = new HashSet<>();
        Set<String> numberFields = new HashSet<>();

        for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
            String fieldType = entry.getValue();
            String fieldName = entry.getKey();

            if (USEFUL_FIELD_TYPES.contains(fieldType)) {
                keywordFields.add(fieldName);
            }
            if (NUMBER_FIELD_TYPES.contains(fieldType)) {
                numberFields.add(fieldName);
            }
        }

        Set<String> normalizedFields = keywordFields
            .stream()
            .map(field -> field.endsWith(".keyword") ? field.replace(".keyword", "") : field)
            .collect(Collectors.toSet());

        Map<String, Set<String>> fieldValueSets = new HashMap<>();
        normalizedFields.forEach(field -> fieldValueSets.put(field, new HashSet<>()));

        int maxCardinality = Math.max(MIN_CARDINALITY_BASE, data.size() / MIN_CARDINALITY_DIVISOR);

        data.forEach(doc -> {
            normalizedFields.forEach(field -> {
                Object value = getFlattenedValue(doc, field);
                if (value != null) {
                    fieldValueSets.get(field).add(gson.toJson(value));
                }
            });
        });

        return normalizedFields.stream().filter(field -> {
            int cardinality = fieldValueSets.get(field).size();
            if (field.toLowerCase(Locale.ROOT).endsWith("id")) {
                return cardinality <= ID_FIELD_MAX_CARDINALITY && cardinality > 0;
            }
            if (numberFields.contains(field)) {
                return true;
            }
            return cardinality <= maxCardinality && cardinality > 0;
        }).collect(Collectors.toList());
    }

    /**
     * Extracts nested field values from document using dot notation
     *
     * @param doc Document map to extract value from
     * @param field Field path using dot notation (e.g., "user.name")
     * @return Field value or null if not found
     */
    private Object getFlattenedValue(Map<String, Object> doc, String field) {
        String[] parts = field.split("\\.");
        Object current = doc;

        for (String part : parts) {
            if (current instanceof Map) {
                current = ((Map<?, ?>) current).get(part);
            } else if (current instanceof List) {
                return gson.toJson(current);
            } else {
                return null;
            }
        }

        return current;
    }

    /**
     * Calculates distribution of values for a specific field across the dataset
     *
     * @param data Dataset to analyze
     * @param field Field name to calculate distribution for
     * @return Map of field values to their relative frequencies
     */
    private Map<String, Double> calculateFieldDistribution(List<Map<String, Object>> data, String field) {
        if (data == null || data.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, Integer> counts = new HashMap<>();

        for (Map<String, Object> doc : data) {
            Object value = getFlattenedValue(doc, field);
            if (value != null) {
                String strValue = String.valueOf(value);
                counts.merge(strValue, 1, Integer::sum);
            }
        }
        return counts.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> (double) entry.getValue() / data.size()));
    }

    /**
     * Calculates maximum difference between selection and baseline distributions
     *
     * @param selectionDist Selection period distribution
     * @param baselineDist Baseline period distribution
     * @return Maximum difference value across all field values
     */
    private double calculateMaxDifference(Map<String, Double> selectionDist, Map<String, Double> baselineDist) {
        Set<String> allKeys = new HashSet<>(selectionDist.keySet());
        allKeys.addAll(baselineDist.keySet());

        if (allKeys.isEmpty()) {
            return Double.NEGATIVE_INFINITY;
        }
        return allKeys.stream().mapToDouble(key -> {
            double selectionVal = selectionDist.getOrDefault(key, 0.0);
            double baselineVal = baselineDist.getOrDefault(key, 0.0);
            return selectionVal - baselineVal;
        }).max().orElse(Double.NEGATIVE_INFINITY);
    }

    /**
     * Extracts field names from sample data when mapping is not available
     *
     * @param data Sample data to analyze
     * @return List of field names suitable for analysis
     */
    private List<String> getFieldsFromData(List<Map<String, Object>> data) {
        if (data.isEmpty()) {
            return List.of();
        }

        Set<String> allFields = new HashSet<>();
        for (Map<String, Object> doc : data) {
            allFields.addAll(doc.keySet());
        }

        // Filter out timestamp and other non-useful fields
        return allFields
            .stream()
            .filter(field -> !field.equals("@timestamp") && !field.equals("_id") && !field.equals("_index"))
            .filter(field -> {
                // Check cardinality - exclude high cardinality fields
                Set<String> values = new HashSet<>();
                for (Map<String, Object> doc : data) {
                    Object value = doc.get(field);
                    if (value != null) {
                        values.add(String.valueOf(value));
                    }
                }
                int cardinality = values.size();
                return cardinality > 0 && cardinality <= Math.max(DATA_FIELD_MAX_CARDINALITY, data.size() / DATA_FIELD_CARDINALITY_DIVISOR);
            })
            .collect(Collectors.toList());
    }

    /**
     * Gets number fields from field type mappings
     *
     * @param fieldTypes Map of field names to their types
     * @return Set of number field names
     */
    private Set<String> getNumberFields(Map<String, String> fieldTypes) {
        return fieldTypes
            .entrySet()
            .stream()
            .filter(entry -> NUMBER_FIELD_TYPES.contains(entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    /**
     * Groups numeric keys and merges counts
     *
     * @param selectionDist Selection distribution
     * @param baselineDist Baseline distribution
     * @return Grouped distributions
     */
    private GroupedDistributions groupNumericKeys(Map<String, Double> selectionDist, Map<String, Double> baselineDist) {
        Set<String> allKeys = new HashSet<>(selectionDist.keySet());
        allKeys.addAll(baselineDist.keySet());

        if (allKeys.size() <= NUMERIC_GROUPING_THRESHOLD || allKeys.stream().anyMatch(key -> !NumberUtils.isCreatable(key))) {
            return new GroupedDistributions(selectionDist, baselineDist);
        }

        List<Double> numericKeys = allKeys.stream().map(Double::parseDouble).sorted().collect(Collectors.toList());
        Function<Double, String> getGroupLabel = getDoubleStringFunction(numericKeys);
        // Group the keys and aggregate the values
        Map<String, Double> groupedSelectionDist = numericKeys
            .stream()
            .collect(
                Collectors
                    .groupingBy(getGroupLabel, Collectors.summingDouble(numKey -> selectionDist.getOrDefault(String.valueOf(numKey), 0.0)))
            );
        Map<String, Double> groupedBaselineDist = numericKeys
            .stream()
            .collect(
                Collectors
                    .groupingBy(getGroupLabel, Collectors.summingDouble(numKey -> baselineDist.getOrDefault(String.valueOf(numKey), 0.0)))
            );
        // Ensure all groups are present in both maps (in case some have zero values)
        Set<String> allGroups = new HashSet<>();
        allGroups.addAll(groupedSelectionDist.keySet());
        allGroups.addAll(groupedBaselineDist.keySet());
        allGroups.forEach(group -> {
            groupedSelectionDist.putIfAbsent(group, 0.0);
            groupedBaselineDist.putIfAbsent(group, 0.0);
        });

        return new GroupedDistributions(groupedSelectionDist, groupedBaselineDist);
    }

    private static Function<Double, String> getDoubleStringFunction(List<Double> numericKeys) {
        double min = numericKeys.get(0);
        double max = numericKeys.get(numericKeys.size() - 1);
        double range = max - min;
        int numGroups = 5;
        double groupSize = range / numGroups;
        // Create a function to determine which group a key belongs to
        Function<Double, String> getGroupLabel = numKey -> {
            int groupIndex = numKey == max ? numGroups - 1 : (int) ((numKey - min) / groupSize);
            double lowerBound = min + groupIndex * groupSize;
            double upperBound = groupIndex == numGroups - 1 ? max : min + (groupIndex + 1) * groupSize;
            return String.format(Locale.ROOT, "%.1f-%.1f", lowerBound, upperBound);
        };
        return getGroupLabel;
    }

    /**
     * Formats field analysis results into summary data items
     *
     * @param differences List of field analysis results
     * @param maxResults Maximum number of results to return
     * @return Formatted list of summary data items
     */
    private List<SummaryDataItem> formatComparisonSummary(List<FieldAnalysis> differences, int maxResults) {
        return differences.stream().filter(diff -> diff.divergence > 0).limit(maxResults).map(diff -> {
            Set<String> allKeys = new HashSet<>(diff.selectionDist.keySet());
            allKeys.addAll(diff.baselineDist.keySet());

            List<ChangeItem> changes = allKeys.stream().map(value -> {
                double selectionPercentage = Math.round(diff.selectionDist.getOrDefault(value, 0.0) * PERCENTAGE_MULTIPLIER)
                    / PERCENTAGE_MULTIPLIER;
                double baselinePercentage = Math.round(diff.baselineDist.getOrDefault(value, 0.0) * PERCENTAGE_MULTIPLIER)
                    / PERCENTAGE_MULTIPLIER;
                return new ChangeItem(value, selectionPercentage, baselinePercentage);
            }).collect(Collectors.toList());

            List<ChangeItem> topChanges = changes
                .stream()
                .sorted(
                    (a, b) -> Double
                        .compare(
                            Math.max(b.baselinePercentage, b.selectionPercentage),
                            Math.max(a.baselinePercentage, a.selectionPercentage)
                        )
                )
                .limit(TOP_CHANGES_LIMIT)
                .collect(Collectors.toList());

            return new SummaryDataItem(diff.field, diff.divergence, topChanges);
        }).collect(Collectors.toList());
    }

    /**
     * Builds query conditions from filter map for DSL queries
     *
     * @param filterMap Filter conditions as map
     * @param queryBuilder Query builder to add conditions to
     */
    private void buildQueryFromMap(Map<String, Object> filterMap, BoolQueryBuilder queryBuilder) {
        log.debug("Building query from map: {}", filterMap);

        for (Map.Entry<String, Object> entry : filterMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            log.debug("Processing query key: {}, value: {}", key, value);

            // Handle special query types
            switch (key) {
                case "match_all" -> {
                    // {"match_all": {}}
                    log.debug("Adding match_all query");
                    queryBuilder.must(QueryBuilders.matchAllQuery());
                }
                case "match_none" -> {
                    // {"match_none": {}}
                    log.debug("Adding match_none query");
                    queryBuilder.mustNot(QueryBuilders.matchAllQuery());
                }
                case "bool" -> {
                    if (value instanceof Map) {
                        log.debug("Processing bool query: {}", value);
                        processBoolQuery((Map<String, Object>) value, queryBuilder);
                    }
                }
                case "term" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        log.debug("Adding term query: {}", valueMap);
                        // {"term": {"field": "value"}}
                        for (Map.Entry<String, Object> termEntry : valueMap.entrySet()) {
                            log.debug("Term query - field: {}, value: {}", termEntry.getKey(), termEntry.getValue());
                            queryBuilder.must(QueryBuilders.termQuery(termEntry.getKey(), termEntry.getValue()));
                        }
                    }
                }
                case "wildcard" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        log.debug("Adding wildcard query: {}", valueMap);
                        // {"wildcard": {"field": "pattern"}}
                        for (Map.Entry<String, Object> wildcardEntry : valueMap.entrySet()) {
                            log.debug("Wildcard query - field: {}, pattern: {}", wildcardEntry.getKey(), wildcardEntry.getValue());
                            queryBuilder.must(QueryBuilders.wildcardQuery(wildcardEntry.getKey(), wildcardEntry.getValue().toString()));
                        }
                    }
                }
                case "range" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        // {"range": {"field": {"gte": 1, "lte": 10}}}
                        for (Map.Entry<String, Object> rangeEntry : valueMap.entrySet()) {
                            String field = rangeEntry.getKey();
                            Object rangeValue = rangeEntry.getValue();
                            if (rangeValue instanceof Map) {
                                processRangeQuery(field, rangeValue, queryBuilder);
                            }
                        }
                    }
                }
                case "match" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        // {"match": {"field": "value"}}
                        for (Map.Entry<String, Object> matchEntry : valueMap.entrySet()) {
                            queryBuilder.must(QueryBuilders.matchQuery(matchEntry.getKey(), matchEntry.getValue()));
                        }
                    }
                }
                case "match_phrase" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        // {"match_phrase": {"field": "value"}}
                        for (Map.Entry<String, Object> matchPhraseEntry : valueMap.entrySet()) {
                            queryBuilder.must(QueryBuilders.matchPhraseQuery(matchPhraseEntry.getKey(), matchPhraseEntry.getValue()));
                        }
                    }
                }
                case "prefix" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        // {"prefix": {"field": "value"}}
                        for (Map.Entry<String, Object> prefixEntry : valueMap.entrySet()) {
                            queryBuilder.must(QueryBuilders.prefixQuery(prefixEntry.getKey(), prefixEntry.getValue().toString()));
                        }
                    }
                }
                case "exists" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        // {"exists": {"field": "fieldname"}}
                        Object fieldValue = valueMap.get("field");
                        if (fieldValue != null) {
                            queryBuilder.must(QueryBuilders.existsQuery(fieldValue.toString()));
                        }
                    }
                }
                case "regexp" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        // {"regexp": {"field": "pattern"}}
                        for (Map.Entry<String, Object> regexpEntry : valueMap.entrySet()) {
                            queryBuilder.must(QueryBuilders.regexpQuery(regexpEntry.getKey(), regexpEntry.getValue().toString()));
                        }
                    }
                }
                case "terms" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        // {"terms": {"field": ["value1", "value2"]}}
                        for (Map.Entry<String, Object> termsEntry : valueMap.entrySet()) {
                            if (termsEntry.getValue() instanceof List) {
                                queryBuilder.must(QueryBuilders.termsQuery(termsEntry.getKey(), (List<?>) termsEntry.getValue()));
                            }
                        }
                    }
                }
                case "multi_match" -> {
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        Object queryValue = valueMap.get("query");
                        Object fieldsValue = valueMap.get("fields");
                        if (queryValue != null && fieldsValue instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<String> fields = (List<String>) fieldsValue;
                            queryBuilder.must(QueryBuilders.multiMatchQuery(queryValue, fields.toArray(new String[0])));
                        }
                    }
                }
                default -> {
                    // Handle direct field-value pairs or unknown query types
                    if (value instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) value;
                        // This might be a field with nested operators like {"field": {"term": "value"}}
                        processNestedQuery(key, valueMap, queryBuilder);
                    } else {
                        // Direct field-value mapping
                        queryBuilder.must(QueryBuilders.termQuery(key, value));
                    }
                }
            }
        }
    }

    /**
     * Processes bool query conditions
     *
     * @param boolMap Bool query conditions
     * @param queryBuilder Query builder to add conditions to
     */
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
                        default -> log.warn("Unsupported bool query type: {}", boolType);
                    }
                }
            }
        }
    }

    /**
     * Processes nested query conditions for a field
     *
     * @param field Field name
     * @param nestedMap Nested query conditions
     * @param queryBuilder Query builder to add conditions to
     */
    private void processNestedQuery(String field, Map<String, Object> nestedMap, BoolQueryBuilder queryBuilder) {
        for (Map.Entry<String, Object> nestedEntry : nestedMap.entrySet()) {
            String operator = nestedEntry.getKey();
            Object operatorValue = nestedEntry.getValue();

            switch (operator) {
                case "term" -> queryBuilder.must(QueryBuilders.termQuery(field, operatorValue));
                case "range" -> processRangeQuery(field, operatorValue, queryBuilder);
                case "match" -> queryBuilder.must(QueryBuilders.matchQuery(field, operatorValue));
                case "match_phrase" -> queryBuilder.must(QueryBuilders.matchPhraseQuery(field, operatorValue));
                case "prefix" -> queryBuilder.must(QueryBuilders.prefixQuery(field, operatorValue.toString()));
                case "wildcard" -> processWildcardQuery(field, operatorValue, queryBuilder);
                case "exists" -> queryBuilder.must(QueryBuilders.existsQuery(field));
                case "regexp" -> processRegexpQuery(field, operatorValue, queryBuilder);
                default -> {
                    // Handle direct field-value mapping for nested structures
                    if (operatorValue instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> valueMap = (Map<String, Object>) operatorValue;
                        BoolQueryBuilder nestedQuery = QueryBuilders.boolQuery();
                        buildQueryFromMap(Map.of(operator, valueMap), nestedQuery);
                        queryBuilder.must(nestedQuery);
                    } else {
                        log.warn("Unsupported query operator: {}", operator);
                    }
                }
            }
        }
    }

    /**
     * Processes range query conditions
     *
     * @param field Field name
     * @param operatorValue Range conditions
     * @param queryBuilder Query builder to add conditions to
     */
    private void processRangeQuery(String field, Object operatorValue, BoolQueryBuilder queryBuilder) {
        if (!(operatorValue instanceof Map)) {
            return;
        }

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

    /**
     * Processes wildcard query conditions
     *
     * @param field Field name
     * @param operatorValue Wildcard conditions
     * @param queryBuilder Query builder to add conditions to
     */
    private void processWildcardQuery(String field, Object operatorValue, BoolQueryBuilder queryBuilder) {
        if (operatorValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> wildcardMap = (Map<String, Object>) operatorValue;
            Object wildcardValue = wildcardMap.get("value");
            if (wildcardValue != null) {
                queryBuilder.must(QueryBuilders.wildcardQuery(field, wildcardValue.toString()));
            }
        } else {
            queryBuilder.must(QueryBuilders.wildcardQuery(field, operatorValue.toString()));
        }
    }

    /**
     * Processes regexp query conditions
     *
     * @param field Field name
     * @param operatorValue Regexp conditions
     * @param queryBuilder Query builder to add conditions to
     */
    private void processRegexpQuery(String field, Object operatorValue, BoolQueryBuilder queryBuilder) {
        if (operatorValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> regexpMap = (Map<String, Object>) operatorValue;
            Object regexpValue = regexpMap.get("value");
            if (regexpValue != null) {
                queryBuilder.must(QueryBuilders.regexpQuery(field, regexpValue.toString()));
            }
        } else {
            queryBuilder.must(QueryBuilders.regexpQuery(field, operatorValue.toString()));
        }
    }

    /**
     * Factory class for creating DataDistributionTool instances
     */
    public static class Factory implements Tool.Factory<DataDistributionTool> {
        private Client client;
        private static Factory INSTANCE;

        /**
         * Create or return the singleton factory instance
         */
        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (DataDistributionTool.class) {
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
        public DataDistributionTool create(Map<String, Object> map) {
            return new DataDistributionTool(client);
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
