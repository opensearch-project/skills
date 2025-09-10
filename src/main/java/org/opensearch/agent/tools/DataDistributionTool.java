/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.tools.utils.ToolHelper.getPPLTransportActionListener;
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

import org.json.JSONObject;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.search.SearchRequest;
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
 *     "ppl": "source index where a=0"
 *   }
 * }
 * 3. Result: analysis of data distribution patterns
 * {
 *   "inference_results": [
 *     {
 *       "output": [
 *         {
 *           "name": "response",
 *           "result": """{"comparisonAnalysis": [{"field": "status", "divergence": 0.75, "selectionDist": {"error": 0.3}, "baselineDist": {"error": 0.1}}]}"""
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

    private static final Set<String> USEFUL_FIELD_TYPES = Set
        .of("keyword", "boolean", "text", "byte", "short", "integer", "long", "float", "double", "half_float", "scaled_float");
    private static final Set<String> NUMBER_FIELD_TYPES = Set
        .of("byte", "short", "integer", "long", "float", "double", "half_float", "scaled_float");

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
        final String ppl;

        /**
         * Constructs analysis parameters from input map with default values
         *
         * @param parameters Input parameter map from user request
         */
        AnalysisParameters(Map<String, String> parameters) {
            this.index = parameters.getOrDefault("index", "");
            this.timeField = parameters.getOrDefault("timeField", DEFAULT_TIME_FIELD);
            this.selectionTimeRangeStart = parameters.getOrDefault("selectionTimeRangeStart", "");
            this.selectionTimeRangeEnd = parameters.getOrDefault("selectionTimeRangeEnd", "");
            this.baselineTimeRangeStart = parameters.getOrDefault("baselineTimeRangeStart", "");
            this.baselineTimeRangeEnd = parameters.getOrDefault("baselineTimeRangeEnd", "");

            try {
                this.size = Integer.parseInt(parameters.getOrDefault("size", "1000"));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid 'size' parameter: must be a valid integer, got '" + parameters.get("size") + "'"
                );
            }

            this.queryType = parameters.getOrDefault("queryType", "dsl");

            String filterParam = parameters.getOrDefault("filter", "");
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

            this.ppl = parameters.getOrDefault("ppl", "");
        }

        /**
         * Validates required parameters are present
         *
         * @throws IllegalArgumentException if required parameters are missing
         */
        void validate() {
            List<String> missingParams = new ArrayList<>();
            if (Strings.isEmpty(index))
                missingParams.add("index");
            if (Strings.isEmpty(selectionTimeRangeStart))
                missingParams.add("selectionTimeRangeStart");
            if (Strings.isEmpty(selectionTimeRangeEnd))
                missingParams.add("selectionTimeRangeEnd");
            if (Strings.isEmpty(timeField))
                missingParams.add("timeField");
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
            log.info("Starting data distribution analysis with parameters: {}", parameters.keySet());
            AnalysisParameters params = new AnalysisParameters(parameters);
            params.validate();

            if ("ppl".equals(params.queryType)) {
                executePPLAnalysis(params, listener);
            } else {
                executeDSLAnalysis(params, listener);
            }
        } catch (IllegalArgumentException e) {
            log.error("Invalid parameters for DataDistributionTool: {}", e.getMessage());
            listener.onFailure(e);
        } catch (Exception e) {
            log.error("Unexpected error in DataDistributionTool", e);
            listener.onFailure(new RuntimeException("Failed to execute data distribution analysis: " + e.getMessage(), e));
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

            executePPLAndParseResult(pplQuery, pplResultParser, ActionListener.wrap(data -> {
                try {
                    List<SummaryDataItem> result = analyzeSingleDataset(data, params.index);
                    listener.onResponse((T) gson.toJson(Map.of("singleAnalysis", result)));
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
                    List<SummaryDataItem> result = getComparisonDataDistribution(selectionData, baselineData, params.index);
                    listener.onResponse((T) gson.toJson(Map.of("comparisonAnalysis", result)));
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
                List<SummaryDataItem> result = analyzeSingleDataset(data, params.index);
                listener.onResponse((T) gson.toJson(Map.of("singleAnalysis", result)));
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
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
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
            BoolQueryBuilder query = QueryBuilders
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

        executePPLAndParseResult(selectionQuery, pplResultParser, ActionListener.wrap(selectionData -> {
            executePPLAndParseResult(baselineQuery, pplResultParser, ActionListener.wrap(baselineData -> {
                try {
                    if (selectionData.isEmpty()) {
                        throw new IllegalStateException("No data found for selection time range");
                    }
                    if (baselineData.isEmpty()) {
                        throw new IllegalStateException("No data found for baseline time range");
                    }
                    List<SummaryDataItem> result = getComparisonDataDistribution(selectionData, baselineData, params.index);
                    listener.onResponse((T) gson.toJson(Map.of("comparisonAnalysis", result)));
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
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
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
        if (Strings.isEmpty(timeField)) {
            return query;
        }

        String formattedStartTime = formatTimeForPPL(startTime);
        String formattedEndTime = formatTimeForPPL(endTime);
        String timePredicate = String.format(Locale.ROOT, "`%s` >= '%s' AND `%s` <= '%s'", timeField, formattedStartTime, timeField, formattedEndTime);

        if (Strings.isEmpty(query)) {
            return "WHERE " + timePredicate;
        }

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
     * Executes PPL query and parses the result using provided result parser
     *
     * @param <T> The parsed result type
     * @param ppl PPL query string to execute
     * @param resultParser Function to parse PPL result into desired format
     * @param listener Action listener for handling parsed results or failures
     */
    private <T> void executePPLAndParseResult(String ppl, Function<Map<String, Object>, T> resultParser, ActionListener<T> listener) {
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
                            if (pplResult.containsKey("error")) {
                                Object errorObj = pplResult.get("error");
                                String errorDetail = errorObj != null ? errorObj.toString() : "Unknown error";
                                throw new RuntimeException("PPL query error: " + errorDetail);
                            }

                            Object datarowsObj = pplResult.get("datarows");
                            if (!(datarowsObj instanceof List)) {
                                throw new IllegalStateException("Invalid PPL response format: missing or invalid datarows");
                            }

                            listener.onResponse(resultParser.apply(pplResult));
                        }
                    }, error -> {
                        log.error("PPL execution failed: {}", error.getMessage());
                        listener.onFailure(new RuntimeException("PPL execution failed: " + error.getMessage(), error));
                    }))
                );
        } catch (Exception e) {
            String errorMessage = String.format(Locale.ROOT, "Failed to execute PPL query: %s", e.getMessage());
            log.error(errorMessage, e);
            listener.onFailure(new RuntimeException(errorMessage, e));
        }
    }

    /**
     * Analyzes and compares data distributions between selection and baseline datasets
     *
     * @param selectionData Data from the selection time period
     * @param baselineData Data from the baseline time period
     * @param index Index name for field mapping retrieval
     * @return List of summary data items showing distribution differences
     */
    private List<SummaryDataItem> getComparisonDataDistribution(
        List<Map<String, Object>> selectionData,
        List<Map<String, Object>> baselineData,
        String index
    ) {
        List<String> usefulFields = getUsefulFields(selectionData, index);
        Set<String> numberFields = getNumberFields(index);
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
        return formatComparisonSummary(analyses, 10);
    }

    /**
     * Analyzes distribution patterns within a single dataset
     *
     * @param data Dataset to analyze
     * @param index Index name for field mapping retrieval
     * @return List of summary data items showing distribution patterns
     */
    private List<SummaryDataItem> analyzeSingleDataset(List<Map<String, Object>> data, String index) {
        List<String> usefulFields = getUsefulFields(data, index);
        Set<String> numberFields = getNumberFields(index);
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
        return formatComparisonSummary(analyses, 30);
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
     * Identifies useful fields for analysis based on index mapping and data characteristics
     *
     * @param data Sample data for cardinality analysis
     * @param index Index name for mapping retrieval
     * @return List of field names suitable for distribution analysis
     */
    private List<String> getUsefulFields(List<Map<String, Object>> data, String index) {
        try {
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            var getMappingsActionFuture = client.admin().indices().getMappings(getMappingsRequest);
            if (getMappingsActionFuture == null) {
                log.warn("Failed to get mappings for index: {}, using data-based field detection", index);
                return getFieldsFromData(data);
            }
            Map<String, MappingMetadata> mappings = getMappingsActionFuture.actionGet(5000).getMappings();
            if (mappings.isEmpty()) {
                log.warn("No mappings found for index: {}, using data-based field detection", index);
                return getFieldsFromData(data);
            }

            MappingMetadata mappingMetadata = mappings.values().iterator().next();
            Map<String, Object> mappingSource = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
            if (mappingSource == null) {
                return List.of();
            }

            Map<String, String> fieldsToType = new HashMap<>();
            ToolHelper.extractFieldNamesTypes(mappingSource, fieldsToType, "", true);

            Set<String> keywordFields = fieldsToType
                .entrySet()
                .stream()
                .filter(entry -> USEFUL_FIELD_TYPES.contains(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            Set<String> numberFields = fieldsToType
                .entrySet()
                .stream()
                .filter(entry -> NUMBER_FIELD_TYPES.contains(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            Set<String> normalizedFields = keywordFields
                .stream()
                .map(field -> field.endsWith(".keyword") ? field.replace(".keyword", "") : field)
                .collect(Collectors.toSet());

            Map<String, Set<String>> fieldValueSets = new HashMap<>();
            normalizedFields.forEach(field -> fieldValueSets.put(field, new HashSet<>()));

            int maxCardinality = Math.max(5, data.size() / 4);

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
                    return cardinality <= 30 && cardinality > 0;
                }
                if (numberFields.contains(field)) {
                    return true;
                }
                return cardinality <= maxCardinality && cardinality > 0;
            }).collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Failed to get useful fields for index: {}, using data-based field detection", index, e);
            return getFieldsFromData(data);
        }
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
                return cardinality > 0 && cardinality <= Math.max(10, data.size() / 2);
            })
            .collect(Collectors.toList());
    }

    /**
     * Gets number fields from index mapping
     *
     * @param index Index name
     * @return Set of number field names
     */
    private Set<String> getNumberFields(String index) {
        try {
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            var getMappingsActionFuture = client.admin().indices().getMappings(getMappingsRequest);
            if (getMappingsActionFuture == null) {
                return Set.of();
            }
            Map<String, MappingMetadata> mappings = getMappingsActionFuture.actionGet(5000).getMappings();
            if (mappings.isEmpty()) {
                return Set.of();
            }

            MappingMetadata mappingMetadata = mappings.values().iterator().next();
            Map<String, Object> mappingSource = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
            if (mappingSource == null) {
                return Set.of();
            }

            Map<String, String> fieldsToType = new HashMap<>();
            ToolHelper.extractFieldNamesTypes(mappingSource, fieldsToType, "", true);

            return fieldsToType
                .entrySet()
                .stream()
                .filter(entry -> NUMBER_FIELD_TYPES.contains(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        } catch (Exception e) {
            log.warn("Failed to get number fields for index: {}", index, e);
            return Set.of();
        }
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

        if (allKeys.size() <= 10 || !allKeys.stream().allMatch(key -> {
            try {
                Double.parseDouble(key);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        })) {
            return new GroupedDistributions(selectionDist, baselineDist);
        }

        List<Double> numericKeys = allKeys.stream().map(Double::parseDouble).sorted().collect(Collectors.toList());
        double min = numericKeys.get(0);
        double max = numericKeys.get(numericKeys.size() - 1);
        double range = max - min;
        int numGroups = 5;
        double groupSize = range / numGroups;

        Map<String, Double> groupedSelectionDist = new HashMap<>();
        Map<String, Double> groupedBaselineDist = new HashMap<>();

        for (int i = 0; i < numGroups; i++) {
            double lowerBound = min + i * groupSize;
            double upperBound = i == numGroups - 1 ? max : min + (i + 1) * groupSize;
            String groupLabel = String.format(Locale.ROOT, "%.1f-%.1f", lowerBound, upperBound);

            groupedSelectionDist.put(groupLabel, 0.0);
            groupedBaselineDist.put(groupLabel, 0.0);

            for (double numKey : numericKeys) {
                boolean belongs = i == numGroups - 1
                    ? numKey >= lowerBound && numKey <= upperBound
                    : numKey >= lowerBound && numKey < upperBound;
                if (belongs) {
                    String strKey = String.valueOf(numKey);
                    groupedSelectionDist.merge(groupLabel, selectionDist.getOrDefault(strKey, 0.0), Double::sum);
                    groupedBaselineDist.merge(groupLabel, baselineDist.getOrDefault(strKey, 0.0), Double::sum);
                }
            }
        }

        return new GroupedDistributions(groupedSelectionDist, groupedBaselineDist);
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
                double selectionPercentage = Math.round(diff.selectionDist.getOrDefault(value, 0.0) * 100.0) / 100.0;
                double baselinePercentage = Math.round(diff.baselineDist.getOrDefault(value, 0.0) * 100.0) / 100.0;
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
                .limit(10)
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
        for (Map.Entry<String, Object> entry : filterMap.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nestedMap = (Map<String, Object>) value;
                for (Map.Entry<String, Object> nestedEntry : nestedMap.entrySet()) {
                    String operator = nestedEntry.getKey();
                    Object operatorValue = nestedEntry.getValue();

                    switch (operator) {
                        case "term":
                            queryBuilder.must(QueryBuilders.termQuery(field, operatorValue));
                            break;
                        case "range":
                            if (operatorValue instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> rangeMap = (Map<String, Object>) operatorValue;
                                RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(field);
                                rangeMap.forEach((rangeOp, rangeVal) -> {
                                    switch (rangeOp) {
                                        case "gte":
                                            rangeQuery.gte(rangeVal);
                                            break;
                                        case "lte":
                                            rangeQuery.lte(rangeVal);
                                            break;
                                        case "gt":
                                            rangeQuery.gt(rangeVal);
                                            break;
                                        case "lt":
                                            rangeQuery.lt(rangeVal);
                                            break;
                                    }
                                });
                                queryBuilder.must(rangeQuery);
                            }
                            break;
                        case "match":
                            queryBuilder.must(QueryBuilders.matchQuery(field, operatorValue));
                            break;
                        case "match_phrase":
                            queryBuilder.must(QueryBuilders.matchPhraseQuery(field, operatorValue));
                            break;
                        case "prefix":
                            queryBuilder.must(QueryBuilders.prefixQuery(field, operatorValue.toString()));
                            break;
                        case "wildcard":
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
                            break;
                        case "exists":
                            queryBuilder.must(QueryBuilders.existsQuery(field));
                            break;
                        case "regexp":
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
                            break;
                        default:
                            log.warn("Unsupported query operator: {}", operator);
                            break;
                    }
                }
            } else {
                queryBuilder.must(QueryBuilders.termQuery(field, value));
            }
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
