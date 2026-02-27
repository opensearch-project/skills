/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.agent.tools.utils.PPLExecuteHelper;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import com.google.gson.reflect.TypeToken;

import lombok.extern.log4j.Log4j2;

/**
 * Helper class for fetching and processing data from OpenSearch indices.
 * Provides common functionality for data analysis tools including:
 * - Field type detection and mapping
 * - Time-based data fetching with DSL/PPL query support
 * - Query building and parameter validation
 * - Nested field value extraction
 */
@Log4j2
public class DataFetchingHelper {

    private static final String DEFAULT_TIME_FIELD = "@timestamp";
    public static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String QUERY_TYPE_PPL = "ppl";
    public static final String QUERY_TYPE_DSL = "dsl";
    private static final String DEFAULT_SIZE = "1000";
    private static final int MAX_SIZE_LIMIT = 10000;

    public static final Set<String> NUMBER_FIELD_TYPES = Set
        .of("byte", "short", "integer", "long", "float", "double", "half_float", "scaled_float");

    private final Client client;

    /**
     * Constructs a DataFetchingHelper with the given OpenSearch client
     *
     * @param client The OpenSearch client for executing queries
     */
    public DataFetchingHelper(Client client) {
        this.client = client;
    }

    /**
     * Parameters for data analysis operations
     */
    public static class AnalysisParameters {
        public final String index;
        public final String timeField;
        public final String selectionTimeRangeStart;
        public final String selectionTimeRangeEnd;
        public final String baselineTimeRangeStart;
        public final String baselineTimeRangeEnd;
        public final int size;
        public final String queryType;
        public final List<String> filter;
        public final String dsl;
        public final String ppl;

        public AnalysisParameters(Map<String, String> parameters) {
            this.index = parameters.getOrDefault("index", "");
            this.timeField = parameters.getOrDefault("timeField", DEFAULT_TIME_FIELD);
            this.selectionTimeRangeStart = parameters.getOrDefault("selectionTimeRangeStart", "");
            this.selectionTimeRangeEnd = parameters.getOrDefault("selectionTimeRangeEnd", "");
            this.baselineTimeRangeStart = parameters.getOrDefault("baselineTimeRangeStart", "");
            this.baselineTimeRangeEnd = parameters.getOrDefault("baselineTimeRangeEnd", "");

            String sizeStr = parameters.getOrDefault("size", DEFAULT_SIZE);
            int parsedSize;
            try {
                parsedSize = Double.valueOf(sizeStr).intValue();
                if (parsedSize <= 0 || parsedSize > MAX_SIZE_LIMIT) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Invalid 'size' parameter: must be between 1 and %d, got '%s'", MAX_SIZE_LIMIT, sizeStr)
                    );
                }
            } catch (NumberFormatException e) {
                // fallback to default
                parsedSize = Integer.parseInt(DEFAULT_SIZE);
            }
            this.size = parsedSize;

            this.queryType = parameters.getOrDefault("queryType", QUERY_TYPE_DSL);

            // Parse filter from JSON string to List<String>
            String filterParam = parameters.getOrDefault("filter", "");
            if (Strings.isNullOrEmpty(filterParam)) {
                this.filter = List.of();
            } else {
                try {
                    this.filter = List.of(gson.fromJson(filterParam, String[].class));
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                        String
                            .format(
                                Locale.ROOT,
                                "Invalid 'filter' parameter: must be a valid JSON array of strings, got '%s'. "
                                    + "Example: [\"{'term': {'status': 'error'}}\", \"{'range': {'level': {'gte': 3}}}\"]",
                                filterParam
                            )
                    );
                }
            }

            this.dsl = parameters.getOrDefault("dsl", "");
            this.ppl = parameters.getOrDefault("ppl", "");
        }

        /**
         * Validates required parameters are present
         *
         * @throws IllegalArgumentException if required parameters are missing
         */
        public void validate() {
            if (Strings.isNullOrEmpty(index)) {
                throw new IllegalArgumentException("Missing required parameter: 'index'");
            }
            if (Strings.isNullOrEmpty(selectionTimeRangeStart) || Strings.isNullOrEmpty(selectionTimeRangeEnd)) {
                throw new IllegalArgumentException("Missing required parameters: 'selectionTimeRangeStart' and 'selectionTimeRangeEnd'");
            }
            if (!QUERY_TYPE_DSL.equals(queryType) && !QUERY_TYPE_PPL.equals(queryType)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid 'queryType': must be 'dsl' or 'ppl', got '%s'", queryType)
                );
            }
        }

        public boolean hasBaselineTimeRange() {
            return !Strings.isNullOrEmpty(baselineTimeRangeStart) && !Strings.isNullOrEmpty(baselineTimeRangeEnd);
        }
    }

    /**
     * Retrieves field type mappings from the specified index
     *
     * @param index The index name
     * @param listener Action listener for handling the field type map or failures
     */
    public void getFieldTypes(String index, ActionListener<Map<String, String>> listener) {
        GetMappingsRequest request = new GetMappingsRequest().indices(index);

        client.admin().indices().getMappings(request, ActionListener.wrap(response -> {
            Map<String, String> fieldTypes = new HashMap<>();

            for (Map.Entry<String, MappingMetadata> entry : response.getMappings().entrySet()) {
                MappingMetadata metadata = entry.getValue();
                Map<String, Object> mappingSource = metadata.getSourceAsMap();

                if (mappingSource.containsKey("properties")) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> properties = (Map<String, Object>) mappingSource.get("properties");
                    extractFieldTypes(properties, "", fieldTypes);
                }
            }

            listener.onResponse(fieldTypes);
        }, listener::onFailure));
    }

    /**
     * Recursively extracts field types from mapping properties
     */
    private void extractFieldTypes(Map<String, Object> properties, String prefix, Map<String, String> fieldTypes) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();

            @SuppressWarnings("unchecked")
            Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();

            if (fieldProps.containsKey("type")) {
                fieldTypes.put(fieldName, (String) fieldProps.get("type"));
            }

            if (fieldProps.containsKey("properties")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nestedProps = (Map<String, Object>) fieldProps.get("properties");
                extractFieldTypes(nestedProps, fieldName, fieldTypes);
            }
        }
    }

    /**
     * Fetches data from index within the specified time range
     *
     * @param timeRangeStart Start time string
     * @param timeRangeEnd End time string
     * @param params Analysis parameters
     * @param listener Action listener for handling the fetched data or failures
     */
    public void fetchIndexData(
        String timeRangeStart,
        String timeRangeEnd,
        AnalysisParameters params,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        try {
            if (QUERY_TYPE_PPL.equals(params.queryType)) {
                fetchDataWithPPL(timeRangeStart, timeRangeEnd, params, listener);
            } else {
                fetchDataWithDSL(timeRangeStart, timeRangeEnd, params, listener);
            }
        } catch (Exception e) {
            log.error("Failed to fetch index data", e);
            listener.onFailure(e);
        }
    }

    /**
     * Builds a BoolQueryBuilder with time range filter and optional custom filters.
     * Can be used with any SearchSourceBuilder (for documents or aggregations).
     *
     * @param timeRangeStart Start time string
     * @param timeRangeEnd End time string
     * @param params Analysis parameters containing timeField, dsl, and filter settings
     * @return BoolQueryBuilder with time range and custom filters applied
     */
    public BoolQueryBuilder buildFilterQuery(String timeRangeStart, String timeRangeEnd, AnalysisParameters params) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // Add time range filter
        RangeQueryBuilder timeRangeQuery = QueryBuilders
            .rangeQuery(params.timeField)
            .gte(formatTimeString(timeRangeStart))
            .lte(formatTimeString(timeRangeEnd))
            .format("strict_date_optional_time||epoch_millis");
        boolQuery.must(timeRangeQuery);

        // Add custom query if provided
        if (!Strings.isNullOrEmpty(params.dsl)) {
            Map<String, Object> dslMap = buildQueryFromMap(params.dsl);
            boolQuery.must(QueryBuilders.wrapperQuery(gson.toJson(dslMap)));
        } else if (!params.filter.isEmpty()) {
            for (String filterStr : params.filter) {
                Map<String, Object> filterMap = buildQueryFromMap(filterStr);
                boolQuery.must(QueryBuilders.wrapperQuery(gson.toJson(filterMap)));
            }
        }

        return boolQuery;
    }

    /**
     * Fetches data using DSL query
     */
    private void fetchDataWithDSL(
        String timeRangeStart,
        String timeRangeEnd,
        AnalysisParameters params,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        try {
            BoolQueryBuilder boolQuery = buildFilterQuery(timeRangeStart, timeRangeEnd, params);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQuery).size(params.size).fetchSource(true);

            SearchRequest searchRequest = new SearchRequest(params.index).source(searchSourceBuilder);

            client.search(searchRequest, ActionListener.wrap(response -> {
                List<Map<String, Object>> data = new ArrayList<>();
                for (SearchHit hit : response.getHits().getHits()) {
                    data.add(hit.getSourceAsMap());
                }
                listener.onResponse(data);
            }, listener::onFailure));

        } catch (Exception e) {
            log.error("Failed to fetch data with DSL", e);
            listener.onFailure(e);
        }
    }

    /**
     * Fetches data using PPL query
     */
    private void fetchDataWithPPL(
        String timeRangeStart,
        String timeRangeEnd,
        AnalysisParameters params,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        try {
            String pplQuery = buildPPLQuery(timeRangeStart, timeRangeEnd, params);

            // Use PPLExecuteHelper with parser function
            Function<Map<String, Object>, List<Map<String, Object>>> pplResultParser = this::parsePPLResult;

            PPLExecuteHelper.executePPLAndParseResult(client, pplQuery, pplResultParser, listener);

        } catch (Exception e) {
            log.error("Failed to fetch data with PPL", e);
            listener.onFailure(e);
        }
    }

    /**
     * Parses PPL query result into list of documents
     */
    private List<Map<String, Object>> parsePPLResult(Map<String, Object> pplResult) {
        Object datarowsObj = pplResult.get("datarows");
        Object schemaObj = pplResult.get("schema");

        if (!(datarowsObj instanceof List) || !(schemaObj instanceof List)) {
            log.warn("Invalid PPL result format");
            return new ArrayList<>();
        }

        @SuppressWarnings("unchecked")
        List<List<Object>> datarows = (List<List<Object>>) datarowsObj;
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> schema = (List<Map<String, Object>>) schemaObj;

        List<String> fieldNames = new ArrayList<>();
        for (Map<String, Object> field : schema) {
            fieldNames.add((String) field.get("name"));
        }

        List<Map<String, Object>> documents = new ArrayList<>();
        for (List<Object> row : datarows) {
            Map<String, Object> doc = new HashMap<>();
            for (int i = 0; i < Math.min(row.size(), fieldNames.size()); i++) {
                doc.put(fieldNames.get(i), row.get(i));
            }
            documents.add(doc);
        }

        return documents;
    }

    /**
     * Builds PPL query with time range filter
     */
    private String buildPPLQuery(String timeRangeStart, String timeRangeEnd, AnalysisParameters params) {
        String pplBase = !Strings.isNullOrEmpty(params.ppl) ? params.ppl : "source=" + params.index;

        String timeFilter = String
            .format(
                Locale.ROOT,
                "%s >= '%s' AND %s <= '%s'",
                params.timeField,
                formatTimeString(timeRangeStart),
                params.timeField,
                formatTimeString(timeRangeEnd)
            );

        return String.format(Locale.ROOT, "%s | where %s | head %d", pplBase, timeFilter, params.size);
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

        throw new RuntimeException("Invalid time format: " + timeString);
    }

    /**
     * Builds query map from JSON string
     */
    public Map<String, Object> buildQueryFromMap(String queryStr) {
        if (Strings.isNullOrEmpty(queryStr)) {
            return new HashMap<>();
        }

        try {
            String normalizedQuery = queryStr.trim().replace("'", "\"");
            return gson.fromJson(normalizedQuery, new TypeToken<Map<String, Object>>() {
            }.getType());
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid query format: %s. Error: %s", queryStr, e.getMessage()));
        }
    }

    /**
     * Filters numeric fields from field type mappings
     *
     * @param fieldTypes Map of field names to their types
     * @return Set of numeric field names
     */
    public Set<String> getNumberFields(Map<String, String> fieldTypes) {
        Set<String> numberFields = new HashSet<>();
        for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
            if (NUMBER_FIELD_TYPES.contains(entry.getValue())) {
                numberFields.add(entry.getKey());
            }
        }
        return numberFields;
    }

    /**
     * Extracts nested field value from document using dot notation
     *
     * @param doc The document map
     * @param field The field path (e.g., "metrics.response_time")
     * @return The field value or null if not found
     */
    public Object getFlattenedValue(Map<String, Object> doc, String field) {
        if (doc == null || field == null) {
            return null;
        }

        String[] parts = field.split("\\.");
        Object current = doc;

        for (String part : parts) {
            if (!(current instanceof Map)) {
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> currentMap = (Map<String, Object>) current;
            current = currentMap.get(part);
            if (current == null) {
                return null;
            }
        }

        return current;
    }
}
