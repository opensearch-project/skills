/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.CommonValue.TOOL_INPUT_SCHEMA_FIELD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.utils.ToolUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Tool to search N documents before and N documents after a specific document ID,
 * ordered by a timestamp field using search_after pagination.
 */
@Getter
@Setter
@Log4j2
@ToolAnnotation(SearchAroundDocumentTool.TYPE)
public class SearchAroundDocumentTool implements Tool {

    public static final String INPUT_FIELD = "input";
    public static final String INDEX_FIELD = "index";
    public static final String DOC_ID_FIELD = "doc_id";
    public static final String TIMESTAMP_FIELD = "timestamp_field";
    public static final String COUNT_FIELD = "count";
    public static final String INPUT_SCHEMA_FIELD = "input_schema";

    public static final String TYPE = "SearchAroundDocumentTool";
    private static final String DEFAULT_DESCRIPTION = """
        Use this tool to search documents around a specific document by providing: \
        'index' for the index name, 'doc_id' for the target document ID, \
        'timestamp_field' for the field to order by, and 'count' for the number of documents before and after. \
        Returns N documents before, the target document, and N documents after.""";

    public static final String DEFAULT_INPUT_SCHEMA = """
        {
          "type": "object",
          "properties": {
            "index": {
              "type": "string",
              "description": "OpenSearch index name"
            },
            "doc_id": {
              "type": "string",
              "description": "Target document ID"
            },
            "timestamp_field": {
              "type": "string",
              "description": "Timestamp field for ordering (e.g., @timestamp)"
            },
            "count": {
              "type": "integer",
              "description": "Number of documents before and after the target"
            }
          },
          "required": ["index", "doc_id", "timestamp_field", "count"],
          "additionalProperties": false
        }
        """;

    private static final Gson GSON = new GsonBuilder().serializeSpecialFloatingPointValues().create();

    public static final Map<String, Object> DEFAULT_ATTRIBUTES = Map.of(TOOL_INPUT_SCHEMA_FIELD, DEFAULT_INPUT_SCHEMA);

    private String name = TYPE;
    private Map<String, Object> attributes;
    private String description = DEFAULT_DESCRIPTION;

    private Client client;

    private NamedXContentRegistry xContentRegistry;

    public SearchAroundDocumentTool(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;

        this.attributes = new HashMap<>();
        attributes.put(INPUT_SCHEMA_FIELD, DEFAULT_INPUT_SCHEMA);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return false;
        }
        boolean argumentsFromInput = parameters.containsKey(INPUT_FIELD) && !StringUtils.isEmpty(parameters.get(INPUT_FIELD));
        boolean argumentsFromParameters = parameters.containsKey(INDEX_FIELD)
            && parameters.containsKey(DOC_ID_FIELD)
            && parameters.containsKey(TIMESTAMP_FIELD)
            && parameters.containsKey(COUNT_FIELD)
            && !StringUtils.isEmpty(parameters.get(INDEX_FIELD))
            && !StringUtils.isEmpty(parameters.get(DOC_ID_FIELD))
            && !StringUtils.isEmpty(parameters.get(TIMESTAMP_FIELD))
            && !StringUtils.isEmpty(parameters.get(COUNT_FIELD));
        boolean validRequest = argumentsFromInput || argumentsFromParameters;
        if (!validRequest) {
            log.error("SearchAroundDocumentTool requires: index, doc_id, timestamp_field, and count parameters!");
            return false;
        }
        return true;
    }

    private static Map<String, Object> processResponse(SearchHit hit) {
        Map<String, Object> docContent = new HashMap<>();
        docContent.put("_index", hit.getIndex());
        docContent.put("_id", hit.getId());
        docContent.put("_score", hit.getScore());
        docContent.put("_source", hit.getSourceAsMap());
        if (hit.getSortValues() != null && hit.getSortValues().length > 0) {
            docContent.put("sort", hit.getSortValues());
        }
        return docContent;
    }

    @Override
    public <T> void run(Map<String, String> originalParameters, ActionListener<T> listener) {
        try {
            Map<String, String> parameters = ToolUtils.extractInputParameters(originalParameters, attributes);
            String input = parameters.get(INPUT_FIELD);
            String index = null;
            String docId = null;
            String timestampField = null;
            Integer count = null;

            if (!StringUtils.isEmpty(input)) {
                try {
                    JsonObject jsonObject = GSON.fromJson(input, JsonObject.class);
                    if (jsonObject != null) {
                        if (jsonObject.has(INDEX_FIELD)) {
                            index = jsonObject.get(INDEX_FIELD).getAsString();
                        }
                        if (jsonObject.has(DOC_ID_FIELD)) {
                            docId = jsonObject.get(DOC_ID_FIELD).getAsString();
                        }
                        if (jsonObject.has(TIMESTAMP_FIELD)) {
                            timestampField = jsonObject.get(TIMESTAMP_FIELD).getAsString();
                        }
                        if (jsonObject.has(COUNT_FIELD)) {
                            count = jsonObject.get(COUNT_FIELD).getAsInt();
                        }
                    }
                } catch (JsonSyntaxException e) {
                    log.error("Invalid JSON input: {}", input, e);
                }
            }

            // Fall back to direct parameters
            if (StringUtils.isEmpty(index)) {
                index = parameters.get(INDEX_FIELD);
            }
            if (StringUtils.isEmpty(docId)) {
                docId = parameters.get(DOC_ID_FIELD);
            }
            if (StringUtils.isEmpty(timestampField)) {
                timestampField = parameters.get(TIMESTAMP_FIELD);
            }
            if (count == null && parameters.containsKey(COUNT_FIELD)) {
                try {
                    count = Integer.parseInt(parameters.get(COUNT_FIELD));
                } catch (NumberFormatException e) {
                    log.error("Invalid count parameter: {}", parameters.get(COUNT_FIELD), e);
                }
            }

            // Validate all required parameters
            if (StringUtils.isEmpty(index) || StringUtils.isEmpty(docId) || StringUtils.isEmpty(timestampField) || count == null) {
                listener
                    .onFailure(
                        new IllegalArgumentException(
                            "SearchAroundDocumentTool requires: index, doc_id, timestamp_field, and count parameters"
                        )
                    );
                return;
            }

            final String finalIndex = index;
            final String finalDocId = docId;
            final String finalTimestampField = timestampField;
            final int finalCount = count;

            // Step 1: Fetch the target document by ID with sort values
            log
                .info(
                    "Calling SearchAroundDocumentTool: index={}, doc_id={}, timestamp_field={}, count={}",
                    index,
                    docId,
                    timestampField,
                    count
                );

            SearchSourceBuilder targetSource = new SearchSourceBuilder()
                .query(QueryBuilders.idsQuery().addIds(docId))
                .sort(new FieldSortBuilder(timestampField).order(SortOrder.DESC).unmappedType("boolean"))
                .sort(new FieldSortBuilder("_doc").order(SortOrder.DESC).unmappedType("boolean"))
                .size(1);
            SearchRequest targetRequest = new SearchRequest(index).source(targetSource);

            client.search(targetRequest, ActionListener.wrap(targetResponse -> {
                SearchHit[] targetHits = targetResponse.getHits().getHits();
                if (targetHits == null || targetHits.length == 0) {
                    listener.onFailure(new IllegalArgumentException("Document not found: " + finalDocId));
                    return;
                }

                SearchHit targetHit = targetHits[0];
                Object[] sortValues = targetHit.getSortValues();
                if (sortValues == null || sortValues.length < 2) {
                    listener.onFailure(new IllegalArgumentException("Could not get sort values from target document"));
                    return;
                }

                // Build target document response
                Map<String, Object> targetDoc = processResponse(targetHit);

                // Step 2 & 3: Execute multi-search for documents before and after using search_after
                MultiSearchRequest multiSearchRequest = new MultiSearchRequest();

                // Query for documents AFTER: search_after with ASC sort, exclude target doc
                BoolQueryBuilder afterQuery = QueryBuilders.boolQuery().mustNot(QueryBuilders.idsQuery().addIds(finalDocId));

                SearchSourceBuilder afterSource = new SearchSourceBuilder()
                    .query(afterQuery)
                    .sort(new FieldSortBuilder(finalTimestampField).order(SortOrder.ASC).unmappedType("boolean"))
                    .sort(new FieldSortBuilder("_doc").order(SortOrder.ASC).unmappedType("boolean"))
                    .searchAfter(sortValues)
                    .size(finalCount);
                SearchRequest afterRequest = new SearchRequest(finalIndex).source(afterSource);
                multiSearchRequest.add(afterRequest);

                // Query for documents BEFORE: search_after with DESC sort, exclude target doc
                BoolQueryBuilder beforeQuery = QueryBuilders.boolQuery().mustNot(QueryBuilders.idsQuery().addIds(finalDocId));

                SearchSourceBuilder beforeSource = new SearchSourceBuilder()
                    .query(beforeQuery)
                    .sort(new FieldSortBuilder(finalTimestampField).order(SortOrder.DESC).unmappedType("boolean"))
                    .sort(new FieldSortBuilder("_doc").order(SortOrder.DESC).unmappedType("boolean"))
                    .searchAfter(sortValues)
                    .size(finalCount);
                SearchRequest beforeRequest = new SearchRequest(finalIndex).source(beforeSource);
                multiSearchRequest.add(beforeRequest);

                client.multiSearch(multiSearchRequest, ActionListener.wrap(multiSearchResponse -> {
                    List<Map<String, Object>> result = new ArrayList<>();
                    MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();

                    // Process "before" results (need to reverse to get chronological order)
                    if (responses.length > 1 && responses[1].getResponse() != null) {
                        SearchHit[] beforeHits = responses[1].getResponse().getHits().getHits();
                        List<Map<String, Object>> beforeDocs = new ArrayList<>();
                        for (SearchHit hit : beforeHits) {
                            beforeDocs.add(processResponse(hit));
                        }
                        Collections.reverse(beforeDocs);
                        result.addAll(beforeDocs);
                    }

                    // Add target document
                    result.add(targetDoc);

                    // Process "after" results
                    if (responses.length > 0 && responses[0].getResponse() != null) {
                        SearchHit[] afterHits = responses[0].getResponse().getHits().getHits();
                        for (SearchHit hit : afterHits) {
                            result.add(processResponse(hit));
                        }
                    }

                    String resultJson = GSON.toJson(result);
                    listener.onResponse((T) resultJson);
                }, e -> {
                    log.error("Failed to execute multi-search", e);
                    listener.onFailure(e);
                }));
            }, e -> {
                log.error("Failed to fetch target document", e);
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            log.error("Failed to run SearchAroundDocumentTool", e);
            listener.onFailure(e);
        }
    }

    public static class Factory implements Tool.Factory<SearchAroundDocumentTool> {

        private Client client;
        private static Factory INSTANCE;

        private NamedXContentRegistry xContentRegistry;

        /**
         * Create or return the singleton factory instance
         */
        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (SearchAroundDocumentTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client, NamedXContentRegistry xContentRegistry) {
            this.client = client;
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public SearchAroundDocumentTool create(Map<String, Object> params) {
            SearchAroundDocumentTool tool = new SearchAroundDocumentTool(client, xContentRegistry);
            return tool;
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

        @Override
        public Map<String, Object> getDefaultAttributes() {
            return DEFAULT_ATTRIBUTES;
        }
    }
}
