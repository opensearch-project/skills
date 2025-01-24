/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Abstract tool supports search paradigms in neural-search plugin.
 */
@Log4j2
@Getter
@Setter
public abstract class AbstractRetrieverTool implements Tool {
    public static final String DEFAULT_DESCRIPTION = "Use this tool to search data in OpenSearch index.";
    public static final String INPUT_FIELD = "input";
    public static final String INDEX_FIELD = "index";
    public static final String SOURCE_FIELD = "source_field";
    public static final String DOC_SIZE_FIELD = "doc_size";
    public static final int DEFAULT_DOC_SIZE = 2;

    protected String description = DEFAULT_DESCRIPTION;
    protected Client client;
    protected NamedXContentRegistry xContentRegistry;
    protected String index;
    protected String[] sourceFields;
    protected Integer docSize;
    protected String version;

    protected AbstractRetrieverTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        String index,
        String[] sourceFields,
        Integer docSize
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.index = index;
        this.sourceFields = sourceFields;
        this.docSize = docSize == null ? DEFAULT_DOC_SIZE : docSize;
    }

    protected abstract String getQueryBody(String queryText);

    private static Map<String, Object> processResponse(SearchHit hit) {
        Map<String, Object> docContent = new HashMap<>();
        docContent.put("_index", hit.getIndex());
        docContent.put("_id", hit.getId());
        docContent.put("_score", hit.getScore());
        docContent.put("_source", hit.getSourceAsMap());
        return docContent;
    }

    protected <T> SearchRequest buildSearchRequest(Map<String, String> parameters) throws IOException {
        String question = parameters.get(INPUT_FIELD);
        if (StringUtils.isBlank(question)) {
            throw new IllegalArgumentException("[" + INPUT_FIELD + "] is null or empty, can not process it.");
        }

        String query = getQueryBody(question);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        XContentParser queryParser = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, query);
        searchSourceBuilder.parseXContent(queryParser);
        searchSourceBuilder.fetchSource(sourceFields, null);
        searchSourceBuilder.size(docSize);
        return new SearchRequest().source(searchSourceBuilder).indices(parameters.getOrDefault(INDEX_FIELD, index));
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        SearchRequest searchRequest;
        try {
            searchRequest = buildSearchRequest(parameters);
        } catch (Exception e) {
            log.error("Failed to build search request.", e);
            listener.onFailure(e);
            return;
        }

        ActionListener actionListener = ActionListener.<SearchResponse>wrap(r -> {
            SearchHit[] hits = r.getHits().getHits();

            if (hits != null && hits.length > 0) {
                StringBuilder contextBuilder = new StringBuilder();
                for (SearchHit hit : hits) {
                    Map<String, Object> docContent = processResponse(hit);
                    String docContentInString = AccessController
                        .doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(docContent));
                    contextBuilder.append(docContentInString).append("\n");
                }
                listener.onResponse((T) contextBuilder.toString());
            } else {
                listener.onResponse((T) "Can not get any match from search result.");
            }
        }, e -> {
            log.error("Failed to search index.", e);
            listener.onFailure(e);
        });
        client.search(searchRequest, actionListener);
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return parameters != null && parameters.size() > 0 && !StringUtils.isBlank(parameters.get("input"));
    }

    protected static abstract class Factory<T extends Tool> implements Tool.Factory<T> {
        protected Client client;
        protected NamedXContentRegistry xContentRegistry;

        public void init(Client client, NamedXContentRegistry xContentRegistry) {
            this.client = client;
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
