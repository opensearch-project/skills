/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.client.AnomalyDetectionNodeClient;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

import lombok.Getter;
import lombok.Setter;

@ToolAnnotation(SearchAnomalyResultsTool.TYPE)
public class SearchAnomalyResultsTool implements Tool {
    public static final String TYPE = "SearchAnomalyResultsTool";
    private static final String DEFAULT_DESCRIPTION = "Use this tool to search anomaly results.";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;

    @Getter
    private String version;

    private Client client;

    private AnomalyDetectionNodeClient adClient;

    @Setter
    private Parser<?, ?> inputParser;
    @Setter
    private Parser<?, ?> outputParser;

    public SearchAnomalyResultsTool(Client client) {
        this.client = client;
        this.adClient = new AnomalyDetectionNodeClient(client);

        // probably keep this overridden output parser. need to ensure the output matches what's expected
        outputParser = new Parser<>() {
            @Override
            public Object parse(Object o) {
                @SuppressWarnings("unchecked")
                List<ModelTensors> mlModelOutputs = (List<ModelTensors>) o;
                return mlModelOutputs.get(0).getMlModelTensors().get(0).getDataAsMap().get("response");
            }
        };
    }

    // TODO: update description
    // Response is currently in a simple string format including the list of anomaly results (only name and ID attached), and
    // number of total results. The output will likely need to be updated, standardized, and include more fields in the
    // future to cover a sufficient amount of potential questions the agent will need to handle.
    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        final String detectorId = parameters.getOrDefault("detectorId", null);
        final Boolean realTime = parameters.containsKey("realTime")
            ? Boolean.parseBoolean(parameters.get("realTime"))
            : null;
        final Boolean anomalyGradeThreshold = parameters.containsKey("anomalyGradeThreshold")
            ? Boolean.parseBoolean(parameters.get("anomalyGradeThreshold"))
            : null;
        final Long dataStartTime = parameters.containsKey("dataStartTime") && StringUtils.isNumeric(parameters.get("dataStartTime"))
            ? Long.parseLong(parameters.get("dataStartTime"))
            : null;
        final Long dataEndTime = parameters.containsKey("dataEndTime") && StringUtils.isNumeric(parameters.get("dataEndTime"))
            ? Long.parseLong(parameters.get("dataEndTime"))
            : null;
        final String sortOrderStr = parameters.getOrDefault("sortOrder", "asc");
        final SortOrder sortOrder = sortOrderStr.equalsIgnoreCase("asc") ? SortOrder.ASC : SortOrder.DESC;
        final String sortString = parameters.getOrDefault("sortString", "name.keyword");
        final int size = parameters.containsKey("size") ? Integer.parseInt(parameters.get("size")) : 20;
        final int startIndex = parameters.containsKey("startIndex") ? Integer.parseInt(parameters.get("startIndex")) : 0;

        // TODO: update list below
        List<QueryBuilder> mustList = new ArrayList<QueryBuilder>();
        if (detectorName != null) {
            mustList.add(new TermQueryBuilder("name.keyword", detectorName));
        }
        if (detectorNamePattern != null) {
            mustList.add(new WildcardQueryBuilder("name.keyword", detectorNamePattern));
        }
        if (indices != null) {
            mustList.add(new TermQueryBuilder("indices", indices));
        }
        if (highCardinality != null) {
            mustList.add(new TermQueryBuilder("detector_type", highCardinality ? "MULTI_ENTITY" : "SINGLE_ENTITY"));
        }
        if (lastUpdateTime != null) {
            mustList.add(new BoolQueryBuilder().filter(new RangeQueryBuilder("last_update_time").gte(lastUpdateTime)));

        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must().addAll(mustList);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(boolQueryBuilder)
            .size(size)
            .from(startIndex)
            .sort(sortString, sortOrder);

        SearchRequest searchDetectorRequest = new SearchRequest().source(searchSourceBuilder);

        ActionListener<SearchResponse> searchAnomalyResultListener = ActionListener.<SearchResponse>wrap(response -> {
            StringBuilder sb = new StringBuilder();
            SearchHit[] hits = response.getHits().getHits();
            sb.append("AnomalyResults=[");
            for (SearchHit hit : hits) {
                sb.append("{");
                sb.append("id=").append(hit.getId()).append(",");
                sb.append("name=").append(hit.getSourceAsMap().get("name"));
                sb.append("}");
            }
            sb.append("]");
            sb.append("TotalAnomalyResults=").append(response.getHits().getTotalHits().value);
            listener.onResponse((T) sb.toString());
        }, e -> { listener.onFailure(e); });

        adClient.searchAnomalyResults(searchDetectorRequest, searchDetectorListener);
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return true;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Factory for the {@link SearchAnomalyResultsTool}
     */
    public static class Factory implements Tool.Factory<SearchAnomalyResultsTool> {
        private Client client;

        private AnomalyDetectionNodeClient adClient;

        private static Factory INSTANCE;

        /** 
         * Create or return the singleton factory instance
         */
        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (SearchAnomalyResultsTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        /**
         * Initialize this factory
         * @param client The OpenSearch client
         */
        public void init(Client client) {
            this.client = client;
            this.adClient = new AnomalyDetectionNodeClient(client);
        }

        @Override
        public SearchAnomalyResultsTool create(Map<String, Object> map) {
            return new SearchAnomalyResultsTool(client);
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }

}
