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
import org.opensearch.agent.tools.utils.ToolConstants;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@ToolAnnotation(SearchAnomalyResultsTool.TYPE)
public class SearchAnomalyResultsTool implements Tool {
    public static final String TYPE = "SearchAnomalyResultsTool";
    private static final String DEFAULT_DESCRIPTION =
        "This is a tool that searches anomaly results. It takes 9 arguments named detectorId which defines the detector ID to filter for (default is null), and realtime which defines whether the anomaly results are from a realtime detector (set to false to only get results from historical analyses) (default is null), and anomalyGradeThreshold which defines the threshold for anomaly grade (a number between 0 and 1 that indicates how anomalous a data point is) (default is greater than 0), and dataStartTime which defines the start time of the anomaly data in epoch milliseconds (default is null), and dataEndTime which defines the end time of the anomaly data in epoch milliseconds (default is null), and sortOrder which defines the order of the results (options are asc or desc, and default is desc), and sortString which defines how to sort the results (default is data_start_time), and size which defines the number of anomalies to be returned (default is 20), and startIndex which defines the paginated index to start from (default is 0). The tool returns 2 values: a list of anomaly results (where each result contains the detector ID, the anomaly grade, and the confidence), and the total number of anomaly results.";

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

    public SearchAnomalyResultsTool(Client client, NamedWriteableRegistry namedWriteableRegistry) {
        this.client = client;
        this.adClient = new AnomalyDetectionNodeClient(client, namedWriteableRegistry);

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

    // Response is currently in a simple string format including the list of anomaly results (only detector ID, grade, confidence),
    // and total # of results. The output will likely need to be updated, standardized, and include more fields in the
    // future to cover a sufficient amount of potential questions the agent will need to handle.
    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        final String detectorId = parameters.getOrDefault("detectorId", null);
        final Boolean realTime = parameters.containsKey("realTime") ? Boolean.parseBoolean(parameters.get("realTime")) : null;
        final Double anomalyGradeThreshold = parameters.containsKey("anomalyGradeThreshold")
            ? Double.parseDouble(parameters.get("anomalyGradeThreshold"))
            : 0;
        final Long dataStartTime = parameters.containsKey("dataStartTime") && StringUtils.isNumeric(parameters.get("dataStartTime"))
            ? Long.parseLong(parameters.get("dataStartTime"))
            : null;
        final Long dataEndTime = parameters.containsKey("dataEndTime") && StringUtils.isNumeric(parameters.get("dataEndTime"))
            ? Long.parseLong(parameters.get("dataEndTime"))
            : null;
        final String sortOrderStr = parameters.getOrDefault("sortOrder", "asc");
        final SortOrder sortOrder = sortOrderStr.equalsIgnoreCase("asc") ? SortOrder.ASC : SortOrder.DESC;
        final String sortString = parameters.getOrDefault("sortString", "data_start_time");
        final int size = parameters.containsKey("size") ? Integer.parseInt(parameters.get("size")) : 20;
        final int startIndex = parameters.containsKey("startIndex") ? Integer.parseInt(parameters.get("startIndex")) : 0;

        List<QueryBuilder> mustList = new ArrayList<QueryBuilder>();
        if (detectorId != null) {
            mustList.add(new TermQueryBuilder("detector_id", detectorId));
        }
        // We include or exclude the task ID if fetching historical or real-time results, respectively.
        // For more details, see https://opensearch.org/docs/latest/observing-your-data/ad/api/#search-detector-result
        if (realTime != null) {
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            ExistsQueryBuilder existsQuery = new ExistsQueryBuilder("task_id");
            if (realTime) {
                boolQuery.mustNot(existsQuery);
            } else {
                boolQuery.must(existsQuery);
            }
            mustList.add(boolQuery);
        }
        if (anomalyGradeThreshold != null) {
            mustList.add(new RangeQueryBuilder("anomaly_grade").gt(anomalyGradeThreshold));
        }
        if (dataStartTime != null || dataEndTime != null) {
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder("anomaly_grade");
            if (dataStartTime != null) {
                rangeQuery.gte(dataStartTime);
            }
            if (dataEndTime != null) {
                rangeQuery.lte(dataEndTime);
            }
            mustList.add(rangeQuery);
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must().addAll(mustList);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(boolQueryBuilder)
            .size(size)
            .from(startIndex)
            .sort(sortString, sortOrder);

        // In the future we may support custom result indices. For now default to the default AD result system indices.
        SearchRequest searchAnomalyResultsRequest = new SearchRequest()
            .source(searchSourceBuilder)
            .indices(ToolConstants.AD_RESULTS_INDEX_PATTERN);

        ActionListener<SearchResponse> searchAnomalyResultsListener = ActionListener.<SearchResponse>wrap(response -> {
            processHits(response.getHits(), listener);
        }, e -> {
            // System index isn't initialized by default, so ignore such errors
            if (e instanceof IndexNotFoundException) {
                processHits(SearchHits.empty(), listener);
            } else {
                log.error("Failed to search anomaly results.", e);
                listener.onFailure(e);

            }
        });

        adClient.searchAnomalyResults(searchAnomalyResultsRequest, searchAnomalyResultsListener);
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return true;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private <T> void processHits(SearchHits searchHits, ActionListener<T> listener) {
        SearchHit[] hits = searchHits.getHits();

        StringBuilder sb = new StringBuilder();
        sb.append("AnomalyResults=[");
        for (SearchHit hit : hits) {
            sb.append("{");
            sb.append("detectorId=").append(hit.getSourceAsMap().get("detector_id")).append(",");
            sb.append("grade=").append(hit.getSourceAsMap().get("anomaly_grade")).append(",");
            sb.append("confidence=").append(hit.getSourceAsMap().get("confidence"));
            sb.append("}");
        }
        sb.append("]");
        sb.append("TotalAnomalyResults=").append(searchHits.getTotalHits().value);
        listener.onResponse((T) sb.toString());
    }

    /**
     * Factory for the {@link SearchAnomalyResultsTool}
     */
    public static class Factory implements Tool.Factory<SearchAnomalyResultsTool> {
        private Client client;

        private NamedWriteableRegistry namedWriteableRegistry;

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
        public void init(Client client, NamedWriteableRegistry namedWriteableRegistry) {
            this.client = client;
            this.namedWriteableRegistry = namedWriteableRegistry;
            this.adClient = new AnomalyDetectionNodeClient(client, namedWriteableRegistry);
        }

        @Override
        public SearchAnomalyResultsTool create(Map<String, Object> map) {
            return new SearchAnomalyResultsTool(client, namedWriteableRegistry);
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
