/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.client.AnomalyDetectionNodeClient;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.transport.GetAnomalyDetectorRequest;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.agent.tools.utils.ToolConstants.DetectorStateString;
import org.opensearch.client.Client;
import org.opensearch.common.lucene.uid.Versions;
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
import lombok.extern.log4j.Log4j2;

@Log4j2
@ToolAnnotation(SearchAnomalyDetectorsTool.TYPE)
public class SearchAnomalyDetectorsTool implements Tool {
    public static final String TYPE = "SearchAnomalyDetectorsTool";
    private static final String DEFAULT_DESCRIPTION = "Use this tool to search anomaly detectors.";

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

    public SearchAnomalyDetectorsTool(Client client) {
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

    // Response is currently in a simple string format including the list of anomaly detectors (only name and ID attached), and
    // number of total detectors. The output will likely need to be updated, standardized, and include more fields in the
    // future to cover a sufficient amount of potential questions the agent will need to handle.
    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        final String detectorName = parameters.getOrDefault("detectorName", null);
        final String detectorNamePattern = parameters.getOrDefault("detectorNamePattern", null);
        final String indices = parameters.getOrDefault("indices", null);
        final Boolean highCardinality = parameters.containsKey("highCardinality")
            ? Boolean.parseBoolean(parameters.get("highCardinality"))
            : null;
        final Long lastUpdateTime = parameters.containsKey("lastUpdateTime") && StringUtils.isNumeric(parameters.get("lastUpdateTime"))
            ? Long.parseLong(parameters.get("lastUpdateTime"))
            : null;
        final String sortOrderStr = parameters.getOrDefault("sortOrder", "asc");
        final SortOrder sortOrder = sortOrderStr.equalsIgnoreCase("asc") ? SortOrder.ASC : SortOrder.DESC;
        final String sortString = parameters.getOrDefault("sortString", "name.keyword");
        final int size = parameters.containsKey("size") ? Integer.parseInt(parameters.get("size")) : 20;
        final int startIndex = parameters.containsKey("startIndex") ? Integer.parseInt(parameters.get("startIndex")) : 0;
        final Boolean running = parameters.containsKey("running") ? Boolean.parseBoolean(parameters.get("running")) : null;
        final Boolean disabled = parameters.containsKey("disabled") ? Boolean.parseBoolean(parameters.get("disabled")) : null;
        final Boolean failed = parameters.containsKey("failed") ? Boolean.parseBoolean(parameters.get("failed")) : null;

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

        ActionListener<SearchResponse> searchDetectorListener = ActionListener.<SearchResponse>wrap(response -> {
            StringBuilder sb = new StringBuilder();
            List<SearchHit> hits = Arrays.asList(response.getHits().getHits());
            Map<String, SearchHit> hitsAsMap = hits.stream().collect(Collectors.toMap(SearchHit::getId, hit -> hit));

            // If we need to filter by detector state, make subsequent profile API calls to each detector
            if (running != null || disabled != null || failed != null) {
                List<CompletableFuture<GetAnomalyDetectorResponse>> profileFutures = new ArrayList<>();
                for (SearchHit hit : hits) {
                    CompletableFuture<GetAnomalyDetectorResponse> profileFuture = new CompletableFuture<GetAnomalyDetectorResponse>()
                        .orTimeout(30000, TimeUnit.MILLISECONDS);
                    profileFutures.add(profileFuture);
                    ActionListener<GetAnomalyDetectorResponse> profileListener = ActionListener
                        .<GetAnomalyDetectorResponse>wrap(profileResponse -> {
                            profileFuture.complete(profileResponse);
                        }, e -> {
                            log.error("Failed to get anomaly detector profile.", e);
                            profileFuture.completeExceptionally(e);
                            listener.onFailure(e);
                        });

                    GetAnomalyDetectorRequest profileRequest = new GetAnomalyDetectorRequest(
                        hit.getId(),
                        Versions.MATCH_ANY,
                        true,
                        true,
                        "",
                        "",
                        false,
                        null
                    );
                    adClient.getDetectorProfile(profileRequest, profileListener);
                }

                List<GetAnomalyDetectorResponse> profileResponses = new ArrayList<>();
                try {
                    CompletableFuture<List<GetAnomalyDetectorResponse>> listFuture = CompletableFuture
                        .allOf(profileFutures.toArray(new CompletableFuture<?>[0]))
                        .thenApply(v -> profileFutures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
                    profileResponses = listFuture.join();
                } catch (Exception e) {
                    log.error("Failed to get all anomaly detector profiles.", e);
                    listener.onFailure(e);
                }

                for (GetAnomalyDetectorResponse profileResponse : profileResponses) {
                    if (profileResponse != null && profileResponse.getDetector() != null) {
                        String detectorId = profileResponse.getDetector().getId();

                        // We follow the existing logic as the frontend to determine overall detector state
                        // https://github.com/opensearch-project/anomaly-detection-dashboards-plugin/blob/main/server/routes/utils/adHelpers.ts#L437
                        String detectorState = DetectorStateString.Disabled.name();
                        ADTask realtimeTask = profileResponse.getRealtimeAdTask();

                        if (realtimeTask != null) {
                            String taskState = realtimeTask.getState();
                            if (taskState.equalsIgnoreCase("CREATED")) {
                                detectorState = DetectorStateString.Initializing.name();
                            } else if (taskState.equalsIgnoreCase("RUNNING")) {
                                detectorState = DetectorStateString.Running.name();
                            } else if (taskState.equalsIgnoreCase("INIT_FAILURE")
                                || taskState.equalsIgnoreCase("UNEXPECTED_FAILURE")
                                || taskState.equalsIgnoreCase("FAILED")) {
                                detectorState = DetectorStateString.Failed.name();
                            }
                        }

                        if ((Boolean.FALSE.equals(running) && detectorState.equals(DetectorStateString.Running.name()))
                            || (Boolean.FALSE.equals(disabled) && detectorState.equals(DetectorStateString.Disabled.name()))
                            || (Boolean.FALSE.equals(failed) && detectorState.equals(DetectorStateString.Failed.name()))) {
                            hitsAsMap.remove(detectorId);
                        }

                    }
                }
            }

            sb.append("AnomalyDetectors=[");
            for (SearchHit hit : hitsAsMap.values()) {
                sb.append("{");
                sb.append("id=").append(hit.getId()).append(",");
                sb.append("name=").append(hit.getSourceAsMap().get("name"));
                sb.append("}");
            }
            sb.append("]");
            sb.append("TotalAnomalyDetectors=").append(hitsAsMap.size());
            listener.onResponse((T) sb.toString());
        }, e -> {
            log.error("Failed to search anomaly detectors.", e);
            listener.onFailure(e);
        });

        adClient.searchAnomalyDetectors(searchDetectorRequest, searchDetectorListener);
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
     * Factory for the {@link SearchAnomalyDetectorsTool}
     */
    public static class Factory implements Tool.Factory<SearchAnomalyDetectorsTool> {
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
            synchronized (SearchAnomalyDetectorsTool.class) {
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
        public SearchAnomalyDetectorsTool create(Map<String, Object> map) {
            return new SearchAnomalyDetectorsTool(client);
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
