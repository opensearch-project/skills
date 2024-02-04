/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.commons.alerting.AlertingPluginInterface;
import org.opensearch.commons.alerting.action.SearchMonitorRequest;
import org.opensearch.commons.alerting.model.ScheduledJob;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
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
@ToolAnnotation(SearchMonitorsTool.TYPE)
public class SearchMonitorsTool implements Tool {
    public static final String TYPE = "SearchMonitorsTool";
    private static final String DEFAULT_DESCRIPTION =
        "This is a tool that searches alerting monitors. It takes 10 optional arguments named monitorId which defines the monitor ID to filter for (default is null), and monitorName which defines explicit name of the monitor (default is null), and monitorNamePattern which is a wildcard query to match monitor name (default is null), and enabled which defines whether the monitor is enabled (default is null, indicating both enabled and disabled), and hasTriggers which defines whether the monitor has triggers enabled (default is null, indicating both), and indices which defines the index being monitored (default is null), and sortOrder which defines the order of the results (options are asc or desc, and default is asc), and sortString which defines how to sort the results (default is name.keyword), and size which defines the size of the request to be returned (default is 20), and startIndex which defines the paginated index to start from (default is 0).  The tool returns 2 values: a list of alerting monitors (each containining monitor ID, monitor name, monitor type (indicating query-level, document-level, or bucket-level monitor types), enabled, enabled time in epoch milliseconds, last update time in epoch milliseconds), and the total number of alerting monitors.";
    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String type;
    @Getter
    private String version;

    private Client client;
    @Setter
    private Parser<?, ?> inputParser;
    @Setter
    private Parser<?, ?> outputParser;

    public SearchMonitorsTool(Client client) {
        this.client = client;

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

    // Response is currently in a simple string format including the list of monitors (only name and ID attached), and
    // number of total monitors. The output will likely need to be updated, standardized, and include more fields in the
    // future to cover a sufficient amount of potential questions the agent will need to handle.
    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        final String monitorId = parameters.getOrDefault("monitorId", null);
        final String monitorName = parameters.getOrDefault("monitorName", null);
        final String monitorNamePattern = parameters.getOrDefault("monitorNamePattern", null);
        final Boolean enabled = parameters.containsKey("enabled") ? Boolean.parseBoolean(parameters.get("enabled")) : null;
        final Boolean hasTriggers = parameters.containsKey("hasTriggers") ? Boolean.parseBoolean(parameters.get("hasTriggers")) : null;
        final String indices = parameters.getOrDefault("indices", null);
        final String sortOrderStr = parameters.getOrDefault("sortOrder", "asc");
        final SortOrder sortOrder = "asc".equalsIgnoreCase(sortOrderStr) ? SortOrder.ASC : SortOrder.DESC;
        final String sortString = parameters.getOrDefault("sortString", "monitor.name.keyword");
        final int size = parameters.containsKey("size") && StringUtils.isNumeric(parameters.get("size"))
            ? Integer.parseInt(parameters.get("size"))
            : 20;
        final int startIndex = parameters.containsKey("startIndex") && StringUtils.isNumeric(parameters.get("startIndex"))
            ? Integer.parseInt(parameters.get("startIndex"))
            : 0;

        List<QueryBuilder> mustList = new ArrayList<QueryBuilder>();
        if (monitorId != null) {
            mustList.add(new TermQueryBuilder("_id", monitorId));
        }
        if (monitorName != null) {
            mustList.add(new TermQueryBuilder("monitor.name.keyword", monitorName));
        }
        if (monitorNamePattern != null) {
            mustList.add(new WildcardQueryBuilder("monitor.name.keyword", monitorNamePattern));
        }
        if (enabled != null) {
            mustList.add(new TermQueryBuilder("monitor.enabled", enabled));
        }
        if (hasTriggers != null) {
            NestedQueryBuilder nestedTriggerQuery = new NestedQueryBuilder(
                "monitor.triggers",
                new ExistsQueryBuilder("monitor.triggers"),
                ScoreMode.None
            );

            BoolQueryBuilder triggerQuery = new BoolQueryBuilder();
            if (hasTriggers) {
                triggerQuery.must(nestedTriggerQuery);
            } else {
                triggerQuery.mustNot(nestedTriggerQuery);
            }
            mustList.add(triggerQuery);
        }
        if (indices != null) {
            mustList
                .add(
                    new NestedQueryBuilder(
                        "monitor.inputs",
                        new WildcardQueryBuilder("monitor.inputs.search.indices", indices),
                        ScoreMode.None
                    )
                );
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must().addAll(mustList);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(boolQueryBuilder)
            .size(size)
            .from(startIndex)
            .sort(sortString, sortOrder);

        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(ScheduledJob.SCHEDULED_JOBS_INDEX);
        SearchMonitorRequest searchMonitorRequest = new SearchMonitorRequest(searchRequest);

        ActionListener<SearchResponse> searchMonitorListener = ActionListener.<SearchResponse>wrap(response -> {
            List<SearchHit> hits = Arrays.asList(response.getHits().getHits());
            Map<String, SearchHit> hitsAsMap = hits.stream().collect(Collectors.toMap(SearchHit::getId, hit -> hit));
            processHits(hitsAsMap, listener);

        }, e -> {
            // System index isn't initialized by default, so ignore such errors. Alerting plugin does not return the
            // standard IndexNotFoundException so we parse the message instead
            if (e.getMessage().contains("Configured indices are not found")) {
                processHits(Collections.emptyMap(), listener);
            } else {
                log.error("Failed to search monitors.", e);
                listener.onFailure(e);
            }
        });
        AlertingPluginInterface.INSTANCE.searchMonitors((NodeClient) client, searchMonitorRequest, searchMonitorListener);

    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return true;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private <T> void processHits(Map<String, SearchHit> hitsAsMap, ActionListener<T> listener) {
        StringBuilder sb = new StringBuilder();
        sb.append("Monitors=[");
        for (SearchHit hit : hitsAsMap.values()) {
            Map<String, Object> monitorAsMap = (Map<String, Object>) hit.getSourceAsMap().get("monitor");
            sb.append("{");
            sb.append("id=").append(hit.getId()).append(",");
            sb.append("name=").append(monitorAsMap.get("name")).append(",");
            sb.append("type=").append(monitorAsMap.get("monitor_type")).append(",");
            sb.append("enabled=").append(monitorAsMap.get("enabled")).append(",");
            sb.append("enabledTime=").append(monitorAsMap.get("enabled_time")).append(",");
            sb.append("lastUpdateTime=").append(monitorAsMap.get("last_update_time"));
            sb.append("}");
        }
        sb.append("]");
        sb.append("TotalMonitors=").append(hitsAsMap.size());
        listener.onResponse((T) sb.toString());
    }

    /**
     * Factory for the {@link SearchMonitorsTool}
     */
    public static class Factory implements Tool.Factory<SearchMonitorsTool> {
        private Client client;

        private static Factory INSTANCE;

        /** 
         * Create or return the singleton factory instance
         */
        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (SearchMonitorsTool.class) {
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
        }

        @Override
        public SearchMonitorsTool create(Map<String, Object> map) {
            return new SearchMonitorsTool(client);
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
