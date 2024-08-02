/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.Map;

import org.apache.commons.lang3.math.NumberUtils;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.agent.tools.utils.ClusterStatsUtil;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
@ToolAnnotation(GetClusterStatsTool.TYPE)
public class GetClusterStatsTool implements Tool {

    public static final String TYPE = "GetClusterStatsTool";

    private String name = TYPE;

    private String description = DEFAULT_DESCRIPTION;

    public static final String INPUT_FIELD = "text";

    private Client client;

    private static final String DEFAULT_DESCRIPTION = "Use this tool to retrieve cluster stats on OpenSearch.";

    public GetClusterStatsTool(Client client) {
        this.client = client;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest();
        if (parameters.containsKey("current_node")) {
            String current_node = parameters.get("current_node");
            request.setCurrentNode(current_node);
        }
        if (parameters.containsKey("index")) {
            String index = parameters.get("index");
            request.setIndex(index);
        }
        if (parameters.containsKey("primary")) {
            Boolean primary = Boolean.parseBoolean(parameters.get("primary"));
            request.setPrimary(primary);
        }
        if (parameters.containsKey("shard")) {
            Integer shard = NumberUtils.createInteger(parameters.get("shard"));
            request.setShard(shard);
        }

        ActionListener<ClusterAllocationExplainResponse> internalListener = new ActionListener<ClusterAllocationExplainResponse>() {

            @Override
            public void onResponse(ClusterAllocationExplainResponse allocationExplainResponse) {
                try {
                    ClusterAllocationExplanation clusterAllocationExplanation = allocationExplainResponse.getExplanation();
                    XContentBuilder xContentBuilder = clusterAllocationExplanation
                        .toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
                    @SuppressWarnings("unchecked")
                    T response = (T) xContentBuilder.toString();
                    listener.onResponse(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }

        };

        ClusterStatsUtil.getClusterAllocationExplain(client, request, internalListener);
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
        return parameters != null;
    }

    public static class Factory implements Tool.Factory<GetClusterStatsTool> {
        private Client client;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (GetClusterStatsTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public GetClusterStatsTool create(Map<String, Object> parameters) {
            return new GetClusterStatsTool(client);
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
