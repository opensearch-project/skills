/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.commons.alerting.AlertingPluginInterface;
import org.opensearch.commons.alerting.action.GetAlertsRequest;
import org.opensearch.commons.alerting.action.GetAlertsResponse;
import org.opensearch.commons.alerting.model.Alert;
import org.opensearch.commons.alerting.model.Table;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@ToolAnnotation(SearchAlertsTool.TYPE)
public class SearchAlertsTool implements Tool {
    public static final String TYPE = "SearchAlertsTool";
    private static final String DEFAULT_DESCRIPTION =
        "This is a tool that finds alert trigger information. It takes 12 optional argument named sortOrder which defines the order of the results (options are asc or desc, and default is asc), and sortString which defines how to sort the results (default is monitor_name.keyword), and size which defines the size of the request to be returned (default is 20), and startIndex which defines the index to start from (default is 0), and searchString which defines the search string to use for searching a specific alert (default is an empty String), and severityLevel which defines the severity level to filter for (default is ALL), and alertState which defines the alert state to filter for (default is ALL), and monitorId which defines the monitor ID to filter for, and alertIndex which defines the alert index to search from (default is null), and monitorIds which defines the list of monitor IDs to filter for, and workflowIds which defines the list of workflow IDs to filter for（default is null), and alertIds which defines the list of alert IDs to filter for (default is null). The tool returns 2 values: a list of alerts (each containining id, version, schema version, monitor ID, workflow ID, workflow name, monitor name, monitor version, monitor user, trigger ID, trigger name, finding IDs, related doc IDs, state, start time, end time, last notifcation time, acknowledged time, error message, error history, severity, action execution results, aggregation result bucket, execution ID, associated alert IDs), and the total number of alerts.";

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

    public SearchAlertsTool(Client client) {
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

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        final String tableSortOrder = parameters.getOrDefault("sortOrder", "asc");
        final String tableSortString = parameters.getOrDefault("sortString", "monitor_name.keyword");
        final int tableSize = parameters.containsKey("size") && StringUtils.isNumeric(parameters.get("size"))
            ? Integer.parseInt(parameters.get("size"))
            : 20;
        final int startIndex = parameters.containsKey("startIndex") && StringUtils.isNumeric(parameters.get("startIndex"))
            ? Integer.parseInt(parameters.get("startIndex"))
            : 0;
        final String searchString = parameters.getOrDefault("searchString", null);

        // not exposing "missing" from the table, using default of null
        final Table table = new Table(tableSortOrder, tableSortString, null, tableSize, startIndex, searchString);

        final String severityLevel = parameters.getOrDefault("severityLevel", "ALL");
        final String alertState = parameters.getOrDefault("alertState", "ALL");
        final String monitorId = parameters.getOrDefault("monitorId", null);
        final String alertIndex = parameters.getOrDefault("alertIndex", null);
        @SuppressWarnings("unchecked")
        final List<String> monitorIds = parameters.containsKey("monitorIds")
            ? gson.fromJson(parameters.get("monitorIds"), List.class)
            : null;
        @SuppressWarnings("unchecked")
        final List<String> workflowIds = parameters.containsKey("workflowIds")
            ? gson.fromJson(parameters.get("workflowIds"), List.class)
            : null;
        @SuppressWarnings("unchecked")
        final List<String> alertIds = parameters.containsKey("alertIds") ? gson.fromJson(parameters.get("alertIds"), List.class) : null;

        GetAlertsRequest getAlertsRequest = new GetAlertsRequest(
            table,
            severityLevel,
            alertState,
            monitorId,
            alertIndex,
            monitorIds,
            workflowIds,
            alertIds
        );

        // create response listener
        // stringify the response, may change to a standard format in the future
        ActionListener<GetAlertsResponse> getAlertsListener = ActionListener.<GetAlertsResponse>wrap(response -> {
            StringBuilder sb = new StringBuilder();
            sb.append("Alerts=[");
            for (Alert alert : response.getAlerts()) {
                sb.append(alert.toString());
            }
            sb.append("]");
            sb.append("TotalAlerts=").append(response.getTotalAlerts());
            listener.onResponse((T) sb.toString());
        }, e -> {
            log.error("Failed to search alerts.", e);
            listener.onFailure(e);
        });

        // execute the search
        AlertingPluginInterface.INSTANCE.getAlerts((NodeClient) client, getAlertsRequest, getAlertsListener);
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
     * Factory for the {@link SearchAlertsTool}
     */
    public static class Factory implements Tool.Factory<SearchAlertsTool> {
        private Client client;

        private static Factory INSTANCE;

        /** 
         * Create or return the singleton factory instance
         */
        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (SearchAlertsTool.class) {
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
        public SearchAlertsTool create(Map<String, Object> map) {
            return new SearchAlertsTool(client);
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
