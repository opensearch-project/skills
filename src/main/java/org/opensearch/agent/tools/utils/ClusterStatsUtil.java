/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;

public class ClusterStatsUtil {
    public static void getClusterAllocationExplain(
        final Client client,
        final ClusterAllocationExplainRequest request,
        final ActionListener<ClusterAllocationExplainResponse> listener
    ) {
        client.admin().cluster().allocationExplain(request, listener);
    }

    public static void getCatIndex(
        final Client client,
        final GetSettingsRequest request,
        final ActionListener<GetSettingsResponse> listener
    ) {
        client.admin().indices().getSettings(request, listener);
    }

    public static void getClusterHealth(
        final Client client,
        final ClusterHealthRequest request,
        final ActionListener<ClusterHealthResponse> listener
    ) {
        client.admin().cluster().health(request, listener);
    }
}
