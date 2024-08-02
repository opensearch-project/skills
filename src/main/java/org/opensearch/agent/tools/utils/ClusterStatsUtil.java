/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;

public class ClusterStatsUtil {
    public static void getClusterAllocationExplain(
        Client client,
        ClusterAllocationExplainRequest request,
        ActionListener<ClusterAllocationExplainResponse> listener
    ) {
        client.admin().cluster().allocationExplain(request, listener);
    }
}
