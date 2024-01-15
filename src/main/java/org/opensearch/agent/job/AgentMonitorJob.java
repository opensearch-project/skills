/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.job;

import static org.opensearch.threadpool.ThreadPool.Names.GENERIC;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.opensearch.agent.indices.IndicesHelper;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import lombok.extern.log4j.Log4j2;

/**
 * monitor agent change and trigger index summary embedding job for new agent
 */
@Log4j2
public class AgentMonitorJob implements Runnable {

    private final ClusterService clusterService;
    private final Client client;
    private final IndicesHelper indicesHelper;
    private final MLClients mlClients;

    private final ThreadPool threadPool;

    public AgentMonitorJob(
        ClusterService clusterService,
        Client client,
        IndicesHelper indicesHelper,
        MLClients mlClients,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.indicesHelper = indicesHelper;
        this.mlClients = mlClients;
        this.threadPool = threadPool;
    }

    private static final CopyOnWriteArrayList<String> EMBEDDING_MODEL_IDS = new CopyOnWriteArrayList<>();

    @Override
    public void run() {
        mlClients.getModelIdsForIndexRoutingTool(ActionListener.wrap(modelIds -> {
            List<String> embeddingModelIds = new ArrayList<>(modelIds);
            embeddingModelIds.removeAll(EMBEDDING_MODEL_IDS);
            if (!embeddingModelIds.isEmpty()) {
                IndexSummaryEmbeddingJob job = new IndexSummaryEmbeddingJob(client, clusterService, indicesHelper, mlClients);
                job.setAdhocModelIds(embeddingModelIds);
                threadPool.schedule(job, TimeValue.timeValueSeconds(5), GENERIC);
            }
        }, exception -> log.info("Query agent for index routing tool failed.", exception)));
    }

    public static void setProcessedModelIds(List<String> modelIds, boolean override) {
        if (override) {
            EMBEDDING_MODEL_IDS.clear();
        }
        EMBEDDING_MODEL_IDS.addAllAbsent(modelIds);
    }
}
