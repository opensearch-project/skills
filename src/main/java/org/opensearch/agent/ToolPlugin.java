/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent;

import static org.opensearch.agent.job.Constants.INDEX_SUMMARY_JOB_THREAD_POOL;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.opensearch.agent.indices.IndicesHelper;
import org.opensearch.agent.job.IndexSummaryEmbeddingJob;
import org.opensearch.agent.job.MLClients;
import org.opensearch.agent.job.SkillsClusterManagerEventListener;
import org.opensearch.agent.tools.IndexRoutingTool;
import org.opensearch.agent.tools.NeuralSparseSearchTool;
import org.opensearch.agent.tools.PPLTool;
import org.opensearch.agent.tools.RAGTool;
import org.opensearch.agent.tools.SearchAlertsTool;
import org.opensearch.agent.tools.SearchAnomalyDetectorsTool;
import org.opensearch.agent.tools.SearchAnomalyResultsTool;
import org.opensearch.agent.tools.SearchIndexTool;
import org.opensearch.agent.tools.SearchMonitorsTool;
import org.opensearch.agent.tools.VectorDBTool;
import org.opensearch.agent.tools.VisualizationsTool;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.ml.common.spi.MLCommonsExtension;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import lombok.SneakyThrows;

public class ToolPlugin extends Plugin implements MLCommonsExtension, SystemIndexPlugin {

    private Client client;
    private ClusterService clusterService;
    private NamedXContentRegistry xContentRegistry;
    private IndicesHelper indicesHelper;
    private MLClients mlClients;

    @SneakyThrows
    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;

        mlClients = new MLClients(client, xContentRegistry, clusterService);
        indicesHelper = new IndicesHelper(clusterService, client, mlClients);
        SkillsClusterManagerEventListener clusterManagerEventListener = new SkillsClusterManagerEventListener(
            clusterService,
            client,
            environment.settings(),
            threadPool,
            indicesHelper,
            mlClients
        );

        PPLTool.Factory.getInstance().init(client);
        VisualizationsTool.Factory.getInstance().init(client);
        NeuralSparseSearchTool.Factory.getInstance().init(client, xContentRegistry);
        VectorDBTool.Factory.getInstance().init(client, xContentRegistry);
        SearchIndexTool.Factory.getInstance().init(client, xContentRegistry);
        RAGTool.Factory.getInstance().init(client, xContentRegistry);
        SearchAlertsTool.Factory.getInstance().init(client);
        SearchAnomalyDetectorsTool.Factory.getInstance().init(client);
        SearchAnomalyResultsTool.Factory.getInstance().init(client);
        SearchMonitorsTool.Factory.getInstance().init(client);
        IndexRoutingTool.Factory.getInstance().init(client, xContentRegistry, clusterService);

        return List.of(clusterManagerEventListener);
    }

    @Override
    public List<Tool.Factory<? extends Tool>> getToolFactories() {
        return List
            .of(
                PPLTool.Factory.getInstance(),
                NeuralSparseSearchTool.Factory.getInstance(),
                VectorDBTool.Factory.getInstance(),
                VisualizationsTool.Factory.getInstance(),
                SearchIndexTool.Factory.getInstance(),
                RAGTool.Factory.getInstance(),
                SearchAlertsTool.Factory.getInstance(),
                SearchAnomalyDetectorsTool.Factory.getInstance(),
                SearchAnomalyResultsTool.Factory.getInstance(),
                SearchMonitorsTool.Factory.getInstance(),
                IndexRoutingTool.Factory.getInstance()
            );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List
            .of(
                SkillsClusterManagerEventListener.SKILLS_INDEX_SUMMARY_JOB_INTERVAL,
                SkillsClusterManagerEventListener.SKILLS_INDEX_SUMMARY_JOB_ENABLED
            );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List
            .of(
                new SystemIndexDescriptor(
                    IndexSummaryEmbeddingJob.INDEX_SUMMARY_EMBEDDING_INDEX,
                    "System index for storing index meta and simple data embedding"
                )
            );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        FixedExecutorBuilder indexSummaryJobThreadPool = new FixedExecutorBuilder(
            settings,
            INDEX_SUMMARY_JOB_THREAD_POOL,
            OpenSearchExecutors.allocatedProcessors(settings) * 2,
            100,
            INDEX_SUMMARY_JOB_THREAD_POOL,
            false
        );
        return List.of(indexSummaryJobThreadPool);
    }
}
