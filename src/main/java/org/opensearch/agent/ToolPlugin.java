/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.opensearch.agent.tools.CreateAlertTool;
import org.opensearch.agent.tools.CreateAnomalyDetectorTool;
import org.opensearch.agent.tools.LogPatternAnalysisTool;
import org.opensearch.agent.tools.LogPatternTool;
import org.opensearch.agent.tools.NeuralSparseSearchTool;
import org.opensearch.agent.tools.PPLTool;
import org.opensearch.agent.tools.RAGTool;
import org.opensearch.agent.tools.SearchAlertsTool;
import org.opensearch.agent.tools.SearchAnomalyDetectorsTool;
import org.opensearch.agent.tools.SearchAnomalyResultsTool;
import org.opensearch.agent.tools.SearchMonitorsTool;
import org.opensearch.agent.tools.VectorDBTool;
import org.opensearch.agent.tools.WebSearchTool;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.ml.common.spi.MLCommonsExtension;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import com.google.common.collect.ImmutableList;

import lombok.SneakyThrows;

public class ToolPlugin extends Plugin implements MLCommonsExtension, ActionPlugin {

    private final AtomicReference<RestController> restControllerRef = new AtomicReference<>();
    public static final String SKILLS_THREAD_POOL_PREFIX = "thread_pool.skills";
    public static final String WEBSEARCH_CRAWLER_THREADPOOL = "websearch-crawler-threadpool";

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        restControllerRef.set(restController);
        return Collections.emptyList();
    }

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
        PPLTool.Factory.getInstance().init(client);
        NeuralSparseSearchTool.Factory.getInstance().init(client, xContentRegistry);
        VectorDBTool.Factory.getInstance().init(client, xContentRegistry);
        RAGTool.Factory.getInstance().init(client, xContentRegistry);
        SearchAlertsTool.Factory.getInstance().init(client);
        SearchAnomalyDetectorsTool.Factory.getInstance().init(client, namedWriteableRegistry);
        SearchAnomalyResultsTool.Factory.getInstance().init(client, namedWriteableRegistry);
        SearchMonitorsTool.Factory.getInstance().init(client);
        CreateAlertTool.Factory.getInstance().init(client);
        CreateAnomalyDetectorTool.Factory.getInstance().init(client);
        LogPatternTool.Factory.getInstance().init(client, xContentRegistry);
        WebSearchTool.Factory.getInstance().init(threadPool);
        LogPatternAnalysisTool.Factory.getInstance().init(client);
        return Collections.emptyList();
    }

    @Override
    public List<Tool.Factory<? extends Tool>> getToolFactories() {
        return List
            .of(
                PPLTool.Factory.getInstance(),
                NeuralSparseSearchTool.Factory.getInstance(),
                VectorDBTool.Factory.getInstance(),
                RAGTool.Factory.getInstance(),
                SearchAlertsTool.Factory.getInstance(),
                SearchAnomalyDetectorsTool.Factory.getInstance(),
                SearchAnomalyResultsTool.Factory.getInstance(),
                SearchMonitorsTool.Factory.getInstance(),
                CreateAlertTool.Factory.getInstance(),
                CreateAnomalyDetectorTool.Factory.getInstance(),
                LogPatternTool.Factory.getInstance(),
                WebSearchTool.Factory.getInstance()
                LogPatternAnalysisTool.Factory.getInstance()
            );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        FixedExecutorBuilder websearchCrawlThread = new FixedExecutorBuilder(
            settings,
            WEBSEARCH_CRAWLER_THREADPOOL,
            Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) - 1),
            100,
            SKILLS_THREAD_POOL_PREFIX,
            false
        );

        return ImmutableList.of(websearchCrawlThread);
    }

}
