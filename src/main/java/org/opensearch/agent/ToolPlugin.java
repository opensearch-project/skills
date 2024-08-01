/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.opensearch.agent.common.SkillSettings;
import org.opensearch.agent.tools.CreateAlertTool;
import org.opensearch.agent.tools.CreateAnomalyDetectorTool;
import org.opensearch.agent.tools.NeuralSparseSearchTool;
import org.opensearch.agent.tools.PPLTool;
import org.opensearch.agent.tools.RAGTool;
import org.opensearch.agent.tools.SearchAlertsTool;
import org.opensearch.agent.tools.SearchAnomalyDetectorsTool;
import org.opensearch.agent.tools.SearchAnomalyResultsTool;
import org.opensearch.agent.tools.SearchMonitorsTool;
import org.opensearch.agent.tools.VectorDBTool;
import org.opensearch.agent.tools.utils.ClusterSettingHelper;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.ml.common.spi.MLCommonsExtension;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import lombok.SneakyThrows;

public class ToolPlugin extends Plugin implements MLCommonsExtension {

    private Client client;
    private ClusterService clusterService;
    private NamedXContentRegistry xContentRegistry;

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
        Settings settings = environment.settings();
        ClusterSettingHelper clusterSettingHelper = new ClusterSettingHelper(settings, clusterService);
        PPLTool.Factory.getInstance().init(client, clusterSettingHelper);
        NeuralSparseSearchTool.Factory.getInstance().init(client, xContentRegistry);
        VectorDBTool.Factory.getInstance().init(client, xContentRegistry);
        RAGTool.Factory.getInstance().init(client, xContentRegistry);
        SearchAlertsTool.Factory.getInstance().init(client);
        SearchAnomalyDetectorsTool.Factory.getInstance().init(client, namedWriteableRegistry);
        SearchAnomalyResultsTool.Factory.getInstance().init(client, namedWriteableRegistry);
        SearchMonitorsTool.Factory.getInstance().init(client);
        CreateAlertTool.Factory.getInstance().init(client);
        CreateAnomalyDetectorTool.Factory.getInstance().init(client);
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
                CreateAnomalyDetectorTool.Factory.getInstance()
            );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SkillSettings.PPL_EXECUTION_ENABLED);
    }
}
