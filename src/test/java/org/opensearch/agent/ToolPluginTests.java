/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

public class ToolPluginTests {

    @Mock
    Client client;
    @Mock
    ClusterService clusterService;
    @Mock
    ThreadPool threadPool;
    @Mock
    ResourceWatcherService resourceWatcherService;
    @Mock
    ScriptService scriptService;
    @Mock
    NamedXContentRegistry xContentRegistry;
    @Mock
    Environment environment;
    @Mock
    NodeEnvironment nodeEnvironment;
    @Mock
    NamedWriteableRegistry namedWriteableRegistry;
    @Mock
    IndexNameExpressionResolver indexNameExpressionResolver;
    @Mock
    Supplier<RepositoriesService> repositoriesServiceSupplier;

    Settings settings;
    @Mock
    RestController restController;
    @Mock
    ClusterSettings clusterSettings;
    @Mock
    IndexScopedSettings indexScopedSettings;
    @Mock
    SettingsFilter settingsFilter;
    @Mock
    Supplier<DiscoveryNodes> nodesInCluster;

    ToolPlugin toolPlugin = new ToolPlugin();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        settings = Settings.builder().put("node.processors", 8).build();
    }

    @Test
    public void test_getRestHandlers_successful() {
        List<RestHandler> restHandlers = toolPlugin
            .getRestHandlers(
                settings,
                restController,
                clusterSettings,
                indexScopedSettings,
                settingsFilter,
                indexNameExpressionResolver,
                nodesInCluster
            );
        assertEquals(0, restHandlers.size());
    }

    @Test
    public void test_getToolFactories_successful() {
        assertEquals(13, toolPlugin.getToolFactories().size());
    }

    @Test
    public void test_getExecutorBuilders_successful() {
        assertEquals(1, toolPlugin.getExecutorBuilders(settings).size());
    }

    @Test
    public void test_createComponent_successful() {
        Collection<Object> collection = toolPlugin
            .createComponents(
                client,
                clusterService,
                threadPool,
                resourceWatcherService,
                scriptService,
                xContentRegistry,
                environment,
                nodeEnvironment,
                namedWriteableRegistry,
                indexNameExpressionResolver,
                repositoriesServiceSupplier
            );
        assertEquals(0, collection.size());
    }

}
