/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.job;

import static org.opensearch.agent.indices.SkillsIndexEnum.SKILLS_INDEX_SUMMARY;
import static org.opensearch.threadpool.ThreadPool.Names.GENERIC;

import java.util.List;
import java.util.stream.Collectors;

import org.opensearch.agent.indices.IndicesHelper;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class SkillsClusterManagerEventListener implements LocalNodeClusterManagerListener {
    public static final Setting<Integer> SKILLS_INDEX_SUMMARY_JOB_INTERVAL = Setting
        .intSetting(
            "plugins.skills.index_summary_embedding_job_interval_in_minutes",
            60,
            5,
            1440,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Boolean> SKILLS_INDEX_SUMMARY_JOB_ENABLED = Setting
        .boolSetting("plugins.skills.index_summary_embedding_job_enabled", true, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private final ClusterService clusterService;
    private final Client client;
    private final IndicesHelper indicesHelper;
    private final MLClients mlClients;

    private final ThreadPool threadPool;
    private Scheduler.Cancellable jobcron;
    private Scheduler.Cancellable jobCronForAgent;

    private volatile Integer jobInterval;
    private volatile boolean jobEnabled;
    private volatile boolean isClusterManager;

    public SkillsClusterManagerEventListener(
        ClusterService clusterService,
        Client client,
        Settings settings,
        ThreadPool threadPool,
        IndicesHelper indicesHelper,
        MLClients mlClients
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService.addListener(this);
        this.indicesHelper = indicesHelper;
        this.mlClients = mlClients;

        this.jobInterval = SKILLS_INDEX_SUMMARY_JOB_INTERVAL.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SKILLS_INDEX_SUMMARY_JOB_INTERVAL, it -> {
            jobInterval = it;
            cancel(jobcron);
            cancel(jobCronForAgent);
            startJob();
        });

        clusterService.getClusterSettings().addSettingsUpdateConsumer(SKILLS_INDEX_SUMMARY_JOB_ENABLED, it -> {
            jobEnabled = it;
            if (!jobEnabled) {
                cancel(jobcron);
                cancel(jobCronForAgent);
            } else {
                startJob();
            }
        });
    }

    @Override
    public void onClusterManager() {
        isClusterManager = true;
        if (jobcron == null) {
            startJob();
        }
    }

    @Override
    public void offClusterManager() {
        isClusterManager = false;
        cancel(jobcron);
        jobcron = null;

        cancel(jobCronForAgent);
        jobCronForAgent = null;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        LocalNodeClusterManagerListener.super.clusterChanged(event);

        // additional check for index deleted and created
        if (isClusterManager) {
            IndexSummaryEmbeddingJob job = new IndexSummaryEmbeddingJob(client, clusterService, indicesHelper, mlClients);
            if (!event.indicesCreated().isEmpty()) {
                job.setAdhocIndexName(event.indicesCreated());
                threadPool.schedule(job, TimeValue.timeValueSeconds(30), GENERIC);
            }

            if (!event.indicesDeleted().isEmpty() && clusterService.state().metadata().hasIndex(SKILLS_INDEX_SUMMARY.getIndexName())) {
                List<String> indexNames = event.indicesDeleted().stream().map(Index::getName).collect(Collectors.toList());
                job.bulkDelete(SKILLS_INDEX_SUMMARY.getIndexName(), indexNames);
            }
        }
    }

    private void startJob() {
        if (!isClusterManager || !jobEnabled)
            return;
        if (jobInterval > 0) {
            IndexSummaryEmbeddingJob job = new IndexSummaryEmbeddingJob(client, clusterService, indicesHelper, mlClients);
            AgentMonitorJob agentMonitorJob = new AgentMonitorJob(clusterService, client, indicesHelper, mlClients, threadPool);
            // trigger the one-shot job
            threadPool.schedule(job, TimeValue.timeValueSeconds(30), GENERIC);

            // schedule the cron job
            log.debug("Scheduling index summary embedding job...");
            jobcron = threadPool.scheduleWithFixedDelay(job, TimeValue.timeValueMinutes(jobInterval), GENERIC);
            jobCronForAgent = threadPool.scheduleWithFixedDelay(agentMonitorJob, TimeValue.timeValueMinutes(5), GENERIC);
        }
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                cancel(jobcron);
                jobcron = null;

                cancel(jobCronForAgent);
                jobCronForAgent = null;
            }
        });
    }

    private void cancel(Scheduler.Cancellable cron) {
        if (cron != null) {
            log.debug("Cancel the index summary embedding job scheduler");
            cron.cancel();
        }
    }
}
