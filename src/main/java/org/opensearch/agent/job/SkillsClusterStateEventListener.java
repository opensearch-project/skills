/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.job;

import static org.opensearch.agent.indices.SkillsIndexEnum.SKILLS_INDEX_SUMMARY;
import static org.opensearch.agent.job.Constants.INDEX_SUMMARY_JOB_THREAD_POOL;

import java.util.List;
import java.util.stream.Collectors;

import org.opensearch.agent.indices.IndicesHelper;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class SkillsClusterStateEventListener implements ClusterStateListener {
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

    private volatile Integer jobInterval;
    private volatile boolean jobEnabled = false;

    private final LockService lockService;

    public SkillsClusterStateEventListener(
        ClusterService clusterService,
        Client client,
        Settings settings,
        ThreadPool threadPool,
        IndicesHelper indicesHelper,
        MLClients mlClients,
        LockService lockService
    ) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService.addListener(this);
        this.indicesHelper = indicesHelper;
        this.mlClients = mlClients;
        this.lockService = lockService;

        this.jobInterval = SKILLS_INDEX_SUMMARY_JOB_INTERVAL.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SKILLS_INDEX_SUMMARY_JOB_INTERVAL, it -> {
            jobInterval = it;
            cancel();
            startJob(false);
        });

        clusterService.getClusterSettings().addSettingsUpdateConsumer(SKILLS_INDEX_SUMMARY_JOB_ENABLED, it -> {
            jobEnabled = it;
            if (!jobEnabled) {
                cancel();
            } else {
                startJob(false);
            }
        });
    }

    public void onNewAgentCreated(String agentId) {
        IndexSummaryEmbeddingJob job = new IndexSummaryEmbeddingJob(
            client,
            clusterService,
            indicesHelper,
            mlClients,
            lockService,
            threadPool
        );
        job.setAdhocAgentId(agentId);
        // wait for agent to be refreshed
        threadPool.schedule(job, TimeValue.timeValueMinutes(1), INDEX_SUMMARY_JOB_THREAD_POOL);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged() || event.isNewCluster()) {
            cancel();
            startJob(true);
        }

        // additional check for index deleted and created
        IndexSummaryEmbeddingJob job = new IndexSummaryEmbeddingJob(
            client,
            clusterService,
            indicesHelper,
            mlClients,
            lockService,
            threadPool
        );
        if (!event.indicesCreated().isEmpty()) {
            job.setAdhocIndexName(event.indicesCreated());
            threadPool.schedule(job, TimeValue.timeValueMinutes(1), INDEX_SUMMARY_JOB_THREAD_POOL);
        }

        if (!event.indicesDeleted().isEmpty() && clusterService.state().metadata().hasIndex(SKILLS_INDEX_SUMMARY.getIndexName())) {
            threadPool.executor(INDEX_SUMMARY_JOB_THREAD_POOL).execute(() -> {
                List<String> indexNames = event.indicesDeleted().stream().map(Index::getName).collect(Collectors.toList());
                job.bulkDelete(SKILLS_INDEX_SUMMARY.getIndexName(), indexNames);
            });
        }
    }

    private void startJob(boolean onetime) {
        if (!jobEnabled)
            return;
        IndexSummaryEmbeddingJob job = new IndexSummaryEmbeddingJob(
            client,
            clusterService,
            indicesHelper,
            mlClients,
            lockService,
            threadPool
        );
        if (onetime) {
            // trigger the one-shot job
            threadPool.schedule(job, TimeValue.timeValueMinutes(5), INDEX_SUMMARY_JOB_THREAD_POOL);
        } else {
            if (jobInterval > 0) {
                job.setIncremental(true);
                jobcron = threadPool.scheduleWithFixedDelay(job, TimeValue.timeValueMinutes(jobInterval), INDEX_SUMMARY_JOB_THREAD_POOL);
            }
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel();
                }
            });
        }
    }

    private void cancel() {
        if (this.jobcron != null) {
            log.debug("Cancel the index summary embedding job scheduler");
            this.jobcron.cancel();
            this.jobcron = null;
        }
    }
}
