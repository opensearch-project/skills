/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.indices;

import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.INDEX_SUMMARY_EMBEDDING_FIELD_PREFIX;
import static org.opensearch.ml.common.CommonValue.META;
import static org.opensearch.ml.common.CommonValue.SCHEMA_VERSION_FIELD;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.text.StringSubstitutor;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.agent.job.MLClients;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.ml.common.CommonValue;
import org.opensearch.ml.common.exception.MLException;
import org.opensearch.ml.common.output.model.ModelTensorOutput;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@Log4j2
public class IndicesHelper {

    ClusterService clusterService;
    Client client;
    MLClients mlClients;
    private static final Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();

    static {
        for (SkillsIndexEnum index : SkillsIndexEnum.values()) {
            indexMappingUpdated.put(index.getIndexName(), new AtomicBoolean(false));
        }
    }

    public void initIndexSummaryEmbeddingIndex(ActionListener<Boolean> listener) {
        initIndexIfAbsent(SkillsIndexEnum.SKILLS_INDEX_SUMMARY_EMBEDDING_INDEX, listener);
    }

    public void initIndexIfAbsent(SkillsIndexEnum skillsIndexEnum, ActionListener<Boolean> listener) {
        try (
            ThreadContext.StoredContext threadContext = client.threadPool().getThreadContext().stashContext();
            InputStream settingIns = this.getClass().getResourceAsStream(skillsIndexEnum.getSetting());
            InputStream mappingIns = this.getClass().getResourceAsStream(skillsIndexEnum.getMapping())
        ) {
            String setting = new String(Objects.requireNonNull(settingIns).readAllBytes(), StandardCharsets.UTF_8);
            String mapping = new String(Objects.requireNonNull(mappingIns).readAllBytes(), StandardCharsets.UTF_8);
            ActionListener<Boolean> internalListener = ActionListener.runBefore(listener, threadContext::restore);

            if (!clusterService.state().metadata().hasIndex(skillsIndexEnum.getIndexName())) {
                ActionListener<CreateIndexResponse> actionListener = ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        log.info("create index:{}", skillsIndexEnum.getIndexName());
                        internalListener.onResponse(true);
                    } else {
                        internalListener.onResponse(false);
                    }
                }, e -> {
                    log.error("Failed to create index " + skillsIndexEnum, e);
                    internalListener.onFailure(e);
                });

                CreateIndexRequest request = new CreateIndexRequest(skillsIndexEnum.getIndexName())
                    .mapping(mapping)
                    .settings(setting, MediaTypeRegistry.JSON);
                client.admin().indices().create(request, actionListener);
            } else {
                log.debug("index:{} is already created", skillsIndexEnum.getIndexName());
                if (indexMappingUpdated.containsKey(skillsIndexEnum.getIndexName())
                    && !indexMappingUpdated.get(skillsIndexEnum.getIndexName()).get()) {
                    shouldUpdateIndex(skillsIndexEnum.getIndexName(), skillsIndexEnum.getVersion(), ActionListener.wrap(r -> {
                        if (r) {
                            // return true if should update skillsIndexEnum
                            client
                                .admin()
                                .indices()
                                .putMapping(
                                    new PutMappingRequest().indices(skillsIndexEnum.getIndexName()).source(mapping, MediaTypeRegistry.JSON),
                                    ActionListener.wrap(response -> {
                                        if (response.isAcknowledged()) {
                                            internalListener.onResponse(true);
                                        } else {
                                            internalListener
                                                .onFailure(new MLException("Failed to update skillsIndexEnum: " + skillsIndexEnum));
                                        }
                                    }, exception -> {
                                        log.error("Failed to update skillsIndexEnum " + skillsIndexEnum, exception);
                                        internalListener.onFailure(exception);
                                    })
                                );
                        } else {
                            // no need to update skillsIndexEnum if it does not exist or the version is already up-to-date.
                            indexMappingUpdated.get(skillsIndexEnum.getIndexName()).set(true);
                            internalListener.onResponse(true);
                        }
                    }, e -> {
                        log.error("Failed to update skillsIndexEnum mapping", e);
                        internalListener.onFailure(e);
                    }));
                } else {
                    // No need to update skillsIndexEnum if it's not system skillsIndexEnum or it's already updated.
                    internalListener.onResponse(true);
                }
            }
        } catch (Exception e) {
            log.error("Failed to init skillsIndexEnum " + skillsIndexEnum, e);
            listener.onFailure(e);
        }
    }

    /**
     * Check if we should update index based on schema version.
     * @param indexName index name
     * @param newVersion new index mapping version
     * @param listener action listener, if should update index, will pass true to its onResponse method
     */
    public void shouldUpdateIndex(String indexName, Integer newVersion, ActionListener<Boolean> listener) {
        IndexMetadata indexMetaData = clusterService.state().getMetadata().indices().get(indexName);
        if (indexMetaData == null) {
            listener.onResponse(Boolean.FALSE);
            return;
        }
        Integer oldVersion = CommonValue.NO_SCHEMA_VERSION;
        Map<String, Object> indexMapping = indexMetaData.mapping().getSourceAsMap();
        Object meta = indexMapping.get(META);
        if (meta instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metaMapping = (Map<String, Object>) meta;
            Object schemaVersion = metaMapping.get(SCHEMA_VERSION_FIELD);
            if (schemaVersion instanceof Integer) {
                oldVersion = (Integer) schemaVersion;
            }
        }
        listener.onResponse(newVersion > oldVersion);
    }

    public void addNewVectorField(String index, String modelId, ActionListener<Boolean> listener) {
        IndexMetadata metadata = clusterService.state().metadata().index(index);
        String fieldName = String.format(Locale.ROOT, "%s_%s", INDEX_SUMMARY_EMBEDDING_FIELD_PREFIX, modelId);
        Map<String, Object> fieldMap = (Map<String, Object>) metadata.mapping().getSourceAsMap().get("properties");
        boolean vectorFieldExists = fieldMap.containsKey(fieldName);
        if (!vectorFieldExists) {
            try (InputStream vectorFieldIns = this.getClass().getResourceAsStream("/vector_field.json")) {
                String vectorField = new String(Objects.requireNonNull(vectorFieldIns).readAllBytes(), StandardCharsets.UTF_8);
                Long dimension = getDimension(modelId);
                StringSubstitutor substitutor = new StringSubstitutor(Map.of("model_id", modelId, "dimension", dimension));
                PutMappingRequest request = new PutMappingRequest(index).source(substitutor.replace(vectorField), XContentType.JSON);
                client.admin().indices().putMapping(request, ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        listener.onResponse(true);
                    } else {
                        listener.onResponse(false);
                    }
                }, listener::onFailure));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        } else {
            listener.onResponse(true);
        }
    }

    private Long getDimension(String modelId) {
        return mlClients.getEmbeddingResult(modelId, List.of("today is sunny"), mlTaskResponse -> {
            ModelTensorOutput tensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
            return tensorOutput.getMlModelOutputs().get(0).getMlModelTensors().get(0).getShape()[0];
        });
    }
}
