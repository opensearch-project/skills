/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.Client;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.ml.common.spi.tools.WithModelTool;

/**
 * This tool supports neural_sparse search with sparse encoding models and rank_features field.
 */
@Log4j2
@Getter
@Setter
@ToolAnnotation(NeuralSparseSearchTool.TYPE)
public class NeuralSparseSearchTool extends AbstractRetrieverTool implements WithModelTool {
    public static final String TYPE = "NeuralSparseSearchTool";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String EMBEDDING_FIELD = "embedding_field";
    public static final String NESTED_PATH_FIELD = "nested_path";

    private String name = TYPE;
    private String modelId;
    private String embeddingField;
    private String nestedPath;

    @Builder
    public NeuralSparseSearchTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        String index,
        String embeddingField,
        String[] sourceFields,
        Integer docSize,
        String modelId,
        String nestedPath
    ) {
        super(client, xContentRegistry, index, sourceFields, docSize);
        this.modelId = modelId;
        this.embeddingField = embeddingField;
        this.nestedPath = nestedPath;
    }

    @Override
    protected String getQueryBody(String queryText) {
        if (StringUtils.isBlank(embeddingField) || StringUtils.isBlank(modelId)) {
            throw new IllegalArgumentException(
                "Parameter [" + EMBEDDING_FIELD + "] and [" + MODEL_ID_FIELD + "] can not be null or empty."
            );
        }

        Map<String, Object> queryBody;
        if (StringUtils.isBlank(nestedPath)) {
            queryBody = Map
                .of("query", Map.of("neural_sparse", Map.of(embeddingField, Map.of("query_text", queryText, "model_id", modelId))));
        } else {
            queryBody = Map
                .of(
                    "query",
                    Map
                        .of(
                            "nested",
                            Map
                                .of(
                                    "path",
                                    nestedPath,
                                    "score_mode",
                                    "max",
                                    "query",
                                    Map.of("neural_sparse", Map.of(embeddingField, Map.of("query_text", queryText, "model_id", modelId)))
                                )
                        )
                );
        }

        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(queryBody));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory extends AbstractRetrieverTool.Factory<NeuralSparseSearchTool> implements WithModelTool.Factory<NeuralSparseSearchTool> {
        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (NeuralSparseSearchTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        @Override
        public NeuralSparseSearchTool create(Map<String, Object> params) {
            String index = (String) params.get(INDEX_FIELD);
            String embeddingField = (String) params.get(EMBEDDING_FIELD);
            String[] sourceFields = gson.fromJson((String) params.get(SOURCE_FIELD), String[].class);
            String modelId = (String) params.get(MODEL_ID_FIELD);
            Integer docSize = params.containsKey(DOC_SIZE_FIELD) ? Integer.parseInt((String) params.get(DOC_SIZE_FIELD)) : DEFAULT_DOC_SIZE;
            String nestedPath = (String) params.get(NESTED_PATH_FIELD);
            return NeuralSparseSearchTool
                .builder()
                .client(client)
                .xContentRegistry(xContentRegistry)
                .index(index)
                .embeddingField(embeddingField)
                .sourceFields(sourceFields)
                .modelId(modelId)
                .docSize(docSize)
                .nestedPath(nestedPath)
                .build();
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }

        @Override
        public List<String> getAllModelKeys() {
            return List.of(MODEL_ID_FIELD);
        }
    }
}
