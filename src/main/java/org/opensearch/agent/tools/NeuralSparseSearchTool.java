/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.Client;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * This tool supports neural_sparse search with sparse encoding models and rank_features field.
 */
@Log4j2
@Getter
@Setter
@ToolAnnotation(NeuralSparseSearchTool.TYPE)
public class NeuralSparseSearchTool extends AbstractRetrieverTool {
    public static final String TYPE = "NeuralSparseSearchTool";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String EMBEDDING_FIELD = "embedding_field";

    private String name = TYPE;
    private String modelId;
    private String embeddingField;

    @Builder
    public NeuralSparseSearchTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        String index,
        String embeddingField,
        String[] sourceFields,
        Integer docSize,
        String modelId
    ) {
        super(client, xContentRegistry, index, sourceFields, docSize);
        this.modelId = modelId;
        this.embeddingField = embeddingField;
    }

    @Override
    protected String getQueryBody(String queryText) {
        if (StringUtils.isBlank(embeddingField) || StringUtils.isBlank(modelId)) {
            throw new IllegalArgumentException(
                "Parameter [" + EMBEDDING_FIELD + "] and [" + MODEL_ID_FIELD + "] can not be null or empty."
            );
        }
        return "{\"query\":{\"neural_sparse\":{\""
            + embeddingField
            + "\":{\"query_text\":\""
            + queryText
            + "\",\"model_id\":\""
            + modelId
            + "\"}}}"
            + " }";
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory extends AbstractRetrieverTool.Factory<NeuralSparseSearchTool> {
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
            return NeuralSparseSearchTool
                .builder()
                .client(client)
                .xContentRegistry(xContentRegistry)
                .index(index)
                .embeddingField(embeddingField)
                .sourceFields(sourceFields)
                .modelId(modelId)
                .docSize(docSize)
                .build();
        }
    }
}
