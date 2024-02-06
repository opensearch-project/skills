/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.apache.commons.lang3.StringEscapeUtils.escapeJson;
import static org.opensearch.agent.tools.AbstractRetrieverTool.*;
import static org.opensearch.ml.common.utils.StringUtils.gson;
import static org.opensearch.ml.common.utils.StringUtils.toJson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * This tool supports retrieving helpful information to optimize the output of the large language model to answer questions..
 */
@Log4j2
@Setter
@Getter
@ToolAnnotation(RAGTool.TYPE)
public class RAGTool implements Tool {
    public static final String TYPE = "RAGTool";
    public static String DEFAULT_DESCRIPTION =
        "Use this tool to retrieve helpful information to optimize the output of the large language model to answer questions.";
    public static final String INFERENCE_MODEL_ID_FIELD = "inference_model_id";
    public static final String EMBEDDING_MODEL_ID_FIELD = "embedding_model_id";
    public static final String INDEX_FIELD = "index";
    public static final String SOURCE_FIELD = "source_field";
    public static final String DOC_SIZE_FIELD = "doc_size";
    public static final String EMBEDDING_FIELD = "embedding_field";
    public static final String OUTPUT_FIELD = "output_field";
    public static final String QUERY_TYPE = "query_type";
    public static final String CONTENT_GENERATION_FIELD = "enable_Content_Generation";
    public static final String K_FIELD = "k";
    private final AbstractRetrieverTool queryTool;
    private String name = TYPE;
    private String description = DEFAULT_DESCRIPTION;
    private Client client;
    private String inferenceModelId;
    private Boolean enableContentGeneration;
    private NamedXContentRegistry xContentRegistry;
    private String queryType;
    @Setter
    private Parser inputParser;
    @Setter
    private Parser outputParser;

    @Override
    public boolean useOriginalInput() {
        return true;
    }

    @Builder
    public RAGTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        String inferenceModelId,
        Boolean enableContentGeneration,
        AbstractRetrieverTool queryTool
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.inferenceModelId = inferenceModelId;
        this.enableContentGeneration = enableContentGeneration;
        this.queryTool = queryTool;
        outputParser = new Parser() {
            @Override
            public Object parse(Object o) {
                List<ModelTensors> mlModelOutputs = (List<ModelTensors>) o;
                return mlModelOutputs.get(0).getMlModelTensors().get(0).getDataAsMap().get("response");
            }
        };
    }

    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        String input = null;

        if (!this.validate(parameters)) {
            throw new IllegalArgumentException("[" + INPUT_FIELD + "] is null or empty, can not process it.");
        }

        try {
            input = parameters.get(INPUT_FIELD);
        } catch (Exception e) {
            log.error("Failed to read question from " + INPUT_FIELD, e);
            listener.onFailure(new IllegalArgumentException("Failed to read question from " + INPUT_FIELD));
            return;
        }

        String embeddingInput = input;
        ActionListener actionListener = ActionListener.<T>wrap(r -> {
            T queryToolOutput;
            if (!this.enableContentGeneration) {
                listener.onResponse(r);
            }
            if (r.equals("Can not get any match from search result.")) {
                queryToolOutput = (T) "";
            } else {
                Gson gson = new Gson();
                String[] hits = r.toString().split("\n");

                StringBuilder resultBuilder = new StringBuilder();
                for (String hit : hits) {
                    JsonObject jsonObject = gson.fromJson(hit, JsonObject.class);
                    String id = jsonObject.get("_id").getAsString();
                    JsonObject source = jsonObject.getAsJsonObject("_source");

                    resultBuilder.append("_id: ").append(id).append("\n");
                    resultBuilder.append("_source: ").append(source.toString()).append("\n");
                }

                queryToolOutput = (T) gson.toJson(resultBuilder.toString());
            }

            Map<String, String> tmpParameters = new HashMap<>();
            tmpParameters.putAll(parameters);

            if (queryToolOutput instanceof String) {
                tmpParameters.put(OUTPUT_FIELD, (String) queryToolOutput);
            } else {
                tmpParameters.put(OUTPUT_FIELD, escapeJson(toJson(queryToolOutput.toString())));
            }

            RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder().parameters(tmpParameters).build();
            MLInput mlInput = MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build();
            ActionRequest request = new MLPredictionTaskRequest(this.inferenceModelId, mlInput, null);

            client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(resp -> {
                ModelTensorOutput modelTensorOutput = (ModelTensorOutput) resp.getOutput();
                modelTensorOutput.getMlModelOutputs();
                if (outputParser == null) {
                    listener.onResponse((T) modelTensorOutput.getMlModelOutputs());
                } else {
                    listener.onResponse((T) outputParser.parse(modelTensorOutput.getMlModelOutputs()));
                }
            }, e -> {
                log.error("Failed to run model " + this.inferenceModelId, e);
                listener.onFailure(e);
            }));
        }, e -> {
            log.error("Failed to search index.", e);
            listener.onFailure(e);
        });
        this.queryTool.run(Map.of(INPUT_FIELD, embeddingInput), actionListener);
    }

    public String getType() {
        return TYPE;
    }

    @Override
    public String getVersion() {
        return null;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String s) {
        this.name = s;
    }

    public boolean validate(Map<String, String> parameters) {
        if (parameters == null || parameters.size() == 0) {
            return false;
        }
        String question = parameters.get(INPUT_FIELD);
        return question != null && !question.trim().isEmpty();
    }

    /**
     * Factory class to create RAGTool
     */
    public static class Factory implements Tool.Factory<RAGTool> {
        private Client client;
        private NamedXContentRegistry xContentRegistry;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (RAGTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client, NamedXContentRegistry xContentRegistry) {
            this.client = client;
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public RAGTool create(Map<String, Object> params) {
            String queryType = params.containsKey(QUERY_TYPE) ? (String) params.get(QUERY_TYPE) : "neural";
            String embeddingModelId = (String) params.get(EMBEDDING_MODEL_ID_FIELD);
            Boolean enableContentGeneration = params.containsKey(CONTENT_GENERATION_FIELD)
                ? Boolean.parseBoolean((String) params.get(CONTENT_GENERATION_FIELD))
                : true;
            String inferenceModelId = enableContentGeneration ? (String) params.get(INFERENCE_MODEL_ID_FIELD) : "";
            switch (queryType) {
                case "neural_sparse":
                    params.put(NeuralSparseSearchTool.MODEL_ID_FIELD, embeddingModelId);
                    NeuralSparseSearchTool neuralSparseSearchTool = NeuralSparseSearchTool.Factory.getInstance().create(params);
                    return RAGTool
                        .builder()
                        .client(client)
                        .xContentRegistry(xContentRegistry)
                        .inferenceModelId(inferenceModelId)
                        .enableContentGeneration(enableContentGeneration)
                        .queryTool(neuralSparseSearchTool)
                        .build();
                case "neural":
                    params.put(VectorDBTool.MODEL_ID_FIELD, embeddingModelId);
                    VectorDBTool vectorDBTool = VectorDBTool.Factory.getInstance().create(params);
                    return RAGTool
                        .builder()
                        .client(client)
                        .xContentRegistry(xContentRegistry)
                        .inferenceModelId(inferenceModelId)
                        .enableContentGeneration(enableContentGeneration)
                        .queryTool(vectorDBTool)
                        .build();
                default:
                    log.error("Failed to read queryType, please input neural_sparse or neural.");
                    throw new IllegalArgumentException("Failed to read queryType, please input neural_sparse or neural.");
            }

        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }
    }
}
