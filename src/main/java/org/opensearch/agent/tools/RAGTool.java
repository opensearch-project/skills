/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.apache.commons.lang3.StringEscapeUtils.escapeJson;
import static org.opensearch.agent.tools.VectorDBTool.DEFAULT_K;
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
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Parser;
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
public class RAGTool extends AbstractRetrieverTool {
    public static final String TYPE = "RAGTool";
    public static String DEFAULT_DESCRIPTION =
        "Use this tool to retrieve helpful information to optimize the output of the large language model to answer questions.";
    public static final String INFERENCE_MODEL_ID_FIELD = "inference_model_id";
    public static final String EMBEDDING_MODEL_ID_FIELD = "embedding_model_id";
    public static final String EMBEDDING_FIELD = "embedding_field";
    public static final String OUTPUT_FIELD = "output_field";
    private String name = TYPE;
    private String description = DEFAULT_DESCRIPTION;
    private Client client;
    private String inferenceModelId;
    private NamedXContentRegistry xContentRegistry;
    private String index;
    private String embeddingField;
    private String[] sourceFields;
    private String embeddingModelId;
    private Integer docSize;
    private Integer k;
    @Setter
    private Parser inputParser;
    @Setter
    private Parser outputParser;

    @Builder
    public RAGTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        String index,
        String embeddingField,
        String[] sourceFields,
        Integer k,
        Integer docSize,
        String embeddingModelId,
        String inferenceModelId
    ) {
        super(client, xContentRegistry, index, sourceFields, docSize);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.index = index;
        this.embeddingField = embeddingField;
        this.sourceFields = sourceFields;
        this.embeddingModelId = embeddingModelId;
        this.docSize = docSize == null ? DEFAULT_DOC_SIZE : docSize;
        this.k = k == null ? DEFAULT_K : k;
        this.inferenceModelId = inferenceModelId;

        outputParser = new Parser() {
            @Override
            public Object parse(Object o) {
                List<ModelTensors> mlModelOutputs = (List<ModelTensors>) o;
                return mlModelOutputs.get(0).getMlModelTensors().get(0).getDataAsMap().get("response");
            }
        };
    }

    // getQueryBody is not used in RAGTool
    @Override
    protected String getQueryBody(String queryText) {
        return queryText;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        String input = null;

        try {
            if (!this.validate(parameters)) {
                throw new IllegalArgumentException("[" + INPUT_FIELD + "] is null or empty, can not process it.");
            }
            String question = parameters.get(INPUT_FIELD);
            input = gson.fromJson(question, String.class);
        } catch (Exception e) {
            log.error("[" + INPUT_FIELD + "] is null or empty, can not process it.", e);
            listener.onFailure(e);
            return;
        }

        Map<String, Object> params = new HashMap<>();
        VectorDBTool.Factory.getInstance().init(client, xContentRegistry);
        params.put(VectorDBTool.INDEX_FIELD, this.index);
        params.put(VectorDBTool.EMBEDDING_FIELD, this.embeddingField);
        params.put(VectorDBTool.SOURCE_FIELD, gson.toJson(this.sourceFields));
        params.put(VectorDBTool.MODEL_ID_FIELD, this.embeddingModelId);
        params.put(VectorDBTool.DOC_SIZE_FIELD, String.valueOf(this.docSize));
        params.put(VectorDBTool.K_FIELD, String.valueOf(this.k));
        VectorDBTool vectorDBTool = VectorDBTool.Factory.getInstance().create(params);

        String embeddingInput = input;
        ActionListener actionListener = ActionListener.<T>wrap(r -> {
            T vectorDBToolOutput;

            if (r.equals("Can not get any match from search result.")) {
                vectorDBToolOutput = (T) "";
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

                vectorDBToolOutput = (T) gson.toJson(resultBuilder.toString());
            }

            Map<String, String> tmpParameters = new HashMap<>();
            tmpParameters.putAll(parameters);

            if (vectorDBToolOutput instanceof List
                && !((List) vectorDBToolOutput).isEmpty()
                && ((List) vectorDBToolOutput).get(0) instanceof ModelTensors) {
                ModelTensors tensors = (ModelTensors) ((List) vectorDBToolOutput).get(0);
                Object response = tensors.getMlModelTensors().get(0).getDataAsMap().get("response");
                tmpParameters.put(OUTPUT_FIELD, response + "");
            } else if (vectorDBToolOutput instanceof ModelTensor) {
                tmpParameters.put(OUTPUT_FIELD, escapeJson(toJson(((ModelTensor) vectorDBToolOutput).getDataAsMap())));
            } else {
                if (vectorDBToolOutput instanceof String) {
                    tmpParameters.put(OUTPUT_FIELD, (String) vectorDBToolOutput);
                } else {
                    tmpParameters.put(OUTPUT_FIELD, escapeJson(toJson(vectorDBToolOutput.toString())));
                }
            }

            RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder().parameters(tmpParameters).build();
            MLInput mlInput = MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build();
            ActionRequest request = new MLPredictionTaskRequest(inferenceModelId, mlInput, null);

            client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(resp -> {
                ModelTensorOutput modelTensorOutput = (ModelTensorOutput) resp.getOutput();
                modelTensorOutput.getMlModelOutputs();
                if (outputParser == null) {
                    listener.onResponse((T) modelTensorOutput.getMlModelOutputs());
                } else {
                    listener.onResponse((T) outputParser.parse(modelTensorOutput.getMlModelOutputs()));
                }
            }, e -> {
                log.error("Failed to run model " + inferenceModelId, e);
                listener.onFailure(e);
            }));
        }, e -> {
            log.error("Failed to search index.", e);
            listener.onFailure(e);
        });
        vectorDBTool.run(Map.of(VectorDBTool.INPUT_FIELD, embeddingInput), actionListener);

    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String s) {
        this.name = s;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        if (parameters == null || parameters.size() == 0) {
            return false;
        }
        String question = parameters.get(INPUT_FIELD);
        return question != null && !question.trim().isEmpty();
    }

    public static class Factory extends AbstractRetrieverTool.Factory<RAGTool> {
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
            String embeddingModelId = (String) params.get(EMBEDDING_MODEL_ID_FIELD);
            String index = (String) params.get(INDEX_FIELD);
            String embeddingField = (String) params.get(EMBEDDING_FIELD);
            String[] sourceFields = gson.fromJson((String) params.get(SOURCE_FIELD), String[].class);
            String inferenceModelId = (String) params.get(INFERENCE_MODEL_ID_FIELD);
            Integer docSize = params.containsKey(DOC_SIZE_FIELD) ? Integer.parseInt((String) params.get(DOC_SIZE_FIELD)) : 2;
            return RAGTool
                .builder()
                .client(client)
                .xContentRegistry(xContentRegistry)
                .index(index)
                .embeddingField(embeddingField)
                .sourceFields(sourceFields)
                .embeddingModelId(embeddingModelId)
                .docSize(docSize)
                .inferenceModelId(inferenceModelId)
                .build();
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
