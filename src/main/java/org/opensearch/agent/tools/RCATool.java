/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.apache.commons.text.StringEscapeUtils.unescapeJson;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.agent.tools.utils.ClusterStatsUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.ml.common.utils.StringUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
@ToolAnnotation(RCATool.TYPE)
public class RCATool implements Tool {

    public static final String TYPE = "RCATool";
    private String name = TYPE;
    private String description = DEFAULT_DESCRIPTION;

    private final Client client;
    private final String modelId;
    private final String embeddingModelId;
    private final Boolean isLLMOption;
    private static final String MODEL_ID = "model_id";
    private static final String EMBEDDING_MODEL_ID_FIELD = "embedding_model_id";
    private static final String IS_LLM_OPTION = "is_llm_option";

    private static final String KNOWLEDGE_BASE_TOOL_OUTPUT_FIELD = KnowledgeBaseTool.TYPE + ".output";
    private static final String API_URL_FIELD = "api_url";
    private static final String INDEX_FIELD = "index";
    private static final String DEFAULT_DESCRIPTION = "Use this tool to perform RCA analysis";

    public RCATool(Client client, String modelId, String embeddingModelId, Boolean isLLMOption) {
        this.client = client;
        this.modelId = modelId;
        this.embeddingModelId = embeddingModelId;
        this.isLLMOption = isLLMOption;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return parameters != null;
    }

    public static final String TOOL_PROMPT =
        "You are going to help find the root cause of the phenomenon from the several potential causes listed below. In this RCA process, for each cause, it usually needs to call an API to get some necessary information verify whether it's the right root cause. I've filled the related response for each cause, you should decide which cause are most possible to be the root cause based on these responses. \n\n"
            + "Human: PHENOMENON\n"
            + "--------------------\n"
            + "${parameters.phenomenon} \n\n"
            + "Human: POTENTIAL CAUSES\n"
            + "--------------------\n"
            + "${parameters.causes} \n\n"
            + "Human: API RESPONSES\n"
            + "${parameters.responses} \n\n"
            + "--------------------\n"
            + "Assistant: ";

    @SuppressWarnings("unchecked")
    public <T> void runOption1(Map<String, String> parameters, ActionListener<T> listener) {
        String knowledge = parameters.get(KNOWLEDGE_BASE_TOOL_OUTPUT_FIELD);
        knowledge = unescapeJson(knowledge);
        Map<String, ?> knowledgeBase = StringUtils.gson.fromJson(knowledge, Map.class);
        List<Map<String, String>> causes = (List<Map<String, String>>) knowledgeBase.get("causes");
        List<String> apiList = causes.stream().map(cause -> cause.get(API_URL_FIELD)).distinct().collect(Collectors.toList());
        final GroupedActionListener<Pair<String, String>> groupedListener = new GroupedActionListener<>(ActionListener.wrap(responses -> {
            Map<String, String> apiToResponse = responses.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            Map<String, String> LLMParams = new java.util.HashMap<>(
                Map
                    .of(
                        "phenomenon",
                        (String) knowledgeBase.get("phenomenon"),
                        "causes",
                        StringUtils.gson.toJson(causes),
                        "responses",
                        StringUtils.gson.toJson(apiToResponse)
                    )
            );
            StringSubstitutor substitute = new StringSubstitutor(LLMParams, "${parameters.", "}");
            String finalToolPrompt = substitute.replace(TOOL_PROMPT);
            LLMParams.put("prompt", finalToolPrompt);
            RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder().parameters(LLMParams).build();
            ActionRequest request = new MLPredictionTaskRequest(
                modelId,
                MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build()
            );
            client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(response -> {
                ModelTensorOutput modelTensorOutput = (ModelTensorOutput) response.getOutput();
                Map<String, ?> dataMap = Optional
                    .ofNullable(modelTensorOutput.getMlModelOutputs())
                    .flatMap(outputs -> outputs.stream().findFirst())
                    .flatMap(modelTensors -> modelTensors.getMlModelTensors().stream().findFirst())
                    .map(ModelTensor::getDataAsMap)
                    .orElse(null);
                if (dataMap == null) {
                    throw new IllegalArgumentException("No dataMap returned from LLM.");
                }
                listener.onResponse((T) dataMap.get("completion"));
            }, listener::onFailure));
        }, listener::onFailure), apiList.size());
        // TODO: support different parameters for different apis
        apiList.forEach(api -> invokeAPI(api, parameters, groupedListener));
    }

    @SuppressWarnings("unchecked")
    public <T> void runOption2(Map<String, ?> knowledgeBase, ActionListener<T> listener) {
        String phenomenon = (String) knowledgeBase.get("phenomenon");

        // API response embedded vectors
        List<Map<String, String>> causes = (List<Map<String, String>>) knowledgeBase.get("causes");
        List<String> responses = causes.stream()
            .map(cause -> cause.get("response"))
            .collect(Collectors.toList());
        List<RealVector> responseVectors = getEmbeddedVector(responses);

        // expected API response embedded vectors
        List<String> expectedResponses = causes.stream()
            .map(cause -> cause.get("expected_response"))
            .collect(Collectors.toList());
        List<RealVector> expectedResponseVectors = getEmbeddedVector(expectedResponses);

        Map<String, Double> dotProductMap = IntStream.range(0, causes.size())
            .boxed()
            .collect(Collectors.toMap(
                i -> causes.get(i).get("reason"),
                i -> responseVectors.get(i).dotProduct(expectedResponseVectors.get(i))
            ));

        Optional<Map.Entry<String, Double>> mapEntry =
            dotProductMap.entrySet().stream()
                .max(Map.Entry.comparingByValue());

        String rootCauseReason = "No root cause found";
        if (mapEntry.isPresent()) {
            Entry<String, Double> entry = mapEntry.get();
            log.info("kNN RCA reason: {} with score: {} for the phenomenon: {}",
                entry.getKey(), entry.getValue(), phenomenon);
            rootCauseReason = entry.getKey();
        } else {
            log.warn("No root cause found for the phenomenon: {}", phenomenon);
        }

        listener.onResponse((T) rootCauseReason);
    }

    /**
     *
     * @param parameters contains parameters:
     *                   1. index
     *                   2. KNOWLEDGE
     * @param listener
     * @param <T>
     */
    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        try {
            String knowledge = parameters.get(KNOWLEDGE_BASE_TOOL_OUTPUT_FIELD);
            knowledge = unescapeJson(knowledge);
            Map<String, ?> knowledgeBase = StringUtils.gson.fromJson(knowledge, Map.class);
            List<Map<String, String>> causes = (List<Map<String, String>>) knowledgeBase.get("causes");
            Map<String, String> apiToResponse = causes
                .stream()
                .map(c -> c.get(API_URL_FIELD))
                .distinct()
                .collect(Collectors.toMap(url -> url, url -> invokeAPI(url, parameters)));
            causes.forEach(cause -> cause.put("response", apiToResponse.get(cause.get(API_URL_FIELD))));

            if (isLLMOption) {
                runOption1(knowledgeBase, listener);
            } else {
                runOption2(knowledgeBase, listener);
            }
        } catch (Exception e) {
            log.error("Failed to run RCA tool", e);
            listener.onFailure(e);
        }
    }

    private void invokeAPI(String url, Map<String, String> parameters, GroupedActionListener<Pair<String, String>> groupedListener) {
        // TODO: add other API urls
        switch (url) {
            case "_cluster/allocation/explain":
                ActionListener<ClusterAllocationExplainResponse> apiListener = new ActionListener<>() {
                    @Override
                    public void onResponse(ClusterAllocationExplainResponse allocationExplainResponse) {
                        List<NodeAllocationResult> nodeDecisions = allocationExplainResponse
                            .getExplanation()
                            .getShardAllocationDecision()
                            .getAllocateDecision()
                            .getNodeDecisions();
                        StringBuilder stringBuilder = new StringBuilder();
                        for (NodeAllocationResult nodeDecision : nodeDecisions) {
                            List<Decision> decisions = nodeDecision.getCanAllocateDecision().getDecisions();
                            for (Decision decision : decisions) {
                                stringBuilder.append(decision.getExplanation());
                            }
                        }
                        groupedListener.onResponse(Pair.of("_cluster/allocation/explain", stringBuilder.toString()));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        groupedListener.onFailure(e);
                    }
                };

                // TODO: support primary false when alert yellow and true when alert true
                ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest();
                request.setIndex(parameters.get("index"));
                request.setPrimary(false);
                request.setShard(0);

                ClusterStatsUtil.getClusterAllocationExplain(client, request, apiListener);
                break;
            default:
                Exception exception = new IllegalArgumentException("API not supported");
                groupedListener.onFailure(exception);
        }
    }

    private List<RealVector> getEmbeddedVector(List<String> docs) {
        TextDocsInputDataSet inputDataSet = TextDocsInputDataSet.builder()
            .docs(docs)
            .resultFilter(ModelResultFilter.builder()
                .returnNumber(true)
                .targetResponse(List.of("sentence_embedding"))
                .build())
            .build();
        ActionRequest request = new MLPredictionTaskRequest(
            embeddingModelId,
            MLInput.builder().algorithm(FunctionName.TEXT_EMBEDDING).inputDataset(inputDataSet).build());
        ActionFuture<MLTaskResponse> mlTaskRspFuture =  client.execute(MLPredictionTaskAction.INSTANCE, request);
        ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskRspFuture.actionGet().getOutput();
        List<ModelTensor> mlModelOutputs = modelTensorOutput.getMlModelOutputs().stream()
            .map(modelTensors -> modelTensors.getMlModelTensors().get(0))
            .collect(Collectors.toList());
        return mlModelOutputs.stream()
            .map(tensor -> {
                Number[] data = tensor.getData();
                // Simplify the computation in POC, every MLResultDataType will use high precision FLOAT32, aka double in Java.
                return new ArrayRealVector(Arrays.stream(data).mapToDouble(Number::doubleValue).toArray());
            }).collect(Collectors.toList());
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return parameters != null;
    }

    public static class Factory implements Tool.Factory<RCATool> {
        private Client client;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (RCATool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public RCATool create(Map<String, Object> parameters) {
            Boolean isLLMOption  = Boolean.valueOf((String) parameters.getOrDefault(IS_LLM_OPTION, "true"));
            String modelId = (String) parameters.get(MODEL_ID);
            if (isLLMOption && Strings.isBlank(modelId)) {
                throw new IllegalArgumentException("model_id cannot be null or blank.");
            }
            String embeddingModelId = (String) parameters.get(EMBEDDING_MODEL_ID_FIELD);
            if (!isLLMOption && Strings.isBlank(embeddingModelId)) {
                throw new IllegalArgumentException("embedding_model_id cannot be null or blank.");
            }
            return new RCATool(client, modelId, embeddingModelId, isLLMOption);
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
