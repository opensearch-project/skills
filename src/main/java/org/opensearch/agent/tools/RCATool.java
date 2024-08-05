/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.apache.commons.text.StringEscapeUtils.unescapeJson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.opensearch.agent.tools.utils.ClusterStatsUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.routing.allocation.NodeAllocationResult;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
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
    private static final String MODEL_ID = "model_id";

    private static final String KNOWLEDGE_BASE_TOOL_OUTPUT_FIELD = KnowledgeBaseTool.TYPE + ".output";
    private static final String API_URL_FIELD = "api_url";
    private static final String INDEX_FIELD = "index";
    private static final String DEFAULT_DESCRIPTION = "Use this tool to perform RCA analysis";

    public RCATool(Client client, String modelId) {
        this.client = client;
        this.modelId = modelId;
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
        Set<String> apis = causes.stream().map(c -> c.get(API_URL_FIELD)).collect(Collectors.toSet());
        ActionListener<Map<String, String>> apiListener = new ActionListener<>() {
            @Override
            public void onResponse(Map<String, String> apiToResponse) {
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
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
        // TODO: support different parameters for different apis
        Map<String, Map<String, String>> apiToParameters = apis.stream().collect(Collectors.toMap(api -> api, api -> parameters));
        invokeAPIs(apis, apiToParameters, apiListener);
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
            runOption1(parameters, listener);
        } catch (Exception e) {
            log.error("Failed to run RCA tool", e);
            listener.onFailure(e);
        }
    }

    private void invokeAPIs(Set<String> urls, Map<String, Map<String, String>> parameters, ActionListener<Map<String, String>> listener) {
        Map<String, CompletableFuture<String>> apiFutures = new HashMap<>();
        for (String url : urls) {
            Map<String, String> parameter = parameters.get(url);
            CompletableFuture<String> apiFuture = new CompletableFuture<>();
            apiFutures.put(url, apiFuture);

            ActionListener<String> apiListener = new ActionListener<>() {
                @Override
                public void onResponse(String response) {
                    apiFuture.complete(response);
                }

                @Override
                public void onFailure(Exception e) {
                    apiFuture.completeExceptionally(e);
                    listener.onFailure(e);
                }
            };

            invokeAPI(url, parameter, apiListener);
        }

        try {
            CompletableFuture<Map<String, String>> mapFuture = CompletableFuture
                .allOf(apiFutures.values().toArray(new CompletableFuture<?>[0]))
                .thenApply(
                    v -> apiFutures.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().join()))
                );
            Map<String, String> apiToResponse = mapFuture.join();
            listener.onResponse(apiToResponse);
        } catch (Exception e) {
            log.error("Failed to get all api results from rca tool", e);
            listener.onFailure(e);
        }
    }

    private void invokeAPI(String url, Map<String, String> parameters, ActionListener<String> listener) {
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
                        listener.onResponse(stringBuilder.toString());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
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
                listener.onFailure(exception);
        }
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
            String modelId = (String) parameters.get(MODEL_ID);
            if (Strings.isBlank(modelId)) {
                throw new IllegalArgumentException("model_id cannot be null or blank.");
            }
            return new RCATool(client, modelId);
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
