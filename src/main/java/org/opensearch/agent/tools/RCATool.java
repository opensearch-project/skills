/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.apache.commons.text.StringEscapeUtils.unescapeJson;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
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
    private static final String DEFAULT_DESCRIPTION = "Use this tool to perform RCA analysis";

    public RCATool(Client client, String modelId) {
        this.client = client;
        this.modelId = modelId;
    }

    public static final String TOOL_PROMPT =
        "You are going to help find the root cause of the phenomenon from the several potential causes listed below. In this RCA process, for each cause, it usually needs to call an API to get some necessary information verify whether it's the right root cause. I've filled the related response for each cause, you should decide which cause are most possible to be the root cause based on these responses. \n\n"
            + "Human: PHENOMENON\n"
            + "--------------------\n"
            + "${parameters.phenomenon} \n\n"
            + "Human: POTENTIAL CAUSES AND RESPONSE\n"
            + "--------------------\n"
            + "${parameters.causes} \n\n"
            + "Assistant: ";

    @SuppressWarnings("unchecked")
    public <T> void runOption1(Map<String, String> parameters, ActionListener<T> listener) {
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
        Map<String, String> LLMParams = new java.util.HashMap<>(
            Map.of("phenomenon", (String) knowledgeBase.get("phenomenon"), "causes", StringUtils.gson.toJson(causes))
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

    private String invokeAPI(String url, Map<String, String> parameters) {
        switch (url) {
            case "_cluster/allocation/explain":
                ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest();
                request.setIndex(parameters.get("index"));
                request.setPrimary(false);
                request.setShard(0);
                try {
                    // TODO: need to be optimized to use listener to avoid block wait
                    ClusterAllocationExplanation clusterAllocationExplanation = client
                        .admin()
                        .cluster()
                        .allocationExplain(request)
                        .get()
                        .getExplanation();
                    XContentBuilder xContentBuilder = clusterAllocationExplanation
                        .toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
                    return xContentBuilder.toString();
                } catch (Exception e) {
                    return "Meet with exception when calling API _cluster/allocation/explain";
                }
            default:
                return "API not supported";
        }
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
