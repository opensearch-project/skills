/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;
import static org.opensearch.ml.common.utils.StringUtils.isJson;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.text.StringSubstitutor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.agent.tools.utils.ToolConstants.ModelType;
import org.opensearch.agent.tools.utils.ToolHelper;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.logging.LoggerMessageFormat;
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

import com.google.gson.reflect.TypeToken;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@ToolAnnotation(CreateAlertTool.TYPE)
public class CreateAlertTool implements Tool {
    public static final String TYPE = "CreateAlertTool";

    private static final String DEFAULT_DESCRIPTION =
        "This is a tool that helps to create an alert(i.e. monitor with triggers), some parameters should be parsed based on user's question and context. The parameters should include: \n"
            + "1. indices: The input indices of the monitor, should be a list of string in json format.\n";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;

    private final Client client;
    @Getter
    private final String modelId;
    @Getter
    private final String modelType;
    @Getter
    private final String toolPrompt;

    private static final String MODEL_ID = "model_id";
    private static final String PROMPT_FILE_PATH = "CreateAlertDefaultPrompt.json";
    private static final String DEFAULT_QUESTION = "Create an alert as your recommendation based on the context";
    private static final Map<String, String> promptDict = ToolHelper.loadDefaultPromptDictFromFile(CreateAlertTool.class, PROMPT_FILE_PATH);

    public enum ModelType {
        CLAUDE,
        OPENAI;

        public static ModelType from(String value) {
            if (value.isEmpty()) {
                return ModelType.CLAUDE;
            }
            try {
                return ModelType.valueOf(value.toUpperCase(Locale.ROOT));
            } catch (Exception e) {
                log.error("Wrong Model type, should be CLAUDE or OPENAI");
                return ModelType.CLAUDE;
            }
        }
    }

    public CreateAlertTool(Client client, String modelId, String modelType, String prompt) {
        this.client = client;
        this.modelId = modelId;
        this.modelType = String.valueOf(ModelType.from(modelType));
        if (prompt.isEmpty()) {
            if (!promptDict.containsKey(this.modelType)) {
                throw new IllegalArgumentException(
                    LoggerMessageFormat
                        .format(
                            null,
                            "Failed to find the right prompt for modelType: {}, this tool supports prompts for these models: [{}]",
                            modelType,
                            String.join(",", promptDict.keySet())
                        )
                );
            }
            this.toolPrompt = promptDict.get(this.modelType);
        } else {
            this.toolPrompt = prompt;
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
        return true;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        Map<String, String> tmpParams = new HashMap<>(parameters);
        if (!tmpParams.containsKey("indices") || Strings.isEmpty(tmpParams.get("indices"))) {
            throw new IllegalArgumentException(
                "No indices in the input parameter. Ask user to "
                    + "provide index as your final answer directly without using any other tools"
            );
        }
        String rawIndex = tmpParams.getOrDefault("indices", "");
        Boolean isLocal = Boolean.parseBoolean(tmpParams.getOrDefault("local", "true"));
        final GetIndexRequest getIndexRequest = constructIndexRequest(rawIndex, isLocal);
        client.admin().indices().getIndex(getIndexRequest, ActionListener.wrap(response -> {
            if (response.indices().length == 0) {
                throw new IllegalArgumentException(
                    LoggerMessageFormat
                        .format(
                            null,
                            "Cannot find provided indices {}. Ask "
                                + "user to check the provided indices as your final answer without using any other "
                                + "tools",
                            rawIndex
                        )
                );
            }
            StringBuilder sb = new StringBuilder();
            for (String index : response.indices()) {
                sb.append("index: ").append(index).append("\n\n");

                MappingMetadata mapping = response.mappings().get(index);
                if (mapping != null) {
                    sb.append("mappings:\n");
                    for (Entry<String, Object> entry : mapping.sourceAsMap().entrySet()) {
                        sb.append(entry.getKey()).append("=").append(entry.getValue()).append('\n');
                    }
                    sb.append("\n\n");
                }
            }
            String mappingInfo = sb.toString();
            ActionRequest request = constructMLPredictRequest(tmpParams, mappingInfo);
            client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(r -> {
                ModelTensorOutput modelTensorOutput = (ModelTensorOutput) r.getOutput();
                Map<String, ?> dataMap = Optional
                    .ofNullable(modelTensorOutput.getMlModelOutputs())
                    .flatMap(outputs -> outputs.stream().findFirst())
                    .flatMap(modelTensors -> modelTensors.getMlModelTensors().stream().findFirst())
                    .map(ModelTensor::getDataAsMap)
                    .orElse(null);
                if (dataMap == null) {
                    throw new IllegalArgumentException("No dataMap returned from LLM.");
                }
                String alertInfo = "";
                if (dataMap.containsKey("response")) {
                    alertInfo = (String) dataMap.get("response");
                    Pattern jsonPattern = Pattern.compile("```json(.*?)```", Pattern.DOTALL);
                    Matcher jsonBlockMatcher = jsonPattern.matcher(alertInfo);
                    if (jsonBlockMatcher.find()) {
                        alertInfo = jsonBlockMatcher.group(1);
                        alertInfo = alertInfo.replace("\\\"", "\"");
                    }
                } else {
                    // LLM sometimes returns the tensor results as a json object directly instead of
                    // string response, and the json object is stored as a map.
                    alertInfo = StringUtils.toJson(dataMap);
                }
                if (!isJson(alertInfo)) {
                    throw new IllegalArgumentException(
                        LoggerMessageFormat.format(null, "The response from LLM is not a json: [{}]", alertInfo)
                    );
                }
                listener.onResponse((T) alertInfo);
            }, e -> {
                log.error("Failed to run model " + modelId, e);
                listener.onFailure(e);
            }));
        }, e -> {
            log.error("failed to get index mapping: " + e);
            if (e.toString().contains("IndexNotFoundException")) {
                listener
                    .onFailure(
                        new IllegalArgumentException(
                            LoggerMessageFormat
                                .format(
                                    null,
                                    "Cannot find provided indices {}. Ask "
                                        + "user to check the provided indices as your final answer without using any other "
                                        + "tools",
                                    rawIndex
                                )
                        )
                    );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private ActionRequest constructMLPredictRequest(Map<String, String> tmpParams, String mappingInfo) {
        tmpParams.put("mapping_info", mappingInfo);
        tmpParams.putIfAbsent("indices", "");
        tmpParams.putIfAbsent("chat_history", "");
        tmpParams.putIfAbsent("question", DEFAULT_QUESTION); // In case no question is provided, use a default question.
        StringSubstitutor substitute = new StringSubstitutor(tmpParams, "${parameters.", "}");
        String finalToolPrompt = substitute.replace(toolPrompt);
        tmpParams.put("prompt", finalToolPrompt);

        RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder().parameters(tmpParams).build();
        ActionRequest request = new MLPredictionTaskRequest(
            modelId,
            MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build()
        );
        return request;
    }

    private static GetIndexRequest constructIndexRequest(String rawIndex, Boolean isLocal) {
        List<String> indexList;
        try {
            indexList = StringUtils.gson.fromJson(rawIndex, new TypeToken<List<String>>() {
            }.getType());
        } catch (Exception e) {
            // LLM sometimes returns the indices as a string but not json format, although we require that in the tool description.
            indexList = Arrays.asList(rawIndex.split("\\."));
        }
        if (indexList.isEmpty()) {
            throw new IllegalArgumentException(
                "The input indices is empty. Ask user to " + "provide index as your final answer directly without using any other tools"
            );
        } else if (indexList.stream().anyMatch(index -> index.startsWith("."))) {
            throw new IllegalArgumentException(
                LoggerMessageFormat
                    .format(
                        null,
                        "The provided indices [{}] contains system index, which is not allowed. Ask user to "
                            + "check the provided indices as your final answer without using any other.",
                        rawIndex
                    )
            );
        }
        final String[] indices = indexList.toArray(Strings.EMPTY_ARRAY);
        final GetIndexRequest getIndexRequest = new GetIndexRequest()
            .indices(indices)
            .indicesOptions(IndicesOptions.strictExpand())
            .local(isLocal)
            .clusterManagerNodeTimeout(DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT);
        return getIndexRequest;
    }

    public static class Factory implements Tool.Factory<CreateAlertTool> {

        private Client client;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (CreateAlertTool.class) {
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
        public CreateAlertTool create(Map<String, Object> params) {
            String modelId = (String) params.get(MODEL_ID);
            if (org.apache.commons.lang3.StringUtils.isBlank(modelId)) {
                throw new IllegalArgumentException("model_id cannot be null or blank.");
            }
            String modelType = (String) params.getOrDefault("model_type", ModelType.CLAUDE.toString());
            String prompt = (String) params.getOrDefault("prompt", "");
            return new CreateAlertTool(client, modelId, modelType, prompt);
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
