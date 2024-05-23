/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;
import static org.opensearch.ml.common.utils.StringUtils.isJson;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.MappingMetadata;
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

@Log4j2
@ToolAnnotation(CreateAlertTool.TYPE)
public class CreateAlertTool implements Tool {
    public static final String TYPE = "CreateAlertTool";

    private static final String DEFAULT_DESCRIPTION =
       "This is a tool that helps to create an alert(i.e. monitor with triggers), some parameters should be parsed based on user's question and context. The parameters should include: \n"
           +"1. question: user's question about creating a new alert\n"
           +"2. indices: The input indices of the monitor, should be a list of string in json format.\n";

    private static final String TOOL_PROMPT_TEMPLATE =
        "Generate the monitor definition for an alert as the following json format without any other information.\n"
            + "The value of `${field_name}` should be a property in the mapping info and `${field_type} should be its type`\n"
            + "You could recommend some recommended fields or values for the definition if user does not provide them.\n"
            + "No need explanation in the response, it's very important that don't miss ```json.\n\n"
            + "Human:RESPONSE FORMAT INSTRUCTIONS\n" + "----------------------------\n"
            + "```json\n"
            + "{\n"
            + "    \"name\": \"\" //monitor name\n"
            + "    \"search\": {\n"
            + "        \"indices\": ${parameters.indices} //no need to change.\n"
            + "        \"timeField\": \"${field_name}\",\n"
            + "        \"bucketValue\": 1, //A numeric value defining the time range for the last, default is 1.\n"
            + "        \"bucketUnitOfTime\": \"m\",  //The time unit for the bucketValue, options include 'm' (minutes), 'h' (hours), or 'd' (days), with a default of 'h'\n"
            + "        \"filters\": [ // A list of filters to filter logs to meet user's alert definition.\n"
            + "            {\n"
            + "                \"fieldName\": [ //The length should be 1.\n"
            + "                    {\n"
            + "                      \"label\": \"${field_name}\",\n"
            + "                      \"type\": \"${field_type}\" //options are 'number', 'text', 'keyword', 'boolean'\n"
            + "                    }\n"
            + "                ],\n"
            + "                \"fieldValue\": 10,\n"
            + "                \"operator\": \"is\" //options are 'is', 'is_not', 'contains', 'does_not_contains', 'starts_with', 'ends_with', 'is_greater', 'is_greater_equal, 'is_less', 'is_less_equal.\n"
            + "            },\n"
            + "        ],\n"
            + "        \"aggregations\": [\n"
            + "            {\n"
            + "                \"aggregationType\": \"count\", //options are ‘count’, ‘max’, ‘min’, ‘avg’, or ‘sum’, with a default of ‘count’.\n"
            + "                \"fieldName\": \"${field_name}\"\n"
            + "            }\n"
            + "        ]\n"
            + "    },\n"
            + "    \"triggers\": [{  // The triggers for the alert, it triggers when the above aggregation result satisfies the threshold.\n"
            + "        \"name\": 'Trigger' //The name of the trigger. You could generate this name based on this trigger definition.\n"
            + "        \"severity\": 1, //The severity level of the trigger, options are 1, 2 and 3.\n"
            + "        \"thresholdValue\": 0, //A numeric value defining the threshold.\n"
            + "        \"thresholdEnum\": \"ABOVE\" //options are 'ABOVE', 'BELOW', or 'EXACTLY'.\n"
            + "    }],\n"
            + "}\n"
            + "```\n"
            + "Human: Examples\n" + "--------------------\n"
            + "question: create alert if the count of non 200 response happens over 30 times per hour.\n"
            + "response: {\\n\"name\": \"Error Response Alert\",\\n\"search\": {\\n\"indices\": [\"opensearch_dashboards_sample_data_logs\"],\\n\"timeField\": \"timestamp\",\\n\"bucketValue\": 60,\\n\"bucketUnitOfTime\": \"m\",\\n\"filters\": [\\n{\\n\"fieldName\": [\\n{\\n\"label\": \"response\",\\n\"type\": \"text\"\\n}\\n],\\n\"fieldValue\": \"200\",\\n\"operator\": \"is_not\"\\n}\\n],\\n\"aggregations\": [\\n{\\n\"aggregationType\": \"count\",\\n\"fieldName\": \"bytes\"\\n}\\n]\\n},\\n\"triggers\": [{\\n\"name\": \"Error Response Count Above 30\", \\n\"value\": 30,\\n\"enum\": \"ABOVE\"\\n}]\\n}\n"
            + "Human:USER'S CONTEXT\n" + "--------------------\n"
            + "mapping_info of the target index: ${parameters.mapping_info}\n"
            + "Human:CHAT HISTORY\n" + "--------------------\n"
            + "${parameters.chat_history}\n"
            + "Human:USER'S INPUT\n" + "--------------------\n"
            + "Here is the user's input :\n"
            + "${parameters.question}";


    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;

    private final Client client;
    private final String modelId;

    private static final Gson gson = new Gson();
    private static final String MODEL_ID = "model_id";

    public CreateAlertTool(Client client, String modelId) {
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
        return true;
    }

    @Override
    public boolean needHistory() {
        return true;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        try {
            Map<String, String> tmpParams = new HashMap<>(parameters);
            String mappingInfo = getIndexMappingInfo(tmpParams);
            tmpParams.put("mapping_info", mappingInfo);
            tmpParams.putIfAbsent("indices", "");
            tmpParams.putIfAbsent("chat_history", "");
            tmpParams.putIfAbsent("question", "");
            StringSubstitutor substitute = new StringSubstitutor(tmpParams, "${parameters.", "}");
            String finalToolPrompt = substitute.replace(TOOL_PROMPT_TEMPLATE);
            tmpParams.put("prompt", finalToolPrompt);

            RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder().parameters(tmpParams).build();
            ActionRequest request = new MLPredictionTaskRequest(
                modelId,
                MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build()
            );
            client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(r -> {
                ModelTensorOutput modelTensorOutput = (ModelTensorOutput) r.getOutput();
                Map<String, ?> dataMap = Optional.ofNullable(modelTensorOutput.getMlModelOutputs())
                    .flatMap(outputs -> outputs.stream().findFirst())
                    .flatMap(modelTensors -> modelTensors.getMlModelTensors().stream().findFirst())
                    .map(ModelTensor::getDataAsMap).orElse(null);
                if (dataMap == null) {
                    throw new IllegalArgumentException("No dataMap returned from LLM.");
                }
                String response = "";
                if (dataMap.containsKey("response")) {
                    response = (String) dataMap.get("response");
                    Pattern jsonPattern = Pattern.compile("```json(.*?)```", Pattern.DOTALL);
                    Matcher jsonBlockMatcher = jsonPattern.matcher(response);
                    if (jsonBlockMatcher.find()) {
                        response = jsonBlockMatcher.group(1);
                        response = response.replace("\\\"", "\"");
                    }
                } else {
                    // LLM sometimes returns the tensor results as a json object directly instead of
                    // string response, and the json object is stored as a map.
                    response = StringUtils.toJson(dataMap);
                }
                if (!isJson(response)) {
                    throw new IllegalArgumentException(String.format("The response from LLM is not a json: [%s]", response));
                }
                listener.onResponse((T) response);
            }, e -> {
                log.error("Failed to run model " + modelId, e);
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            log.error("Failed to call CreateAlertTool", e);
            listener.onFailure(e);
        }
    }

    private String getIndexMappingInfo(Map<String, String> parameters)
        throws InterruptedException, ExecutionException {
        if (!parameters.containsKey("indices") || parameters.get("indices").isEmpty()) {
            throw new IllegalArgumentException("No indices in the input parameter. Ask user to "
                + "provide index as your final answer directly without using any other tools");
        }
        String rawIndex = parameters.getOrDefault("indices", "");
        List<String> indexList = gson.fromJson(rawIndex, new TypeToken<List<String>>(){}.getType());
        final String[] indices = indexList.toArray(Strings.EMPTY_ARRAY);
        final GetIndexRequest getIndexRequest = new GetIndexRequest()
            .indices(indices)
            .indicesOptions(IndicesOptions.strictExpand())
            .local(Boolean.parseBoolean(parameters.getOrDefault("local", "true")))
            .clusterManagerNodeTimeout(DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT);
        GetIndexResponse getIndexResponse = client.admin().indices().getIndex(getIndexRequest).get();
        if (getIndexResponse.indices().length == 0) {
            throw new IllegalArgumentException(String.format("Cannot find provided indices %s. Ask "
                + "user to check the provided indices as your final answer without using any other "
                + "tools", rawIndex));
        }
        StringBuilder sb = new StringBuilder();
        for (String index : getIndexResponse.indices()) {
            sb.append("index: ").append(index).append("\n\n");

            MappingMetadata mapping = getIndexResponse.mappings().get(index);
            if (mapping != null) {
                sb.append("mappings:\n");
                for (Entry<String, Object> entry : mapping.sourceAsMap().entrySet()) {
                    sb.append(entry.getKey()).append("=").append(entry.getValue()).append('\n');
                }
                sb.append("\n\n");
            }
        }
        return sb.toString();
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
            if (Strings.isBlank(modelId)) {
                throw new IllegalArgumentException("model_id cannot be null or blank.");
            }
            return new CreateAlertTool(client, modelId);
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
