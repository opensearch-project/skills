/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.tools.utils.CommonConstants.COMMON_MODEL_ID_FIELD;
import static org.opensearch.ml.common.CommonValue.TENANT_ID_FIELD;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.agent.tools.utils.ToolHelper;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.spi.tools.WithModelTool;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableMap;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * A tool used to help creating anomaly detector, the only one input parameter is the index name, this tool will get the mappings of the index
 * in flight and let LLM give the suggested category field, aggregation field and correspond aggregation method which are required for the create
 * anomaly detector API, the output of this tool is like:
 *{
 *     "index": "opensearch_dashboards_sample_data_ecommerce",
 *     "categoryField": "geoip.country_iso_code",
 *     "aggregationField": "total_quantity,total_unique_products,taxful_total_price",
 *     "aggregationMethod": "sum,count,sum",
 *     "dateFields": "customer_birth_date,order_date,products.created_on"
 * }
 */
@Log4j2
@Setter
@Getter
@ToolAnnotation(CreateAnomalyDetectorTool.TYPE)
public class CreateAnomalyDetectorTool implements WithModelTool {
    // the type of this tool
    public static final String TYPE = "CreateAnomalyDetectorTool";

    // the default description of this tool
    private static final String DEFAULT_DESCRIPTION =
        "This is a tool used to help creating anomaly detector. It takes a required argument which is the name of the index, extract the index mappings and let the LLM to give the suggested aggregation field, aggregation method, category field and the date field which are required to create an anomaly detector.";
    // the regex used to extract the key information from the response of LLM
    private static final String EXTRACT_INFORMATION_REGEX =
        "(?s).*\\{category_field=([^|]*)\\|aggregation_field=([^|]*)\\|aggregation_method=([^}]*)}.*";
    // valid field types which support aggregation
    private static final Set<String> VALID_FIELD_TYPES = Set
        .of(
            "keyword",
            "constant_keyword",
            "wildcard",
            "long",
            "integer",
            "short",
            "byte",
            "double",
            "float",
            "half_float",
            "scaled_float",
            "unsigned_long",
            "ip"
        );
    // the index name key in the output
    private static final String OUTPUT_KEY_INDEX = "index";
    // the category field key in the output
    private static final String OUTPUT_KEY_CATEGORY_FIELD = "categoryField";
    // the aggregation field key in the output
    private static final String OUTPUT_KEY_AGGREGATION_FIELD = "aggregationField";
    // the aggregation method name key in the output
    private static final String OUTPUT_KEY_AGGREGATION_METHOD = "aggregationMethod";
    // the date fields key in the output
    private static final String OUTPUT_KEY_DATE_FIELDS = "dateFields";
    // the default prompt dictionary, includes claude and openai
    private static final Map<String, String> DEFAULT_PROMPT_DICT = loadDefaultPromptFromFile();
    // the name of this tool
    @Setter
    @Getter
    private String name = TYPE;
    // the description of this tool
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;

    // the version of this tool
    @Getter
    private String version;

    // the OpenSearch transport client
    private Client client;
    // the mode id of LLM
    @Getter
    private String modelId;
    // LLM model type, CLAUDE or OPENAI
    @Getter
    private ModelType modelType;
    // the default prompt for creating anomaly detector
    private String contextPrompt;
    private Map<String, Object> attributes;

    enum ModelType {
        CLAUDE,
        OPENAI;

        public static ModelType from(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

    }

    /**
     *
     * @param client the OpenSearch transport client
     * @param modelId the model ID of LLM
     */
    public CreateAnomalyDetectorTool(Client client, String modelId, String modelType, String contextPrompt) {
        this.client = client;
        this.modelId = modelId;
        if (!ModelType.OPENAI.toString().equalsIgnoreCase(modelType) && !ModelType.CLAUDE.toString().equalsIgnoreCase(modelType)) {
            throw new IllegalArgumentException("Unsupported model_type: " + modelType);
        }
        this.modelType = ModelType.from(modelType);
        if (contextPrompt.isEmpty()) {
            this.contextPrompt = DEFAULT_PROMPT_DICT.getOrDefault(this.modelType.toString(), "");
        } else {
            this.contextPrompt = contextPrompt;
        }
    }

    /**
     * The main running method of this tool
     * @param parameters the input parameters
     * @param listener the action listener
     *
     */
    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        final String tenantId = parameters.get(TENANT_ID_FIELD);
        Map<String, String> enrichedParameters = enrichParameters(parameters);
        String indexName = enrichedParameters.get("index");
        if (Strings.isNullOrEmpty(indexName)) {
            throw new IllegalArgumentException(
                "Return this final answer to human directly and do not use other tools: 'Please provide index name'. Please try to directly send this message to human to ask for index name"
            );
        }
        if (indexName.startsWith(".")) {
            throw new IllegalArgumentException(
                "CreateAnomalyDetectionTool doesn't support searching indices starting with '.' since it could be system index, current searching index name: "
                    + indexName
            );
        }

        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(indexName);
        client.admin().indices().getMappings(getMappingsRequest, ActionListener.wrap(response -> {
            Map<String, MappingMetadata> mappings = response.getMappings();
            if (mappings.size() == 0) {
                throw new IllegalArgumentException("No mapping found for the index: " + indexName);
            }

            // when the index name is a wildcard pattern, a data stream, or an alias, we fetch the mappings of the first index
            String firstIndexName = (String) mappings.keySet().toArray()[0];
            MappingMetadata mappingMetadata = mappings.get(firstIndexName);

            Map<String, Object> mappingSource = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
            if (Objects.isNull(mappingSource)) {
                throw new IllegalArgumentException(
                    "The index " + indexName + " doesn't have mapping metadata, please add data to it or using another index."
                );
            }

            // flatten all the fields in the mapping
            Map<String, String> fieldsToType = new HashMap<>();
            ToolHelper.extractFieldNamesTypes(mappingSource, fieldsToType, "", true);

            // find all date type fields from the mapping
            final Set<String> dateFields = findDateTypeFields(fieldsToType);
            if (dateFields.isEmpty()) {
                throw new IllegalArgumentException(
                    "The index " + indexName + " doesn't have date type fields, cannot create an anomaly detector for it."
                );
            }
            StringJoiner dateFieldsJoiner = new StringJoiner(",");
            dateFields.forEach(dateFieldsJoiner::add);

            // filter the mapping to improve the accuracy of the result
            // only fields support aggregation can be existed in the mapping and sent to LLM
            Map<String, String> filteredMapping = fieldsToType
                .entrySet()
                .stream()
                .filter(entry -> VALID_FIELD_TYPES.contains(entry.getValue()))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            // construct the prompt
            String prompt = constructPrompt(filteredMapping, firstIndexName);
            RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet
                .builder()
                .parameters(Collections.singletonMap("prompt", prompt))
                .build();
            ActionRequest request = new MLPredictionTaskRequest(
                modelId,
                MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build(),
                null,
                tenantId
            );

            client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(mlTaskResponse -> {
                ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
                ModelTensors modelTensors = modelTensorOutput.getMlModelOutputs().get(0);
                ModelTensor modelTensor = modelTensors.getMlModelTensors().get(0);
                Map<String, String> dataAsMap = (Map<String, String>) modelTensor.getDataAsMap();
                if (dataAsMap == null) {
                    listener.onFailure(new IllegalStateException("Remote endpoint fails to inference."));
                    return;
                }
                String finalResponse = dataAsMap.get("response");
                if (Strings.isNullOrEmpty(finalResponse)) {
                    listener.onFailure(new IllegalStateException("Remote endpoint fails to inference, no response found."));
                    return;
                }

                // use regex pattern to extract the suggested parameters for the create anomaly detector API
                Pattern pattern = Pattern.compile(EXTRACT_INFORMATION_REGEX);
                Matcher matcher = pattern.matcher(finalResponse);
                if (!matcher.matches()) {
                    log
                        .error(
                            "The inference result from remote endpoint is not valid because the result: ["
                                + finalResponse
                                + "] cannot match the regex: "
                                + EXTRACT_INFORMATION_REGEX
                        );
                    listener
                        .onFailure(
                            new IllegalStateException(
                                "The inference result from remote endpoint is not valid, cannot extract the key information from the result."
                            )
                        );
                    return;
                }

                // remove double quotes or whitespace if exists
                String categoryField = matcher.group(1).replaceAll("\"", "").strip();
                String aggregationField = matcher.group(2).replaceAll("\"", "").strip();
                String aggregationMethod = matcher.group(3).replaceAll("\"", "").strip();

                Map<String, String> result = ImmutableMap
                    .of(
                        OUTPUT_KEY_INDEX,
                        firstIndexName,
                        OUTPUT_KEY_CATEGORY_FIELD,
                        categoryField,
                        OUTPUT_KEY_AGGREGATION_FIELD,
                        aggregationField,
                        OUTPUT_KEY_AGGREGATION_METHOD,
                        aggregationMethod,
                        OUTPUT_KEY_DATE_FIELDS,
                        dateFieldsJoiner.toString()
                    );
                listener.onResponse((T) AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(result)));
            }, e -> {
                log.error("fail to predict model: " + e);
                listener.onFailure(e);
            }));
        }, e -> {
            log.error("failed to get mapping: " + e);
            if (e.toString().contains("IndexNotFoundException")) {
                listener
                    .onFailure(
                        new IllegalArgumentException(
                            "Return this final answer to human directly and do not use other tools: 'The index doesn't exist, please provide another index and retry'. Please try to directly send this message to human to ask for index name"
                        )
                    );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Enrich the parameters by adding the parameters extracted from the chat
     * @param parameters the original parameters
     * @return the enriched parameters with parameters extracting from the chat
     */
    private Map<String, String> enrichParameters(Map<String, String> parameters) {
        Map<String, String> result = new HashMap<>(parameters);
        try {
            // input is a map
            Map<String, String> chatParameters = gson.fromJson(parameters.get("input"), Map.class);
            result.putAll(chatParameters);
        } catch (Exception e) {
            // input is a string
            String indexName = parameters.getOrDefault("input", "");
            if (!indexName.isEmpty()) {
                result.put("index", indexName);
            }
        }
        return result;
    }

    /**
     *
     * @param fieldsToType the flattened field-> field type mapping
     * @return a list containing all the date type fields
     */
    private Set<String> findDateTypeFields(final Map<String, String> fieldsToType) {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, String> entry : fieldsToType.entrySet()) {
            String value = entry.getValue();
            if (value.equals("date") || value.equals("date_nanos")) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> loadDefaultPromptFromFile() {
        try (InputStream inputStream = CreateAnomalyDetectorTool.class.getResourceAsStream("CreateAnomalyDetectorDefaultPrompt.json")) {
            if (inputStream != null) {
                return gson.fromJson(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8), Map.class);
            }
        } catch (IOException e) {
            log.error("Failed to load prompt from the file CreateAnomalyDetectorDefaultPrompt.json, error: ", e);
        }
        return new HashMap<>();
    }

    /**
     *
     * @param fieldsToType the flattened field-> field type mapping
     * @param indexName the index name
     * @return the prompt about creating anomaly detector
     */
    private String constructPrompt(final Map<String, String> fieldsToType, final String indexName) {
        StringJoiner tableInfoJoiner = new StringJoiner("\n");
        for (Map.Entry<String, String> entry : fieldsToType.entrySet()) {
            tableInfoJoiner.add("- " + entry.getKey() + ": " + entry.getValue());
        }

        Map<String, String> indexInfo = ImmutableMap.of("indexName", indexName, "indexMapping", tableInfoJoiner.toString());
        StringSubstitutor substitutor = new StringSubstitutor(indexInfo, "${indexInfo.", "}");
        return substitutor.replace(contextPrompt);
    }

    /**
     *
     * @param parameters the input parameters
     * @return false if the input parameters is null or empty
     */
    @Override
    public boolean validate(Map<String, String> parameters) {
        return parameters != null && parameters.size() != 0;
    }

    /**
     *
     * @return the type of this tool
     */
    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * The tool factory
     */
    public static class Factory implements WithModelTool.Factory<CreateAnomalyDetectorTool> {
        private Client client;

        private static CreateAnomalyDetectorTool.Factory INSTANCE;

        /**
         * Create or return the singleton factory instance
         */
        public static CreateAnomalyDetectorTool.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (CreateAnomalyDetectorTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new CreateAnomalyDetectorTool.Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        /**
         *
         * @param map the input parameters
         * @return the instance of this tool
         */
        @Override
        public CreateAnomalyDetectorTool create(Map<String, Object> map) {
            String modelId = (String) map.getOrDefault(COMMON_MODEL_ID_FIELD, "");
            if (modelId.isEmpty()) {
                throw new IllegalArgumentException("model_id cannot be empty.");
            }
            String modelType = (String) map.getOrDefault("model_type", ModelType.CLAUDE.toString());
            // if model type is empty, use the default value
            if (modelType.isEmpty()) {
                modelType = ModelType.CLAUDE.toString();
            } else if (!ModelType.OPENAI.toString().equalsIgnoreCase(modelType)
                && !ModelType.CLAUDE.toString().equalsIgnoreCase(modelType)) {
                throw new IllegalArgumentException("Unsupported model_type: " + modelType);
            }
            String prompt = (String) map.getOrDefault("prompt", "");
            return new CreateAnomalyDetectorTool(client, modelId, modelType, prompt);
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

        @Override
        public List<String> getAllModelKeys() {
            return List.of(COMMON_MODEL_ID_FIELD);
        }
    }
}
