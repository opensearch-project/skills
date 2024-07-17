/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.text.StringSubstitutor;
import org.json.JSONObject;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.agent.common.SkillSettings;
import org.opensearch.agent.tools.utils.ClusterSettingHelper;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableMap;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

import com.google.gson.Gson;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
@ToolAnnotation(PPLTool.TYPE)
public class PPLTool implements Tool {

    public static final String TYPE = "PPLTool";

    @Setter
    private Client client;

    private static final String DEFAULT_DESCRIPTION =
        "\"Use this tool when user ask question based on the data in the cluster or parse user statement about which index to use in a conversion.\nAlso use this tool when question only contains index information.\n1. If uesr question contain both question and index name, the input parameters are {'question': UserQuestion, 'index': IndexName}.\n2. If user question contain only question, the input parameter is {'question': UserQuestion}.\n3. If uesr question contain only index name, find the original human input from the conversation histroy and formulate parameter as {'question': UserQuestion, 'index': IndexName}\nThe index name should be exactly as stated in user's input.";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;

    private String modelId;

    private String contextPrompt;

    private Boolean execute;

    private PPLModelType pplModelType;

    private String previousToolKey;

    private int head;

    private ClusterSettingHelper clusterSettingHelper;

    private static Gson gson = new Gson();

    private static Map<String, String> DEFAULT_PROMPT_DICT;

    private static Set<String> ALLOWED_FIELDS_TYPE;

    static {
        ALLOWED_FIELDS_TYPE = new HashSet<>(); // from
                                               // https://github.com/opensearch-project/sql/blob/2.x/docs/user/ppl/general/datatypes.rst#data-types-mapping
                                               // and https://opensearch.org/docs/latest/field-types/supported-field-types/index/
        ALLOWED_FIELDS_TYPE.add("boolean");
        ALLOWED_FIELDS_TYPE.add("byte");
        ALLOWED_FIELDS_TYPE.add("short");
        ALLOWED_FIELDS_TYPE.add("integer");
        ALLOWED_FIELDS_TYPE.add("long");
        ALLOWED_FIELDS_TYPE.add("float");
        ALLOWED_FIELDS_TYPE.add("half_float");
        ALLOWED_FIELDS_TYPE.add("scaled_float");
        ALLOWED_FIELDS_TYPE.add("double");
        ALLOWED_FIELDS_TYPE.add("keyword");
        ALLOWED_FIELDS_TYPE.add("text");
        ALLOWED_FIELDS_TYPE.add("date");
        ALLOWED_FIELDS_TYPE.add("date_nanos");
        ALLOWED_FIELDS_TYPE.add("ip");
        ALLOWED_FIELDS_TYPE.add("binary");
        ALLOWED_FIELDS_TYPE.add("object");
        ALLOWED_FIELDS_TYPE.add("nested");
        ALLOWED_FIELDS_TYPE.add("geo_point");

        DEFAULT_PROMPT_DICT = loadDefaultPromptDict();
    }

    public enum PPLModelType {
        CLAUDE,
        FINETUNE,
        OPENAI;

        public static PPLModelType from(String value) {
            if (value.isEmpty()) {
                return PPLModelType.CLAUDE;
            }
            try {
                return PPLModelType.valueOf(value.toUpperCase(Locale.ROOT));
            } catch (Exception e) {
                log.error("Wrong PPL Model type, should be CLAUDE, FINETUNE, or OPENAI");
                return PPLModelType.CLAUDE;
            }
        }

    }

    public PPLTool(
        Client client,
        ClusterSettingHelper clusterSettingHelper,
        String modelId,
        String contextPrompt,
        String pplModelType,
        String previousToolKey,
        int head,
        boolean execute
    ) {
        this.client = client;
        this.modelId = modelId;
        this.pplModelType = PPLModelType.from(pplModelType);
        if (contextPrompt.isEmpty()) {
            this.contextPrompt = DEFAULT_PROMPT_DICT.getOrDefault(this.pplModelType.toString(), "");
        } else {
            this.contextPrompt = contextPrompt;
        }
        this.previousToolKey = previousToolKey;
        this.head = head;
        this.execute = execute;
        this.clusterSettingHelper = clusterSettingHelper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        extractFromChatParameters(parameters);
        String indexName = getIndexNameFromParameters(parameters);
        if (StringUtils.isBlank(indexName)) {
            throw new IllegalArgumentException(
                "Return this final answer to human directly and do not use other tools: 'Please provide index name'. Please try to directly send this message to human to ask for index name"
            );
        }
        String question = parameters.get("question");
        if (StringUtils.isBlank(indexName) || StringUtils.isBlank(question)) {
            throw new IllegalArgumentException("Parameter index and question can not be null or empty.");
        }
        if (indexName.startsWith(".")) {
            throw new IllegalArgumentException(
                "PPLTool doesn't support searching indices starting with '.' since it could be system index, current searching index name: "
                    + indexName
            );
        }

        GetMappingsRequest getMappingsRequest = buildGetMappingRequest(indexName);
        client.admin().indices().getMappings(getMappingsRequest, ActionListener.wrap(getMappingsResponse -> {
            Map<String, MappingMetadata> mappings = getMappingsResponse.getMappings();
            if (mappings.isEmpty()) {
                throw new IllegalArgumentException("No matching mapping with index name: " + indexName);
            }
            String firstIndexName = (String) mappings.keySet().toArray()[0];
            SearchRequest searchRequest = buildSearchRequest(firstIndexName);
            client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                String tableInfo = constructTableInfo(searchHits, mappings);
                String prompt = constructPrompt(tableInfo, question.strip(), indexName);
                RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet
                    .builder()
                    .parameters(Collections.singletonMap("prompt", prompt))
                    .build();
                ActionRequest request = new MLPredictionTaskRequest(
                    modelId,
                    MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build()
                );
                client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(mlTaskResponse -> {
                    ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
                    ModelTensors modelTensors = modelTensorOutput.getMlModelOutputs().get(0);
                    ModelTensor modelTensor = modelTensors.getMlModelTensors().get(0);
                    Map<String, String> dataAsMap = (Map<String, String>) modelTensor.getDataAsMap();
                    String ppl = parseOutput(dataAsMap.get("response"), indexName);
                    boolean pplExecutedEnabled = clusterSettingHelper.getClusterSettings(SkillSettings.PPL_EXECUTION_ENABLED);
                    if (!pplExecutedEnabled || !this.execute) {
                        if (!pplExecutedEnabled) {
                            log
                                .debug(
                                    "PPL execution is disabled, the query will be returned directly, to enable this, please set plugins.skills.ppl_execution_enabled to true"
                                );
                        }
                        Map<String, String> ret = ImmutableMap.of("ppl", ppl);
                        listener.onResponse((T) AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(ret)));
                        return;
                    }
                    JSONObject jsonContent = new JSONObject(ImmutableMap.of("query", ppl));
                    PPLQueryRequest pplQueryRequest = new PPLQueryRequest(ppl, jsonContent, null, "jdbc");
                    TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest(pplQueryRequest);
                    client
                        .execute(
                            PPLQueryAction.INSTANCE,
                            transportPPLQueryRequest,
                            getPPLTransportActionListener(ActionListener.wrap(transportPPLQueryResponse -> {
                                String results = transportPPLQueryResponse.getResult();
                                Map<String, String> returnResults = ImmutableMap.of("ppl", ppl, "executionResult", results);
                                listener
                                    .onResponse(
                                        (T) AccessController
                                            .doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(returnResults))
                                    );
                            }, e -> {
                                String pplError = "execute ppl:" + ppl + ", get error: " + e.getMessage();
                                Exception exception = new Exception(pplError);
                                listener.onFailure(exception);
                            }))
                        );
                    // Execute output here
                }, e -> {
                    log.error(String.format(Locale.ROOT, "fail to predict model: %s with error: %s", modelId, e.getMessage()), e);
                    listener.onFailure(e);
                }));
            }, e -> {
                log.error(String.format(Locale.ROOT, "fail to search model: %s with error: %s", modelId, e.getMessage()), e);
                listener.onFailure(e);
            }));
        }, e -> {
            log.error(String.format(Locale.ROOT, "fail to get mapping of index: %s with error: %s", indexName, e.getMessage()), e);
            String errorMessage = e.getMessage();
            if (errorMessage.contains("no such index")) {
                listener
                    .onFailure(
                        new IllegalArgumentException(
                            "Return this final answer to human directly and do not use other tools: 'Please provide index name'. Please try to directly send this message to human to ask for index name"
                        )
                    );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return parameters != null && !parameters.isEmpty();
    }

    public static class Factory implements Tool.Factory<PPLTool> {
        private Client client;

        private ClusterSettingHelper clusterSettingHelper;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (PPLTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client, ClusterSettingHelper clusterSettingHelper) {
            this.client = client;
            this.clusterSettingHelper = clusterSettingHelper;
        }

        @Override
        public PPLTool create(Map<String, Object> map) {
            validatePPLToolParameters(map);
            return new PPLTool(
                client,
                clusterSettingHelper,
                (String) map.get("model_id"),
                (String) map.getOrDefault("prompt", ""),
                (String) map.getOrDefault("model_type", ""),
                (String) map.getOrDefault("previous_tool_name", ""),
                NumberUtils.toInt((String) map.get("head"), -1),
                Boolean.parseBoolean((String) map.getOrDefault("execute", "true"))
            );
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

    private SearchRequest buildSearchRequest(String indexName) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(1).query(new MatchAllQueryBuilder());
        // client;
        return new SearchRequest(new String[] { indexName }, searchSourceBuilder);
    }

    private GetMappingsRequest buildGetMappingRequest(String indexName) {
        String[] indices = new String[] { indexName };
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices);
        return getMappingsRequest;
    }

    private static void validatePPLToolParameters(Map<String, Object> map) {
        if (StringUtils.isBlank((String) map.get("model_id"))) {
            throw new IllegalArgumentException("PPL tool needs non blank model id.");
        }
        if (map.containsKey("execute") && Objects.nonNull(map.get("execute"))) {
            String execute = map.get("execute").toString().toLowerCase(Locale.ROOT);
            if (!execute.equals("true") && !execute.equals("false")) {
                throw new IllegalArgumentException("PPL tool parameter execute must be false or true");
            }

        }
        if (map.containsKey("head")) {
            String head = map.get("head").toString();
            try {
                int headInt = NumberUtils.createInteger(head);
            } catch (Exception e) {
                throw new IllegalArgumentException("PPL tool parameter head must be integer.");
            }
        }
    }

    private String constructTableInfo(SearchHit[] searchHits, Map<String, MappingMetadata> mappings) throws PrivilegedActionException {
        String firstIndexName = (String) mappings.keySet().toArray()[0];
        MappingMetadata mappingMetadata = mappings.get(firstIndexName);
        Map<String, Object> mappingSource = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        if (Objects.isNull(mappingSource)) {
            throw new IllegalArgumentException(
                "The querying index doesn't have mapping metadata, please add data to it or using another index."
            );
        }
        Map<String, String> fieldsToType = new HashMap<>();
        extractNamesTypes(mappingSource, fieldsToType, "");
        StringJoiner tableInfoJoiner = new StringJoiner("\n");
        List<String> sortedKeys = new ArrayList<>(fieldsToType.keySet());
        Collections.sort(sortedKeys);

        if (searchHits.length > 0) {
            SearchHit hit = searchHits[0];
            Map<String, Object> sampleSource = hit.getSourceAsMap();
            Map<String, String> fieldsToSample = new HashMap<>();
            for (String key : fieldsToType.keySet()) {
                fieldsToSample.put(key, "");
            }
            extractSamples(sampleSource, fieldsToSample, "");

            for (String key : sortedKeys) {
                if (ALLOWED_FIELDS_TYPE.contains(fieldsToType.get(key))) {
                    String line = "- " + key + ": " + fieldsToType.get(key) + " (" + fieldsToSample.get(key) + ")";
                    tableInfoJoiner.add(line);
                }
            }
        } else {
            for (String key : sortedKeys) {
                if (ALLOWED_FIELDS_TYPE.contains(fieldsToType.get(key))) {
                    String line = "- " + key + ": " + fieldsToType.get(key);
                    tableInfoJoiner.add(line);
                }
            }
        }

        return tableInfoJoiner.toString();
    }

    private String constructPrompt(String tableInfo, String question, String indexName) {
        Map<String, String> indexInfo = ImmutableMap.of("mappingInfo", tableInfo, "question", question, "indexName", indexName);
        StringSubstitutor substitutor = new StringSubstitutor(indexInfo, "${indexInfo.", "}");
        return substitutor.replace(contextPrompt);
    }

    private void extractNamesTypes(Map<String, Object> mappingSource, Map<String, String> fieldsToType, String prefix) {
        if (!prefix.isEmpty()) {
            prefix += ".";
        }

        for (Map.Entry<String, Object> entry : mappingSource.entrySet()) {
            String n = entry.getKey();
            Object v = entry.getValue();

            if (v instanceof Map) {
                Map<String, Object> vMap = (Map<String, Object>) v;
                if (vMap.containsKey("type")) {
                    if (!((vMap.getOrDefault("type", "")).equals("alias"))) {
                        fieldsToType.put(prefix + n, (String) vMap.get("type"));
                    }
                } else if (vMap.containsKey("properties")) {
                    extractNamesTypes((Map<String, Object>) vMap.get("properties"), fieldsToType, prefix + n);
                }
            }
        }
    }

    private static void extractSamples(Map<String, Object> sampleSource, Map<String, String> fieldsToSample, String prefix)
        throws PrivilegedActionException {
        if (!prefix.isEmpty()) {
            prefix += ".";
        }

        for (Map.Entry<String, Object> entry : sampleSource.entrySet()) {
            String p = entry.getKey();
            Object v = entry.getValue();

            String fullKey = prefix + p;
            if (fieldsToSample.containsKey(fullKey)) {
                fieldsToSample.put(fullKey, AccessController.doPrivileged((PrivilegedExceptionAction<String>) () -> gson.toJson(v)));
            } else {
                if (v instanceof Map) {
                    extractSamples((Map<String, Object>) v, fieldsToSample, fullKey);
                }
            }
        }
    }

    private <T extends ActionResponse> ActionListener<T> getPPLTransportActionListener(ActionListener<TransportPPLQueryResponse> listener) {
        return ActionListener.wrap(r -> { listener.onResponse(TransportPPLQueryResponse.fromActionResponse(r)); }, listener::onFailure);
    }

    @SuppressWarnings("unchecked")
    private void extractFromChatParameters(Map<String, String> parameters) {
        if (parameters.containsKey("input")) {
            String input = parameters.get("input");
            try {
                Map<String, String> chatParameters = gson.fromJson(input, Map.class);
                parameters.putAll(chatParameters);
            } catch (Exception e) {
                log.error(String.format(Locale.ROOT, "Failed to parse chat parameters, input is: %s, which is not a valid json", input), e);
            }
        }
    }

    private String parseOutput(String llmOutput, String indexName) {
        String ppl;
        Pattern pattern = Pattern.compile("<ppl>((.|[\\r\\n])+?)</ppl>"); // For ppl like <ppl> source=a \n | fields b </ppl>
        Matcher matcher = pattern.matcher(llmOutput);

        if (matcher.find()) {
            ppl = matcher.group(1).replaceAll("[\\r\\n]", "").replaceAll("ISNOTNULL", "isnotnull").trim();
        } else { // logic for only ppl returned
            int sourceIndex = llmOutput.indexOf("source=");
            int describeIndex = llmOutput.indexOf("describe ");
            if (sourceIndex != -1) {
                llmOutput = llmOutput.substring(sourceIndex);

                // Splitting the string at "|"
                String[] lists = llmOutput.split("\\|");

                // Modifying the first element
                if (lists.length > 0) {
                    lists[0] = "source=" + indexName;
                }

                // Joining the string back together
                ppl = String.join("|", lists);
            } else if (describeIndex != -1) {
                llmOutput = llmOutput.substring(describeIndex);
                String[] lists = llmOutput.split("\\|");

                // Modifying the first element
                if (lists.length > 0) {
                    lists[0] = "describe " + indexName;
                }

                // Joining the string back together
                ppl = String.join("|", lists);
            } else {
                throw new IllegalArgumentException("The returned PPL: " + llmOutput + " has wrong format");
            }
        }
        if (this.pplModelType != PPLModelType.FINETUNE) {
            ppl = ppl.replace("`", "");
        }
        ppl = ppl.replaceAll("\\bSPAN\\(", "span(");
        if (this.head > 0) {
            String[] lists = llmOutput.split("\\|");
            String lastCommand = lists[lists.length - 1].strip();
            if (!lastCommand.toLowerCase(Locale.ROOT).startsWith("head")) // not handle cases source=...| ... | head 5 | head <head>
            {
                ppl = ppl + " | head " + this.head;
            }
        }
        return ppl;
    }

    private String getIndexNameFromParameters(Map<String, String> parameters) {
        String indexName = parameters.getOrDefault("index", "");
        if (!StringUtils.isBlank(this.previousToolKey) && StringUtils.isBlank(indexName)) {
            indexName = parameters.getOrDefault(this.previousToolKey + ".output", ""); // read index name from previous key
        }
        return indexName.trim();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> loadDefaultPromptDict() {
        try (InputStream searchResponseIns = PPLTool.class.getResourceAsStream("PPLDefaultPrompt.json")) {
            if (searchResponseIns != null) {
                String defaultPromptContent = new String(searchResponseIns.readAllBytes(), StandardCharsets.UTF_8);
                return gson.fromJson(defaultPromptContent, Map.class);
            }
        } catch (IOException e) {
            log.error("Failed to load default prompt dict", e);
        }
        return new HashMap<>();
    }
}
