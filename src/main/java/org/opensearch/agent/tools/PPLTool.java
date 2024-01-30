/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.CommonValue.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.json.JSONObject;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
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
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

import com.google.common.collect.ImmutableMap;
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

    private static final String DEFAULT_DESCRIPTION = "Use this tool to generate PPL and execute.";

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

    private static Gson gson = new Gson();

    private static Map<String, String> defaultPromptDict;

    static {
        try {
            defaultPromptDict = loadDefaultPromptDict();
        } catch (IOException e) {
            log.error("fail to load default prompt dict" + e.getMessage());
            defaultPromptDict = new HashMap<>();
        }
    }

    public enum PPLModelType {
        CLAUDE,
        FINETUNE;

        public static PPLModelType from(String value) {
            if (value.isEmpty()) {
                return PPLModelType.CLAUDE;
            }
            try {
                return PPLModelType.valueOf(value.toUpperCase(Locale.ROOT));
            } catch (Exception e) {
                log.error("Wrong PPL Model type, should be CLAUDE or FINETUNE");
                return PPLModelType.CLAUDE;
            }
        }

    }

    public PPLTool(Client client, String modelId, String contextPrompt, String pplModelType, boolean execute) {
        this.client = client;
        this.modelId = modelId;
        this.pplModelType = PPLModelType.from(pplModelType);
        if (contextPrompt.isEmpty()) {
            this.contextPrompt = this.defaultPromptDict.getOrDefault(this.pplModelType.toString(), "");
        } else {
            this.contextPrompt = contextPrompt;
        }
        this.execute = execute;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        parameters = extractFromChatParameters(parameters);
        String indexName = parameters.get("index");
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
        SearchRequest searchRequest = buildSearchRequest(indexName);
        GetMappingsRequest getMappingsRequest = buildGetMappingRequest(indexName);
        client.admin().indices().getMappings(getMappingsRequest, ActionListener.<GetMappingsResponse>wrap(getMappingsResponse -> {
            Map<String, MappingMetadata> mappings = getMappingsResponse.getMappings();
            if (mappings.size() == 0) {
                throw new IllegalArgumentException("No matching mapping with index name: " + indexName);
            }
            client.search(searchRequest, ActionListener.<SearchResponse>wrap(searchResponse -> {
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                String tableInfo = constructTableInfo(searchHits, mappings);
                String prompt = constructPrompt(tableInfo, question, indexName);
                RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet
                    .builder()
                    .parameters(Collections.singletonMap("prompt", prompt))
                    .build();
                ActionRequest request = new MLPredictionTaskRequest(
                    modelId,
                    MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build(),
                    null
                );
                client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.<MLTaskResponse>wrap(mlTaskResponse -> {
                    ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
                    ModelTensors modelTensors = modelTensorOutput.getMlModelOutputs().get(0);
                    ModelTensor modelTensor = modelTensors.getMlModelTensors().get(0);
                    Map<String, String> dataAsMap = (Map<String, String>) modelTensor.getDataAsMap();
                    String ppl = parseOutput(dataAsMap.get("response"), indexName);
                    if (!this.execute) {
                        listener.onResponse((T) ppl);
                        return;
                    }
                    JSONObject jsonContent = new JSONObject(ImmutableMap.of("query", ppl));
                    PPLQueryRequest pplQueryRequest = new PPLQueryRequest(ppl, jsonContent, null, "jdbc");
                    TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest(pplQueryRequest);
                    client
                        .execute(
                            PPLQueryAction.INSTANCE,
                            transportPPLQueryRequest,
                            getPPLTransportActionListener(ActionListener.<TransportPPLQueryResponse>wrap(transportPPLQueryResponse -> {
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
                    log.info("fail to predict model: " + e);
                    listener.onFailure(e);
                }));
            }, e -> {
                log.info("fail to search: " + e);
                listener.onFailure(e);
            }

            ));
        }, e -> {
            log.info("fail to get mapping: " + e);
            listener.onFailure(e);
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
        if (parameters == null || parameters.size() == 0) {
            return false;
        }
        return true;
    }

    public static class Factory implements Tool.Factory<PPLTool> {
        private Client client;

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

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public PPLTool create(Map<String, Object> map) {
            return new PPLTool(
                client,
                (String) map.get("model_id"),
                (String) map.getOrDefault("prompt", ""),
                (String) map.getOrDefault("model_type", ""),
                Boolean.valueOf((String) map.getOrDefault("execute", "true"))
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
        SearchRequest request = new SearchRequest(new String[] { indexName }, searchSourceBuilder);
        return request;
    }

    private GetMappingsRequest buildGetMappingRequest(String indexName) {
        String[] indices = new String[] { indexName };
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices);
        return getMappingsRequest;
    }

    private String constructTableInfo(SearchHit[] searchHits, Map<String, MappingMetadata> mappings) throws PrivilegedActionException {
        String firstIndexName = (String) mappings.keySet().toArray()[0];
        MappingMetadata mappingMetadata = mappings.get(firstIndexName);
        Map<String, Object> mappingSource = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
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
                String line = "- " + key + ": " + fieldsToType.get(key) + " (" + fieldsToSample.get(key) + ")";
                tableInfoJoiner.add(line);
            }
        } else {
            for (String key : sortedKeys) {
                String line = "- " + key + ": " + fieldsToType.get(key);
                tableInfoJoiner.add(line);
            }
        }

        String tableInfo = tableInfoJoiner.toString();
        return tableInfo;
    }

    private String constructPrompt(String tableInfo, String question, String indexName) {
        Map<String, String> indexInfo = ImmutableMap.of("mappingInfo", tableInfo, "question", question, "indexName", indexName);
        StringSubstitutor substitutor = new StringSubstitutor(indexInfo, "${indexInfo.", "}");
        String finalPrompt = substitutor.replace(contextPrompt);
        return finalPrompt;
    }

    private void extractNamesTypes(Map<String, Object> mappingSource, Map<String, String> fieldsToType, String prefix) {
        if (prefix.length() > 0) {
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
        if (prefix.length() > 0) {
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

    private Map<String, String> extractFromChatParameters(Map<String, String> parameters) {
        if (parameters.containsKey("input")) {
            try {
                Map<String, String> chatParameters = gson.fromJson(parameters.get("input"), Map.class);
                parameters.putAll(chatParameters);
            } finally {
                return parameters;
            }
        }
        return parameters;
    }

    private String parseOutput(String llmOutput, String indexName) {
        String ppl;
        Pattern pattern = Pattern.compile("<ppl>((.|[\\r\\n])+?)</ppl>"); // For ppl like <ppl> source=a \n | fields b </ppl>
        Matcher matcher = pattern.matcher(llmOutput);

        if (matcher.find()) {
            ppl = matcher.group(1).replaceAll("[\\r\\n]", "").replaceAll("ISNOTNULL", "isnotnull").trim();
        } else { // logic for only ppl returned
            int sourceIndex = llmOutput.indexOf("source=");
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
            } else {
                throw new IllegalArgumentException("The returned PPL: " + llmOutput + " has wrong format");
            }
        }
        ppl = ppl.replace("`", "");
        ppl = ppl.replaceAll("\\bSPAN\\(", "span(");
        return ppl;
    }

    private static Map<String, String> loadDefaultPromptDict() throws IOException {
        InputStream searchResponseIns = PPLTool.class.getResourceAsStream("PPLDefaultPrompt.json");
        if (searchResponseIns != null) {
            String defaultPromptContent = new String(searchResponseIns.readAllBytes(), StandardCharsets.UTF_8);
            Map<String, String> defaultPromptDict = gson.fromJson(defaultPromptContent, Map.class);
            return defaultPromptDict;
        }
        return new HashMap<>();
    }

}
