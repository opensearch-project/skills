/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;
import org.opensearch.action.ActionRequest;
import org.opensearch.client.Client;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class IndexRoutingTool extends AbstractRetrieverTool {

    public static String EMBEDDING_MODEL_ID = "embedding_model_id";

    @Getter
    @Setter
    private String embeddingModelId;
    @Getter
    @Setter
    private String llmModelId;

    public static final String TYPE = "IndexRoutingTool";

    @Setter
    private Client client;

    @Builder
    public IndexRoutingTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        String index,
        String[] sourceFields,
        Integer docSize,
        String embeddingModelId,
        String llmModelId
    ) {
        super(client, xContentRegistry, index, sourceFields, docSize);
        this.client = client;
        this.embeddingModelId = embeddingModelId;
        this.llmModelId = llmModelId;
        outputParser = new Parser<MLTaskResponse, String>() {
            @Override
            public String parse(MLTaskResponse mlTaskResponse) {
                return "null";
            }
        };
    }

    private static final String DEFAULT_DESCRIPTION = "Use this tool to select an appropriate index for your question";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;

    private Parser<MLTaskResponse, String> outputParser;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected String getQueryBody(String question) {
        RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet
            .builder()
            .parameters(Collections.singletonMap("prompt", ""))
            .build();
        MLPredictionTaskRequest request = new MLPredictionTaskRequest(
            llmModelId,
            MLInput.builder().algorithm(FunctionName.TEXT_EMBEDDING).inputDataset(inputDataSet).build()
        );

        ActionFuture<MLTaskResponse> mlTaskResponseActionFuture = client.execute(MLPredictionTaskAction.INSTANCE, request);

        try {
            MLTaskResponse mlTaskResponse = mlTaskResponseActionFuture.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return "{\n"
            + "\"query\": {\n"
            + "    \"knn\": {\n"
            + "        \"index_summary_${embedding_model_id}\": {\n"
            + "            \"vector\": ${embedding_vector},\n"
            + "            \"k\": ${k}\n"
            + "        }\n"
            + "    }\n"
            + "  }\n"
            + "}\n";
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        // get index of knn-index
        this.index = String.format(Locale.ROOT, "index_summary_embedding_%s", embeddingModelId);

        super.run(parameters, ActionListener.wrap(res -> {
            T searchOutput;
            if (res.equals("Can not get any match from search result.")) {
                searchOutput = (T) "";
            } else {
                Gson gson = new Gson();
                String[] hits = res.toString().split("\n");

                StringBuilder resultBuilder = new StringBuilder();
                for (String hit : hits) {
                    JsonObject jsonObject = gson.fromJson(hit, JsonObject.class);
                    String id = jsonObject.get("_id").getAsString();
                    JsonObject source = jsonObject.getAsJsonObject("_source");

                    resultBuilder.append("_id: ").append(id).append("\n");
                    resultBuilder.append("_source: ").append(source.toString()).append("\n");
                }

                searchOutput = (T) gson.toJson(resultBuilder.toString());
            }
            // call LLM, MLModelTool
            List<String> summaryList = new ArrayList<>();
            String question = parameters.get("question");
            String prompt = buildPrompt(summaryList, question);
            RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet
                .builder()
                .parameters(Collections.singletonMap("prompt", prompt))
                .build();
            ActionRequest request = new MLPredictionTaskRequest(
                llmModelId,
                MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build()
            );
            client
                .execute(
                    MLPredictionTaskAction.INSTANCE,
                    request,
                    ActionListener.wrap(r -> { listener.onResponse((T) outputParser.parse(r)); }, e -> {
                        log.error("Failed to run model " + llmModelId, e);
                        listener.onFailure(e);
                    })
                );

        }, exception -> { log.error("failed to retrieve doc for index summary"); }));

    }

    private String buildPrompt(List<String> summaries, String question) {
        String defaultTemplate = "        You are an experienced engineer in OpenSearch and ElasticSearch. \n"
            + "\n"
            + "        Given a question, your task is to choose the relevant indexes from a list of indexes.\n"
            + "\n"
            + "        For every index, you will be given the index mapping, followed by sample data from the index.\n"
            + "\n"
            + "        The data format is like:\n"
            + "\n"
            + "        index-1: Index Mappings:\n"
            + "        mappings of index-1\n"
            + "        Sample data:\n"
            + "        data from index-1\n"
            + "        ---\n"
            + "        index-2: Index Mappings:\n"
            + "        mappings of index-2\n"
            + "        Sample data:\n"
            + "        data from index-2\n"
            + "        ---\n"
            + "        ...\n"
            + "\n"
            + "        Now the actual index mappings and sample data begins:\n"
            + "        ${summaries}\n"
            + "\n"
            + "        -------------------\n"
            + "\n"
            + "        Format the output as a comma-separated sequence, e.g. index-1, index-2, index-3. If no indexes \n"
            + "        appear relevant to the question, return the empty string ''.\n"
            + "\n"
            + "        Just return the index names, nothing else. \n"
            + "        If you are not sure, just return 'Not sure', nothing else.\n"
            + "\n"
            + "        Question: ${question}\n"
            + "        Answer:\n";

        String summaryStr = String.join("\\n---\\n", summaries);

        Map<String, String> params = Map.of("question", question, "summaries", summaryStr);
        StringSubstitutor substitutor = new StringSubstitutor(params);
        return substitutor.replace(defaultTemplate);
    }

    @Override
    public boolean validate(Map<String, String> params) {
        return params.containsKey("input");
    }

    public static class Factory implements Tool.Factory<IndexRoutingTool> {
        private Client client;

        private static IndexRoutingTool.Factory INSTANCE;

        public static IndexRoutingTool.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (IndexRoutingTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new IndexRoutingTool.Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public IndexRoutingTool create(Map<String, Object> params) {
            String embeddingModelId = (String) params.get(EMBEDDING_MODEL_ID);
            return IndexRoutingTool.builder().client(client).embeddingModelId(embeddingModelId).build();
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
