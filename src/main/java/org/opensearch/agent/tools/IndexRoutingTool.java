/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.agent.job.IndexSummaryEmbeddingJob;
import org.opensearch.agent.job.MLClients;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.search.SearchHit;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@ToolAnnotation(IndexRoutingTool.TYPE)
public class IndexRoutingTool extends AbstractRetrieverTool {

    public static String EMBEDDING_MODEL_ID = "embedding_model_id";
    public static String LLM_MODEL_ID = "llm_model_id";

    @Getter
    @Setter
    private String embeddingModelId;
    @Getter
    @Setter
    private String llmModelId;

    public static final String TYPE = "IndexRoutingTool";

    private ClusterService clusterService;

    private MLClients mlClients;

    @Builder
    public IndexRoutingTool(
        Client client,
        MLClients mlClients,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        String index,
        String[] sourceFields,
        Integer docSize,
        String embeddingModelId,
        String llmModelId
    ) {
        super(client, xContentRegistry, index, sourceFields, docSize);
        this.client = client;
        this.mlClients = mlClients;
        this.clusterService = clusterService;
        this.embeddingModelId = embeddingModelId;
        this.llmModelId = llmModelId;
        outputParser = mlTaskResponse -> {
            ModelTensorOutput output = (ModelTensorOutput) mlTaskResponse.getOutput();
            ModelTensor modelTensor = output.getMlModelOutputs().get(0).getMlModelTensors().get(0);
            return (String) modelTensor.getDataAsMap().get("response");
        };
    }

    private static final String DEFAULT_DESCRIPTION = "Use this tool to select an appropriate index for your question, "
        + "This tool take user question as input and return list of most related indexes or `Not sure`. "
        + "If the tool returns `Not sure`, mark it as final answer and ask Human to input exact index name";

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
    protected String getQueryBody(String queryText) {
        try {
            Number[] vector = mlClients.getEmbeddingResult(embeddingModelId, List.of(queryText), mlTaskResponse -> {
                ModelTensorOutput tensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
                return tensorOutput.getMlModelOutputs().get(0).getMlModelTensors().get(0).getData();
            });

            XContentBuilder xContentBuilder = XContentFactory
                .jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("knn")
                .startObject(IndexSummaryEmbeddingJob.INDEX_SUMMARY_EMBEDDING_FIELD_PREFIX + "_" + embeddingModelId)
                .field("vector", vector)
                .field("k", docSize)
                .endObject()
                .endObject()
                .endObject()
                .endObject();

            return xContentBuilder.toString();
        } catch (IOException e) {
            log.error("Can't build query to vector index");
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Parser<SearchResponse, Object> searchResponseParser() {
        return searchResponse -> {
            SearchHit[] hits = searchResponse.getHits().getHits();

            List<Map<String, Object>> indexSummaries = new ArrayList<>();
            if (hits != null) {
                for (SearchHit hit : hits) {
                    indexSummaries.add(hit.getSourceAsMap());
                }
            }
            return indexSummaries;
        };
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        // get index of knn-index
        super.run(parameters, ActionListener.wrap(res -> {
            List<Map<String, Object>> summaries = (List<Map<String, Object>>) res;

            if (summaries.isEmpty()) {
                listener.onResponse((T) "No index found");
                return;
            }

            // build prompt
            String summaryStr = summaries
                .stream()
                .map(
                    summaryMap -> String
                        .format(
                            Locale.ROOT,
                            "%s:%s",
                            summaryMap.get(IndexSummaryEmbeddingJob.INDEX_NAME_FIELD),
                            summaryMap.get(IndexSummaryEmbeddingJob.INDEX_SUMMARY_FIELD)
                        )
                )
                .collect(Collectors.joining("\\n---\\n"));

            // call LLM, MLModelTool
            String question = parameters.get(INPUT_FIELD);
            String prompt = buildPrompt(summaryStr, question);
            mlClients.inference(llmModelId, prompt, ActionListener.wrap(r -> {
                String result = outputParser.parse(r);
                List<String> validIndexes = findMatchedIndex(result);
                listener.onResponse((T) String.join(",", validIndexes));
            }, exception -> { listener.onResponse((T) "Not sure"); }));
        }, exception -> {
            log.error("Failed to query index");
            listener.onFailure(exception);
        }));
    }

    private List<String> findMatchedIndex(String result) {
        List<String> validIndexes = new ArrayList<>();
        Set<String> allIndexes = clusterService.state().metadata().indices().keySet();
        List<String> predictedIndexes = Arrays.stream(result.split(",")).map(String::trim).collect(Collectors.toList());

        for (String predictedIndex : predictedIndexes) {
            if (allIndexes.contains(predictedIndex) || predictedIndex.equals("Not sure")) {
                validIndexes.add(predictedIndex);
            } else {
                Optional<String> similarityIndex = findWithSimilarity(predictedIndex, allIndexes);
                similarityIndex.ifPresent(validIndexes::add);
            }
        }
        return validIndexes;
    }

    private Optional<String> findWithSimilarity(String predictedIndex, Set<String> allIndexes) {
        JaccardSimilarity similarity = new JaccardSimilarity();
        return allIndexes
            .stream()
            .map(index -> Pair.of(index, similarity.apply(index, predictedIndex)))
            .filter(pair -> pair.getValue() > 0.9)
            .max(Map.Entry.comparingByValue())
            .map(Pair::getKey);
    }

    private String buildPrompt(String summaryString, String question) {
        // TODO set template as toolSpec
        String defaultTemplate = "Human: You are an experienced engineer in OpenSearch and ElasticSearch. \n"
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
            + "        Answer:\n\nAssistant:";

        Map<String, String> params = Map.of("question", question, "summaries", summaryString);
        return new StringSubstitutor(params).replace(defaultTemplate);
    }

    @Override
    public boolean validate(Map<String, String> params) {
        return params.containsKey("input");
    }

    public static class Factory implements Tool.Factory<IndexRoutingTool> {
        private Client client;
        private ClusterService clusterService;
        private NamedXContentRegistry xContentRegistry;

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

        public void init(Client client, ClusterService clusterService, NamedXContentRegistry xContentRegistry) {
            this.client = client;
            this.clusterService = clusterService;
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public IndexRoutingTool create(Map<String, Object> params) {
            String embeddingModelId = (String) params.get(EMBEDDING_MODEL_ID);
            String llmModelId = (String) params.get(LLM_MODEL_ID);
            return IndexRoutingTool
                .builder()
                .client(client)
                .xContentRegistry(xContentRegistry)
                .mlClients(new MLClients(client))
                .clusterService(clusterService)
                .embeddingModelId(embeddingModelId)
                .llmModelId(llmModelId)
                .index(IndexSummaryEmbeddingJob.INDEX_SUMMARY_EMBEDDING_INDEX)
                .sourceFields(new String[] { IndexSummaryEmbeddingJob.INDEX_NAME_FIELD, IndexSummaryEmbeddingJob.INDEX_SUMMARY_FIELD })
                .build();
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
