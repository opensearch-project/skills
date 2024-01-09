/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.DATA_STREAM;
import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.INDEX_NAME;
import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.INDEX_PATTERNS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.similarity.LongestCommonSubsequence;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.agent.job.IndexSummaryEmbeddingJob;
import org.opensearch.agent.job.MLClients;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.search.SearchHit;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@ToolAnnotation(IndexRoutingTool.TYPE)
public class IndexRoutingTool extends VectorDBTool {

    public static final String TYPE = "IndexRoutingTool";

    private static final String DEFAULT_DESCRIPTION = "Use this tool to select an appropriate index for user question, "
        + "This tool take user plain original question as input and return list of most related indexes or `Not sure`. "
        + "If the tool returns `Not sure`, mark it as final answer and ask Human to provide index name";

    public static final int DEFAULT_K = 5;
    public static String EMBEDDING_MODEL_ID = "embedding_model_id";
    public static String INFERENCE_MODEL_ID = "inference_model_id";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;

    @Getter
    @Setter
    private String inferenceModelId;

    private final MLClients mlClients;

    public IndexRoutingTool(
        Client client,
        NamedXContentRegistry xContentRegistry,
        Integer docSize,
        Integer k,
        String embeddingModelId,
        String inferenceModelId
    ) {
        super(
            client,
            xContentRegistry,
            IndexSummaryEmbeddingJob.INDEX_SUMMARY_EMBEDDING_INDEX,
            IndexSummaryEmbeddingJob.INDEX_SUMMARY_EMBEDDING_FIELD_PREFIX + "_" + embeddingModelId,
            new String[] {
                IndexSummaryEmbeddingJob.INDEX_NAME_FIELD,
                IndexSummaryEmbeddingJob.INDEX_SUMMARY_FIELD,
                IndexSummaryEmbeddingJob.INDEX_PATTERNS_FIELD,
                IndexSummaryEmbeddingJob.DATA_STREAM_FIELD,
                IndexSummaryEmbeddingJob.ALIAS_FIELD },
            Optional.ofNullable(docSize).orElse(DEFAULT_K),
            embeddingModelId,
            Optional.ofNullable(k).orElse(DEFAULT_K)
        );
        this.mlClients = new MLClients(client);
        this.inferenceModelId = inferenceModelId;
    }

    @Override
    public String getType() {
        return TYPE;
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
            // TODO use MLModelTool
            mlClients.inference(inferenceModelId, prompt, ActionListener.wrap(r -> {
                ModelTensorOutput output = (ModelTensorOutput) r.getOutput();
                ModelTensor modelTensor = output.getMlModelOutputs().get(0).getMlModelTensors().get(0);
                String response = (String) modelTensor.getDataAsMap().get("response");
                List<String> validIndexes = findMatchedIndex(response, summaries);
                listener.onResponse((T) String.join(",", validIndexes));
            }, exception -> { listener.onResponse((T) "Not sure"); }));
        }, exception -> {
            log.error("Failed to query index");
            listener.onFailure(exception);
        }));
    }

    private List<String> findMatchedIndex(String result, List<Map<String, Object>> candidates) {
        List<String> validIndexes = new ArrayList<>();

        Map<String, Map<String, Object>> candidateIndexMap = candidates
            .stream()
            .collect(Collectors.toMap(m -> (String) m.get(INDEX_NAME), m -> m));

        Set<String> allCandidates = candidateIndexMap.keySet();
        List<String> predictedIndexes = Arrays.stream(result.split(",")).map(String::trim).collect(Collectors.toList());

        for (String predictedIndex : predictedIndexes) {
            if (allCandidates.contains(predictedIndex)) {
                Map<String, Object> map = candidateIndexMap.get(predictedIndex);
                // data stream back index
                Optional<Object> dataStreamName = Optional.ofNullable(map.get(DATA_STREAM));
                List<String> patterns = (List<String>) map.get(INDEX_PATTERNS);
                String indexPattern = getIndexPattern(patterns, predictedIndex);
                validIndexes.add((String) dataStreamName.orElse(indexPattern));
            } else if (predictedIndex.equals("Not sure")) {
                validIndexes.add(predictedIndex);
            } else {
                Optional<String> similarityIndex = findWithSimilarity(predictedIndex, allCandidates);
                similarityIndex.ifPresent(validIndexes::add);
            }
        }
        return validIndexes;
    }

    private String getIndexPattern(List<String> patterns, String predictedIndex) {
        // if index name is short, we might don't need to use index pattern
        if (!patterns.isEmpty() && predictedIndex.length() >= 10) {
            patterns.sort(Comparator.comparing(String::length).reversed());
            String longestPattern = patterns.get(0);
            // index pattern can't be * or too generic
            if (!longestPattern.equals("*") && (longestPattern.length() * 2 > predictedIndex.length())) {
                return longestPattern;
            }
        }
        return predictedIndex;
    }

    private Optional<String> findWithSimilarity(String predictedIndex, Collection<String> allIndexes) {
        LongestCommonSubsequence lcs = new LongestCommonSubsequence();
        return allIndexes.stream().map(index -> {
            CharSequence lcsResult = lcs.longestCommonSubsequence(index, predictedIndex);
            // Ratio = 2.0 * M / T
            Double ratio = (2.0 * lcsResult.length()) / (index.length() + predictedIndex.length());
            return Pair.of(index, ratio);
        }).filter(pair -> pair.getValue() > 0.9).max(Map.Entry.comparingByValue()).map(Pair::getKey);
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

        public void init(Client client, NamedXContentRegistry xContentRegistry) {
            this.client = client;
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public IndexRoutingTool create(Map<String, Object> params) {
            String embeddingModelId = (String) params.get(EMBEDDING_MODEL_ID);
            String inferenceModelId = (String) params.get(INFERENCE_MODEL_ID);
            Integer docSize = params.containsKey(DOC_SIZE_FIELD) ? Integer.parseInt((String) params.get(DOC_SIZE_FIELD)) : DEFAULT_K;
            Integer k = params.containsKey(K_FIELD) ? Integer.parseInt((String) params.get(K_FIELD)) : DEFAULT_K;
            return new IndexRoutingTool(client, xContentRegistry, docSize, k, embeddingModelId, inferenceModelId);
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
