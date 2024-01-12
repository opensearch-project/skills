/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.DATA_STREAM_FIELD;
import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.INDEX_NAME_FIELD;
import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.INDEX_PATTERNS_FIELD;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.similarity.LongestCommonSubsequence;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.agent.job.IndexSummaryEmbeddingJob;
import org.opensearch.agent.job.MLClients;
import org.opensearch.agent.tools.utils.LLMProvider;
import org.opensearch.client.Client;
import org.opensearch.common.io.Streams;
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
        + "It takes 1 argument which is a string of user question and return list of most related indexes or `Not sure`. "
        + "If the tool returns `Not sure`, mark it as final answer and ask Human to provide index name";

    public static final int DEFAULT_K = 5;
    public static String EMBEDDING_MODEL_ID = "embedding_model_id";
    public static String INFERENCE_MODEL_ID = "inference_model_id";
    public static String PROMPT_TEMPLATE = "prompt_template";
    public static String LLM_PROVIDER = "llm_provider";

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

    @Setter
    private String prompt;

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
        this.mlClients = new MLClients(client, xContentRegistry);
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
        log.debug("input={}", parameters.get(INPUT_FIELD));
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
            String prompt = buildFinalPrompt(summaryStr, question);
            log.debug("prompt send to inference is {}", prompt);
            // TODO use MLModelTool
            mlClients.inference(inferenceModelId, prompt, ActionListener.wrap(r -> {
                ModelTensorOutput output = (ModelTensorOutput) r.getOutput();
                ModelTensor modelTensor = output.getMlModelOutputs().get(0).getMlModelTensors().get(0);
                String response = (String) modelTensor.getDataAsMap().get("response");
                log.debug("response back from inference mode is {}", response);
                Set<String> validIndexes = findMatchedIndex(response, summaries);
                listener.onResponse((T) (validIndexes.isEmpty() ? "Not sure" : validIndexes.iterator().next()));
            }, exception -> { listener.onResponse((T) "Not sure"); }));
        }, exception -> {
            log.error("Failed to query index");
            listener.onFailure(exception);
        }));
    }

    private Set<String> findMatchedIndex(String result, List<Map<String, Object>> candidates) {
        Set<String> validIndexes = new HashSet<>();

        Map<String, Map<String, Object>> candidateIndexMap = candidates
            .stream()
            .collect(Collectors.toMap(m -> (String) m.get(INDEX_NAME_FIELD), m -> m));

        Set<String> allCandidates = candidateIndexMap.keySet();
        List<String> predictedIndexes = Arrays.stream(result.split(",")).map(String::trim).collect(Collectors.toList());
        log.debug("all candidates are {}, predictedIndexes are {}", allCandidates, predictedIndexes);

        for (String predictedIndex : predictedIndexes) {
            if (allCandidates.contains(predictedIndex)) {
                Map<String, Object> map = candidateIndexMap.get(predictedIndex);
                // data stream back index
                Optional<Object> dataStreamName = Optional.ofNullable(map.get(DATA_STREAM_FIELD));
                List<String> patterns = (List<String>) map.get(INDEX_PATTERNS_FIELD);
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
        /*
         * we might need to add more check here,
         * for example if index name length is short, e.g.<test>, that should be good enough we don't need to get index pattern for it
         * if index pattern is too generic, that also not we want, we can discard the index pattern and use index itself.
         */
        if (!patterns.isEmpty()) {
            patterns.sort(Comparator.comparing(String::length).reversed());
            String longestPattern = patterns.get(0);
            // index pattern can't be * or too generic
            if (!longestPattern.equals("*")) {
                return longestPattern;
            }
        }
        return predictedIndex;
    }

    private Optional<String> findWithSimilarity(String predictedIndex, Collection<String> allIndexes) {
        LongestCommonSubsequence lcs = new LongestCommonSubsequence();
        return allIndexes.stream().map(index -> {
            CharSequence lcsResult = lcs.longestCommonSubsequence(index, predictedIndex);
            // ratio = 2.0 * M / T, M - matched characters, T - total characters
            Double ratio = (2.0 * lcsResult.length()) / (index.length() + predictedIndex.length());
            return Pair.of(index, ratio);
        }).filter(pair -> pair.getValue() > 0.9).max(Map.Entry.comparingByValue()).map(Pair::getKey);
    }

    private String buildFinalPrompt(String summaryString, String question) {
        Map<String, String> params = Map.of("question", question, "summaries", summaryString);
        return new StringSubstitutor(params).replace(prompt);
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
            LLMProvider llmProvider = Optional
                .ofNullable(params.get(LLM_PROVIDER))
                .map(it -> LLMProvider.fromProvider((String) it))
                .orElse(LLMProvider.NONE);
            String promptTemplate;
            if (params.get(PROMPT_TEMPLATE) == null) {
                try (InputStream ins = this.getClass().getResourceAsStream("/index_routing_tool_prompt.txt")) {
                    promptTemplate = Streams.readFully(Objects.requireNonNull(ins)).utf8ToString();
                } catch (IOException e) {
                    log.error("Can't find default prompt template for index routing tool");
                    throw new RuntimeException(e);
                }
            } else {
                promptTemplate = (String) params.get(PROMPT_TEMPLATE);
            }

            Integer docSize = params.containsKey(DOC_SIZE_FIELD) ? Integer.parseInt((String) params.get(DOC_SIZE_FIELD)) : DEFAULT_K;
            Integer k = params.containsKey(K_FIELD) ? Integer.parseInt((String) params.get(K_FIELD)) : DEFAULT_K;
            IndexRoutingTool tool = new IndexRoutingTool(client, xContentRegistry, docSize, k, embeddingModelId, inferenceModelId);
            tool.setPrompt(llmProvider.getPromptFormat().replace("${prompt}", promptTemplate));

            return tool;
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
