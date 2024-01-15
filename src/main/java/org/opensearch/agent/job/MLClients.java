/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.job;

import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.DEFAULT_TIMEOUT_SECOND;
import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.SENTENCE_EMBEDDING;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.agent.tools.IndexRoutingTool;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.ml.common.CommonValue;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.nlp.TextDocsMLInput;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.agent.MLSearchAgentAction;
import org.opensearch.ml.common.transport.deploy.MLDeployModelAction;
import org.opensearch.ml.common.transport.deploy.MLDeployModelRequest;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class MLClients {

    private Client client;

    private NamedXContentRegistry xContentRegistry;

    private ClusterService clusterService;

    public MLClients(Client client, NamedXContentRegistry xContentRegistry, ClusterService clusterService) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
    }

    public <T> T getEmbeddingResult(String modelId, List<String> texts, boolean deploy, Function<MLTaskResponse, T> parser) {
        try {
            TextDocsInputDataSet inputDataSet = TextDocsInputDataSet
                .builder()
                .docs(texts)
                .resultFilter(ModelResultFilter.builder().returnNumber(true).targetResponse(List.of(SENTENCE_EMBEDDING)).build())
                .build();

            TextDocsMLInput input = new TextDocsMLInput(FunctionName.TEXT_EMBEDDING, inputDataSet);
            MLPredictionTaskRequest request = new MLPredictionTaskRequest(modelId, input);

            ActionFuture<MLTaskResponse> predictFuture = client.execute(MLPredictionTaskAction.INSTANCE, request);
            MLTaskResponse mlTaskResponse = predictFuture.get(DEFAULT_TIMEOUT_SECOND, TimeUnit.SECONDS);
            return parser.apply(mlTaskResponse);
        } catch (Exception ex) {
            if (deploy && ExceptionsHelper.stackTrace(ex).contains("Model not ready yet.")) {
                log.info("Model {} has not deployed yet, try to deploy", modelId);
                ActionFuture<MLDeployModelResponse> deployFuture = client
                    .execute(MLDeployModelAction.INSTANCE, new MLDeployModelRequest(modelId, false));
                try {
                    MLDeployModelResponse mlDeployModelResponse = deployFuture.get(2, TimeUnit.MINUTES);
                    if (mlDeployModelResponse.getStatus().equals(MLTaskState.COMPLETED.name())) {
                        return getEmbeddingResult(modelId, texts, false, parser);
                    }
                } catch (Exception e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
            log.error("Invoke ML embedding failed", ex);
            throw ExceptionsHelper.convertToRuntime(ex);
        }
    }

    public void inference(String inferenceModelId, String prompt, ActionListener<MLTaskResponse> listener) {
        RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet
            .builder()
            .parameters(Collections.singletonMap("prompt", prompt))
            .build();
        MLPredictionTaskRequest request = new MLPredictionTaskRequest(
            inferenceModelId,
            MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build()
        );
        client.execute(MLPredictionTaskAction.INSTANCE, request, listener);
    }

    public void getModelIdsForIndexRoutingTool(List<String> adhocModelIds, ActionListener<List<String>> listener) {
        if (adhocModelIds != null && !adhocModelIds.isEmpty()) {
            listener.onResponse(adhocModelIds);
        } else {
            getModelIdsForIndexRoutingTool(listener);
        }
    }

    public void getModelIdsForIndexRoutingTool(ActionListener<List<String>> listener) {
        if (!clusterService.state().metadata().hasIndex(CommonValue.ML_AGENT_INDEX)) {
            listener.onResponse(Collections.emptyList());
            return;
        }
        // search agent with IndexRoutingTool
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        String termQueryKey = String.format(Locale.ROOT, "%s.%s", MLAgent.TOOLS_FIELD, MLToolSpec.TOOL_TYPE_FIELD);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(termQueryKey, IndexRoutingTool.TYPE);
        searchSourceBuilder.query(termQueryBuilder);
        SearchRequest searchRequest = Requests.searchRequest().source(searchSourceBuilder);

        client.execute(MLSearchAgentAction.INSTANCE, searchRequest, ActionListener.wrap(r -> {
            SearchHits hits = r.getHits();
            log.debug("total {} agent found with tool {}", hits.getTotalHits().value, IndexRoutingTool.TYPE);
            // no agent with IndexRoutingTool
            if (hits.getTotalHits().value == 0L) {
                listener.onResponse(Collections.emptyList());
            }
            Set<String> embeddingModelIds = new HashSet<>();
            for (SearchHit hit : hits) {
                embeddingModelIds.addAll(extractModelIdFromAgent(hit));
            }
            listener.onResponse(new ArrayList<>(embeddingModelIds));
        }, ex -> {
            if (ExceptionsHelper.unwrapCause(ex) instanceof IndexNotFoundException) {
                listener.onResponse(Collections.emptyList());
            } else {
                listener.onFailure(ex);
            }
        }));
    }

    private Set<String> extractModelIdFromAgent(SearchHit hit) {
        try (
            XContentParser parser = XContentHelper
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, hit.getSourceRef(), XContentType.JSON);
        ) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            MLAgent mlAgent = MLAgent.parse(parser);
            return mlAgent
                .getTools()
                .stream()
                .filter(mlToolSpec -> mlToolSpec.getType().equals(IndexRoutingTool.TYPE) && mlToolSpec.getParameters() != null)
                .map(MLToolSpec::getParameters)
                .map(params -> params.get(IndexRoutingTool.EMBEDDING_MODEL_ID))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        } catch (Exception e) {
            log.error("Failed to parse ml agent from {}", hit, e);
            return Set.of();
        }
    }
}
