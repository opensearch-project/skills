/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.job;

import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.DEFAULT_TIMEOUT_SECOND;
import static org.opensearch.agent.job.IndexSummaryEmbeddingJob.SENTENCE_EMBEDDING;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.opensearch.client.Client;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.input.nlp.TextDocsMLInput;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class MLClients {

    private Client client;

    public MLClients(Client client) {
        this.client = client;
    }

    public <T> T getEmbeddingResult(String modelId, List<String> texts, Function<MLTaskResponse, T> parser) {
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
            log.error("Invoke ML embedding failed", ex);
            throw new RuntimeException(ex);
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
}
