/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;

import lombok.SneakyThrows;

public class RCAToolTests {
    @Mock
    private Client client;
    @Mock
    private ActionListener<ModelTensorOutput> listener;
    @Mock
    private MLTaskResponse mlTaskResponse;
    @Mock
    private ModelTensorOutput modelTensorOutput;
    @Mock
    private ModelTensors modelTensors;

    private Map<String, Object> params;
    private RCATool rcaTool;

    @Before
    @SneakyThrows
    public void setup() {
        MockitoAnnotations.openMocks(this);
        RCATool.Factory.getInstance().init(client);
        rcaTool = RCATool.Factory.getInstance().create(Map.of("model_id", "model_id"));
    }

    private void initMLTensors(String response) {
        Map<String, ?> modelReturns = Collections.singletonMap("response", response);
        initMLTensors(modelReturns);
    }

    private void initMLTensors(Map<String, ?> modelReturns) {
        ModelTensor modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        when(modelTensors.getMlModelTensors()).thenReturn(Collections.singletonList(modelTensor));
        when(modelTensorOutput.getMlModelOutputs()).thenReturn(Collections.singletonList(modelTensors));
        when(mlTaskResponse.getOutput()).thenReturn(modelTensorOutput);

        // call model
        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            listener.onResponse(mlTaskResponse);
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
    }

    @Test
    public void testRCATool() {
        initMLTensors("hello");
        rcaTool
            .run(
                Map.of("index", "mock_indices", "primary", "true"),
                ActionListener.<String>wrap(response -> assertEquals("hello", response), e -> fail("Tool runs failed: " + e.getMessage()))
            );

    }
}
