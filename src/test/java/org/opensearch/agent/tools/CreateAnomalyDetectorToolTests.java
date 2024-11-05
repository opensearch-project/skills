/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.CommonValue.ML_CONNECTOR_INDEX;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;

import com.google.common.collect.ImmutableMap;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class CreateAnomalyDetectorToolTests {
    @Mock
    private Client client;
    @Mock
    private AdminClient adminClient;
    @Mock
    private IndicesAdminClient indicesAdminClient;
    @Mock
    private GetMappingsResponse getMappingsResponse;
    @Mock
    private MappingMetadata mappingMetadata;
    private Map<String, MappingMetadata> mockedMappings;
    private Map<String, Object> indexMappings;

    @Mock
    private MLTaskResponse mlTaskResponse;
    @Mock
    private ModelTensorOutput modelTensorOutput;
    @Mock
    private ModelTensors modelTensors;

    private ModelTensor modelTensor;

    private Map<String, ?> modelReturns;

    private String mockedIndexName = "http_logs";
    private String mockedResponse = "{category_field=|aggregation_field=response,responseLatency|aggregation_method=count,avg}";
    private String mockedResult =
        "{\"index\":\"http_logs\",\"categoryField\":\"\",\"aggregationField\":\"response,responseLatency\",\"aggregationMethod\":\"count,avg\",\"dateFields\":\"date\"}";

    private String mockedResultForIndexPattern =
        "{\"index\":\"http_logs\",\"categoryField\":\"\",\"aggregationField\":\"response,responseLatency\",\"aggregationMethod\":\"count,avg\",\"dateFields\":\"date\"}";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        createMappings();
        // get mapping
        when(mappingMetadata.getSourceAsMap()).thenReturn(indexMappings);
        when(getMappingsResponse.getMappings()).thenReturn(mockedMappings);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        doAnswer(invocation -> {
            ActionListener<GetMappingsResponse> listener = (ActionListener<GetMappingsResponse>) invocation.getArguments()[1];
            listener.onResponse(getMappingsResponse);
            return null;
        }).when(indicesAdminClient).getMappings(any(), any());

        initMLTensors();
        CreateAnomalyDetectorTool.Factory.getInstance().init(client);
    }

    @Test
    public void testModelIdIsNullOrEmpty() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> CreateAnomalyDetectorTool.Factory.getInstance().create(ImmutableMap.of("model_id", ""))
        );
        assertEquals("model_id cannot be empty.", exception.getMessage());
    }

    @Test
    public void testModelType() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> CreateAnomalyDetectorTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "model_type", "unknown"))
        );
        assertEquals("Unsupported model_type: unknown", exception.getMessage());

        CreateAnomalyDetectorTool tool = CreateAnomalyDetectorTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "model_type", "openai"));
        assertEquals(CreateAnomalyDetectorTool.TYPE, tool.getName());
        assertEquals("modelId", tool.getModelId());
        assertEquals("OPENAI", tool.getModelType().toString());

        tool = CreateAnomalyDetectorTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "model_type", "claude"));
        assertEquals(CreateAnomalyDetectorTool.TYPE, tool.getName());
        assertEquals("modelId", tool.getModelId());
        assertEquals("CLAUDE", tool.getModelType().toString());
    }

    @Test
    public void testTool() {
        CreateAnomalyDetectorTool tool = CreateAnomalyDetectorTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId"));
        assertEquals(CreateAnomalyDetectorTool.TYPE, tool.getName());
        assertEquals("modelId", tool.getModelId());
        assertEquals("CLAUDE", tool.getModelType().toString());

        tool
            .run(
                ImmutableMap.of("index", mockedIndexName),
                ActionListener.<String>wrap(response -> assertEquals(mockedResult, response), log::info)
            );
        tool
            .run(
                ImmutableMap.of("index", mockedIndexName + "*"),
                ActionListener.<String>wrap(response -> assertEquals(mockedResultForIndexPattern, response), log::info)
            );
        tool
            .run(
                ImmutableMap.of("input", mockedIndexName),
                ActionListener.<String>wrap(response -> assertEquals(mockedResult, response), log::info)
            );
        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("index", mockedIndexName))),
                ActionListener.<String>wrap(response -> assertEquals(mockedResult, response), log::info)
            );
    }

    @Test
    public void testToolWithInvalidResponse() {
        CreateAnomalyDetectorTool tool = CreateAnomalyDetectorTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId"));

        modelReturns = Collections.singletonMap("response", "");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        Exception exception = assertThrows(
            IllegalStateException.class,
            () -> tool
                .run(ImmutableMap.of("index", mockedIndexName), ActionListener.<String>wrap(response -> assertEquals(response, ""), e -> {
                    throw new IllegalStateException(e.getMessage());
                }))
        );
        assertEquals("Remote endpoint fails to inference, no response found.", exception.getMessage());

        modelReturns = Collections.singletonMap("response", "not valid response");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        exception = assertThrows(
            IllegalStateException.class,
            () -> tool
                .run(
                    ImmutableMap.of("index", mockedIndexName),
                    ActionListener.<String>wrap(response -> assertEquals(response, "not valid response"), e -> {
                        throw new IllegalStateException(e.getMessage());
                    })
                )
        );
        assertEquals(
            "The inference result from remote endpoint is not valid, cannot extract the key information from the result.",
            exception.getMessage()
        );

        modelReturns = Collections.singletonMap("response", null);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        exception = assertThrows(
            IllegalStateException.class,
            () -> tool
                .run(ImmutableMap.of("index", mockedIndexName), ActionListener.<String>wrap(response -> assertEquals(response, ""), e -> {
                    throw new IllegalStateException(e.getMessage());
                }))
        );
        assertEquals("Remote endpoint fails to inference, no response found.", exception.getMessage());
    }

    @Test
    public void testToolWithSystemIndex() {
        CreateAnomalyDetectorTool tool = CreateAnomalyDetectorTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId"));
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> tool.run(ImmutableMap.of("index", ML_CONNECTOR_INDEX), ActionListener.<String>wrap(result -> {}, e -> {}))
        );
        assertEquals(
            "CreateAnomalyDetectionTool doesn't support searching indices starting with '.' since it could be system index, current searching index name: "
                + ML_CONNECTOR_INDEX,
            exception.getMessage()
        );
    }

    @Test
    public void testToolWithGetMappingFailed() {
        CreateAnomalyDetectorTool tool = CreateAnomalyDetectorTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId"));
        doAnswer(invocation -> {
            ActionListener<GetMappingsResponse> listener = (ActionListener<GetMappingsResponse>) invocation.getArguments()[1];
            listener.onFailure(new Exception("No mapping found for the index: " + mockedIndexName));
            return null;
        }).when(indicesAdminClient).getMappings(any(), any());

        tool.run(ImmutableMap.of("index", mockedIndexName), ActionListener.<String>wrap(result -> {}, e -> {
            assertEquals("No mapping found for the index: " + mockedIndexName, e.getMessage());
        }));
    }

    @Test
    public void testToolWithPredictModelFailed() {
        CreateAnomalyDetectorTool tool = CreateAnomalyDetectorTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId"));
        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            listener.onFailure(new Exception("predict model failed"));
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        tool.run(ImmutableMap.of("index", mockedIndexName), ActionListener.<String>wrap(result -> {}, e -> {
            assertEquals("predict model failed", e.getMessage());
        }));
    }

    @Test
    public void testToolWithCustomPrompt() {
        CreateAnomalyDetectorTool tool = CreateAnomalyDetectorTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "custom prompt"));
        assertEquals(CreateAnomalyDetectorTool.TYPE, tool.getName());
        assertEquals("modelId", tool.getModelId());
        assertEquals("CLAUDE", tool.getModelType().toString());
        assertEquals("custom prompt", tool.getContextPrompt());

        tool
            .run(
                ImmutableMap.of("index", mockedIndexName),
                ActionListener.<String>wrap(response -> assertEquals(mockedResult, response), log::info)
            );
    }

    private void createMappings() {
        indexMappings = new HashMap<>();
        indexMappings
            .put(
                "properties",
                ImmutableMap
                    .of(
                        "response",
                        ImmutableMap.of("type", "integer"),
                        "responseLatency",
                        ImmutableMap.of("type", "float"),
                        "date",
                        ImmutableMap.of("type", "date")
                    )
            );
        mockedMappings = new HashMap<>();
        mockedMappings.put(mockedIndexName, mappingMetadata);

        modelReturns = Collections.singletonMap("response", mockedResponse);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
    }

    private void initMLTensors() {
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
}
