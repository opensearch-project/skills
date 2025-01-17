/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.utils.StringUtils.gson;
import static org.opensearch.ml.common.utils.StringUtils.isJson;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;

import com.google.common.collect.ImmutableMap;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class CreateAlertToolTests {
    private final Client client = mock(Client.class);
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
    @Mock
    private ActionFuture<GetIndexResponse> actionFuture;
    @Mock
    private GetIndexResponse getIndexResponse;

    private final String jsonResponse = "{\"name\":\"mocked_response\"}";
    private final String mockedIndexName = "mocked_index_name";
    private final String mockedIndices = String.format("[%s]", mockedIndexName);
    private CreateAlertTool tool;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        MockitoAnnotations.openMocks(this);
        createMappings();
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        doAnswer(invocation -> {
            ActionListener<GetIndexResponse> listener = (ActionListener<GetIndexResponse>) invocation.getArguments()[1];
            listener.onResponse(getIndexResponse);
            return null;
        }).when(indicesAdminClient).getIndex(any(), any());

        when(getIndexResponse.indices()).thenReturn(new String[] { mockedIndexName });
        when(getIndexResponse.mappings()).thenReturn(mockedMappings);
        when(mappingMetadata.getSourceAsMap()).thenReturn(indexMappings);

        CreateAlertTool.Factory.getInstance().init(client);
        tool = CreateAlertTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId"));
        assertEquals(CreateAlertTool.TYPE, tool.getName());
    }

    private void createMappings() {
        indexMappings = new HashMap<>();
        indexMappings
            .put(
                "properties",
                ImmutableMap
                    .of(
                        "field1",
                        ImmutableMap.of("type", "integer"),
                        "field2",
                        ImmutableMap.of("type", "float"),
                        "field3",
                        ImmutableMap.of("type", "date")
                    )
            );
        mockedMappings = new HashMap<>();
        mockedMappings.put(mockedIndexName, mappingMetadata);
    }

    private void initMLTensors(String response) {
        Map<String, ?> modelReturns = Collections.singletonMap("response", response);
        initMLTensors(modelReturns);
    }

    private void initMLTensorsWithoutResponse(String response) {
        assert (isJson(response));
        Map<String, ?> modelReturns = gson.fromJson(response, Map.class);
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
    public void testTool_WithoutModelId() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> CreateAlertTool.Factory.getInstance().create(Collections.emptyMap())
        );
        assertEquals("model_id cannot be null or blank.", exception.getMessage());
    }

    @Test
    public void testTool_WithBlankModelId() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> CreateAlertTool.Factory.getInstance().create(ImmutableMap.of("model_id", "   "))
        );
        assertEquals("model_id cannot be null or blank.", exception.getMessage());
    }

    @Test
    public void testTool_WithNonSupportedModelType() {
        CreateAlertTool alertTool = CreateAlertTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "model_type", "non_supported_modelType"));
        assertEquals("CLAUDE", alertTool.getModelType());
    }

    @Test
    public void testTool_WithEmptyModelType() {
        CreateAlertTool alertTool = CreateAlertTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "model_type", ""));
        assertEquals("CLAUDE", alertTool.getModelType());
    }

    @Test
    public void testToolWithCustomPrompt() {
        CreateAlertTool tool = CreateAlertTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "custom prompt"));
        assertEquals(CreateAlertTool.TYPE, tool.getName());
        assertEquals("modelId", tool.getModelId());
        assertEquals("custom prompt", tool.getToolPrompt());

        tool
            .run(
                ImmutableMap.of("indices", mockedIndexName),
                ActionListener.<String>wrap(response -> assertEquals(jsonResponse, response), log::info)
            );
    }

    @Test
    public void testTool() {
        // test json response
        initMLTensors(jsonResponse);
        tool
            .run(
                ImmutableMap.of("indices", mockedIndices, "question", "test_question"),
                ActionListener
                    .<String>wrap(response -> assertEquals(jsonResponse, response), e -> fail("Tool runs failed: " + e.getMessage()))
            );

        // test text response wrapping json
        final String textResponseWithJson = String.format("RESPONSE_HEADER\n Tool output: ```json%s```, RESPONSE_FOOTER\n", jsonResponse);
        initMLTensors(textResponseWithJson);
        tool
            .run(
                ImmutableMap.of("indices", mockedIndices, "question", "test_question"),
                ActionListener
                    .<String>wrap(response -> assertEquals(jsonResponse, response), e -> fail("Tool runs failed: " + e.getMessage()))
            );

        // test tensor result without a string response but a json object directly.
        initMLTensorsWithoutResponse(jsonResponse);
        tool
            .run(
                ImmutableMap.of("indices", mockedIndices, "question", "test_question"),
                ActionListener
                    .<String>wrap(response -> assertEquals(jsonResponse, response), e -> fail("Tool runs failed: " + e.getMessage()))
            );
    }

    @Test
    public void testToolWithIndicesNotInJsonFormat() {
        // test indices no in json format
        initMLTensors(jsonResponse);
        tool
            .run(
                ImmutableMap.of("indices", mockedIndexName, "question", "test_question"),
                ActionListener
                    .<String>wrap(response -> assertEquals(jsonResponse, response), e -> fail("Tool runs failed: " + e.getMessage()))
            );

        tool
            .run(
                ImmutableMap.of("indices", mockedIndexName + "," + mockedIndexName, "question", "test_question"),
                ActionListener
                    .<String>wrap(response -> assertEquals(jsonResponse, response), e -> fail("Tool runs failed: " + e.getMessage()))
            );

    }

    @Test
    public void testToolWithNoJsonResponse() {
        String noJsonResponse = "No json response";
        initMLTensors(noJsonResponse);
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> tool
                .run(
                    ImmutableMap.of("indices", mockedIndices, "question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(noJsonResponse, response), e -> {
                        throw new IllegalArgumentException(e.getMessage());
                    })
                )
        );
        assertEquals(String.format("The response from LLM is not a json: [%s]", noJsonResponse), exception.getMessage());

        final String textResponseWithJson = String.format("RESPONSE_HEADER\n Tool output: ```json%s```, RESPONSE_FOOTER\n", noJsonResponse);
        initMLTensors(textResponseWithJson);
        Exception exception2 = assertThrows(
            IllegalArgumentException.class,
            () -> tool
                .run(
                    ImmutableMap.of("indices", mockedIndices, "question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(noJsonResponse, response), e -> {
                        throw new IllegalArgumentException(e.getMessage());
                    })
                )
        );
        assertEquals(String.format("The response from LLM is not a json: [%s]", noJsonResponse), exception2.getMessage());

    }

    @Test
    public void testToolWithPredictModelFailed() {
        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            listener.onFailure(new Exception("Failed to predict"));
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        Exception exception = assertThrows(
            RuntimeException.class,
            () -> tool
                .run(
                    ImmutableMap.of("indices", mockedIndices, "question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(jsonResponse, response), e -> {
                        throw new RuntimeException(e.getMessage());
                    })
                )
        );
        assertEquals("Failed to predict", exception.getMessage());
    }

    @Test
    public void testToolWithIllegalIndices() {
        // no indices in input parameters
        Exception exception = assertThrows(
            RuntimeException.class,
            () -> tool
                .run(
                    ImmutableMap.of("question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(jsonResponse, response), e -> {
                        throw new RuntimeException(e.getMessage());
                    })
                )
        );
        assertEquals(
            "No indices in the input parameter. Ask user to provide index as your final answer directly without using any other tools",
            exception.getMessage()
        );

        // empty string as indices
        exception = assertThrows(
            RuntimeException.class,
            () -> tool
                .run(
                    ImmutableMap.of("indices", "", "question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(jsonResponse, response), e -> {
                        throw new RuntimeException(e.getMessage());
                    })
                )
        );
        assertEquals(
            "No indices in the input parameter. Ask user to provide index as your final answer directly without using any other tools",
            exception.getMessage()
        );

        // indices is an empty list
        exception = assertThrows(
            RuntimeException.class,
            () -> tool
                .run(
                    ImmutableMap.of("indices", "[]", "question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(jsonResponse, response), e -> {
                        throw new RuntimeException(e.getMessage());
                    })
                )
        );
        assertEquals(
            "The input indices is empty. Ask user to provide index as your final answer directly without using any other tools",
            exception.getMessage()
        );

        // indices contain system index
        exception = assertThrows(
            RuntimeException.class,
            () -> tool
                .run(
                    ImmutableMap.of("indices", "[.kibana]", "question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(jsonResponse, response), e -> {
                        throw new RuntimeException(e.getMessage());
                    })
                )
        );
        assertEquals(
            "The provided indices [[.kibana]] contains system index, which is not allowed. Ask user to check the provided indices as your final answer without using any other.",
            exception.getMessage()
        );

        // Cannot find provided indices in opensearch
        when(getIndexResponse.indices()).thenReturn(new String[] {});
        exception = assertThrows(
            RuntimeException.class,
            () -> tool
                .run(
                    ImmutableMap.of("indices", "[non_existed_index]", "question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(jsonResponse, response), e -> {
                        throw new RuntimeException(e.getMessage());
                    })
                )
        );
        assertEquals(
            "Cannot find provided indices [non_existed_index]. Ask user to check the provided indices as your final answer without using any other tools",
            exception.getMessage()
        );

        doAnswer(invocation -> {
            ActionListener<GetIndexResponse> listener = (ActionListener<GetIndexResponse>) invocation.getArguments()[1];
            listener.onFailure(new IndexNotFoundException("no such index"));
            return null;
        }).when(indicesAdminClient).getIndex(any(), any());

        exception = assertThrows(
            RuntimeException.class,
            () -> tool
                .run(
                    ImmutableMap.of("indices", "[non_existed_index]", "question", "test_question"),
                    ActionListener.<String>wrap(response -> assertEquals(jsonResponse, response), e -> {
                        throw new RuntimeException(e.getMessage());
                    })
                )
        );
        assertEquals(
            "Cannot find provided indices [non_existed_index]. Ask user to check the provided indices as your final answer without using any other tools",
            exception.getMessage()
        );
    }
}
