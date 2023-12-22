/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.agent.tools.AbstractRetrieverTool.*;
import static org.opensearch.agent.tools.AbstractRetrieverToolTests.*;
import static org.opensearch.agent.tools.VectorDBTool.DEFAULT_K;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;

public class RAGToolTests {
    public static final String TEST_QUERY_TEXT = "hello?";
    public static final String TEST_EMBEDDING_FIELD = "test_embedding";
    public static final String TEST_EMBEDDING_MODEL_ID = "1234";
    public static final String TEST_INFERENCE_MODEL_ID = "1234";

    public static final String TEST_NEURAL_QUERY = "{\"query\":{\"neural\":{\""
        + TEST_EMBEDDING_FIELD
        + "\":{\"query_text\":\""
        + TEST_QUERY_TEXT
        + "\",\"model_id\":\""
        + TEST_EMBEDDING_MODEL_ID
        + "\",\"k\":"
        + DEFAULT_K
        + "}}}"
        + " }";;
    private RAGTool ragTool;
    private String mockedSearchResponseString;
    private String mockedEmptySearchResponseString;
    @Mock
    private Parser mockOutputParser;
    @Mock
    private Client client;
    @Mock
    private ActionListener<ModelTensorOutput> listener;
    private Map<String, Object> params;

    @Before
    @SneakyThrows
    public void setup() {
        try (InputStream searchResponseIns = AbstractRetrieverTool.class.getResourceAsStream("retrieval_tool_search_response.json")) {
            if (searchResponseIns != null) {
                mockedSearchResponseString = new String(searchResponseIns.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        try (InputStream searchResponseIns = AbstractRetrieverTool.class.getResourceAsStream("retrieval_tool_empty_search_response.json")) {
            if (searchResponseIns != null) {
                mockedEmptySearchResponseString = new String(searchResponseIns.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        client = mock(Client.class);
        listener = mock(ActionListener.class);
        RAGTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_QUERY);

        params = new HashMap<>();
        params.put(RAGTool.INDEX_FIELD, TEST_INDEX);
        params.put(RAGTool.EMBEDDING_FIELD, TEST_EMBEDDING_FIELD);
        params.put(RAGTool.SOURCE_FIELD, gson.toJson(TEST_SOURCE_FIELDS));
        params.put(RAGTool.EMBEDDING_MODEL_ID_FIELD, TEST_EMBEDDING_MODEL_ID);
        params.put(RAGTool.INFERENCE_MODEL_ID_FIELD, TEST_INFERENCE_MODEL_ID);
        params.put(RAGTool.DOC_SIZE_FIELD, AbstractRetrieverToolTests.TEST_DOC_SIZE.toString());
        params.put(VectorDBTool.K_FIELD, DEFAULT_K);
        ragTool = RAGTool.Factory.getInstance().create(params);
    }

    @Test
    public void testValidate() {
        assertTrue(ragTool.validate(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hi")));
        assertFalse(ragTool.validate(Map.of(AbstractRetrieverTool.INPUT_FIELD, "")));
        assertFalse(ragTool.validate(Map.of(AbstractRetrieverTool.INPUT_FIELD, " ")));
        assertFalse(ragTool.validate(Map.of("test", " ")));
        assertFalse(ragTool.validate(new HashMap<>()));
        assertFalse(ragTool.validate(null));
    }

    @Test
    public void testGetAttributes() {
        assertEquals(ragTool.getVersion(), null);
        assertEquals(ragTool.getType(), RAGTool.TYPE);
        assertEquals(ragTool.getIndex(), TEST_INDEX);
        assertEquals(ragTool.getDocSize(), TEST_DOC_SIZE);
        assertEquals(ragTool.getSourceFields(), TEST_SOURCE_FIELDS);
        assertEquals(ragTool.getEmbeddingField(), TEST_EMBEDDING_FIELD);
        assertEquals(ragTool.getEmbeddingModelId(), TEST_EMBEDDING_MODEL_ID);
        assertEquals(ragTool.getK(), DEFAULT_K);
        assertEquals(ragTool.getInferenceModelId(), TEST_INFERENCE_MODEL_ID);
    }

    @Test
    public void testSetName() {
        assertEquals(ragTool.getName(), RAGTool.TYPE);
        ragTool.setName("test-tool");
        assertEquals(ragTool.getName(), "test-tool");
    }

    @Test
    public void testGetQueryBodySuccess() {
        assertEquals(ragTool.getQueryBody(TEST_QUERY_TEXT), TEST_QUERY_TEXT);
    }

    @Test
    public void testOutputParser() throws IOException {

        NamedXContentRegistry mockNamedXContentRegistry = getNeuralQueryNamedXContentRegistry();
        ragTool.setXContentRegistry(mockNamedXContentRegistry);

        ModelTensorOutput mlModelTensorOutput = getMlModelTensorOutput();
        SearchResponse mockedSearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, mockedSearchResponseString)
            );

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedSearchResponse);
            return null;
        }).when(client).search(any(), any());

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        ragTool.setOutputParser(mockOutputParser);
        ragTool.run(Map.of(INPUT_FIELD, "hello?"), listener);

        verify(client).search(any(), any());
        verify(client).execute(any(), any(), any());
    }

    @Test
    public void testRunWithEmptySearchResponse() throws IOException {
        NamedXContentRegistry mockNamedXContentRegistry = getNeuralQueryNamedXContentRegistry();
        ragTool.setXContentRegistry(mockNamedXContentRegistry);

        ModelTensorOutput mlModelTensorOutput = getMlModelTensorOutput();
        SearchResponse mockedEmptySearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, mockedEmptySearchResponseString)
            );

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedEmptySearchResponse);
            return null;
        }).when(client).search(any(), any());

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
        ragTool.run(Map.of(INPUT_FIELD, "hello?"), listener);
        verify(client).search(any(), any());
        verify(client).execute(any(), any(), any());
    }

    @Test
    @SneakyThrows
    public void testRunWithRuntimeExceptionDuringSearch() {
        NamedXContentRegistry mockNamedXContentRegistry = getNeuralQueryNamedXContentRegistry();
        ragTool.setXContentRegistry(mockNamedXContentRegistry);
        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new RuntimeException("Failed to search index"));
            return null;
        }).when(client).search(any(), any());
        ragTool.run(Map.of(INPUT_FIELD, "hello?"), listener);
        verify(listener).onFailure(any(RuntimeException.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to search index", argumentCaptor.getValue().getMessage());
    }

    @Test
    @SneakyThrows
    public void testRunWithRuntimeExceptionDuringExecute() {
        NamedXContentRegistry mockNamedXContentRegistry = getNeuralQueryNamedXContentRegistry();
        ragTool.setXContentRegistry(mockNamedXContentRegistry);

        SearchResponse mockedSearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, mockedSearchResponseString)
            );

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedSearchResponse);
            return null;
        }).when(client).search(any(), any());

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException("Failed to run model " + TEST_INFERENCE_MODEL_ID));
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        ragTool.run(Map.of(INPUT_FIELD, "hello?"), listener);
        verify(listener).onFailure(any(RuntimeException.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to run model " + TEST_INFERENCE_MODEL_ID, argumentCaptor.getValue().getMessage());
    }

    @Test
    public void testRunWithEmptyInput() throws IOException {
        ActionListener listener = mock(ActionListener.class);
        ragTool.run(Map.of(INPUT_FIELD, ""), listener);
        verify(listener).onFailure(any(RuntimeException.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertEquals("[" + INPUT_FIELD + "] is null or empty, can not process it.", argumentCaptor.getValue().getMessage());

    }

    @Test
    public void testFactory() {
        RAGTool.Factory factoryMock = new RAGTool.Factory();
        RAGTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_QUERY);
        factoryMock.init(client, TEST_XCONTENT_REGISTRY_FOR_QUERY);

        String defaultDescription = factoryMock.getDefaultDescription();
        assertEquals(RAGTool.DEFAULT_DESCRIPTION, defaultDescription);
        assertNotNull(RAGTool.Factory.getInstance());
        RAGTool rAGtool1 = factoryMock.create(params);

        RAGTool rAGtool2 = new RAGTool(
            client,
            TEST_XCONTENT_REGISTRY_FOR_QUERY,
            TEST_INDEX,
            TEST_EMBEDDING_FIELD,
            TEST_SOURCE_FIELDS,
            DEFAULT_K,
            TEST_DOC_SIZE,
            TEST_EMBEDDING_MODEL_ID,
            TEST_INFERENCE_MODEL_ID
        );

        assertEquals(rAGtool1.getClient(), rAGtool2.getClient());
        assertEquals(rAGtool1.getK(), rAGtool2.getK());
        assertEquals(rAGtool1.getInferenceModelId(), rAGtool2.getInferenceModelId());
        assertEquals(rAGtool1.getName(), rAGtool2.getName());
        assertEquals(rAGtool1.getDocSize(), rAGtool2.getDocSize());
        assertEquals(rAGtool1.getIndex(), rAGtool2.getIndex());
        assertEquals(rAGtool1.getEmbeddingModelId(), rAGtool2.getEmbeddingModelId());
        assertEquals(rAGtool1.getEmbeddingField(), rAGtool2.getEmbeddingField());
        assertEquals(rAGtool1.getSourceFields(), rAGtool2.getSourceFields());
        assertEquals(rAGtool1.getXContentRegistry(), rAGtool2.getXContentRegistry());

    }

    private static NamedXContentRegistry getNeuralQueryNamedXContentRegistry() {
        QueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();

        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        NamedXContentRegistry.Entry entry = new NamedXContentRegistry.Entry(QueryBuilder.class, new ParseField("neural"), (p, c) -> {
            p.map();
            return matchAllQueryBuilder;
        });
        entries.add(entry);
        NamedXContentRegistry mockNamedXContentRegistry = new NamedXContentRegistry(entries);
        return mockNamedXContentRegistry;
    }

    private static ModelTensorOutput getMlModelTensorOutput() {
        ModelTensor modelTensor = ModelTensor.builder().dataAsMap(ImmutableMap.of("thought", "thought 1", "action", "action1")).build();
        ModelTensors modelTensors = ModelTensors.builder().mlModelTensors(Arrays.asList(modelTensor)).build();
        ModelTensorOutput mlModelTensorOutput = ModelTensorOutput.builder().mlModelOutputs(Arrays.asList(modelTensors)).build();
        return mlModelTensorOutput;
    }
}
