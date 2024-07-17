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
    public static final String TEST_NEURAL_QUERY_TYPE = "neural";
    public static final String TEST_NEURAL_SPARSE_QUERY_TYPE = "neural_sparse";
    public static final String TEST_NESTED_PATH = "nested_path";

    static public final NamedXContentRegistry TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY = getQueryNamedXContentRegistry();
    private RAGTool ragTool;
    private String mockedSearchResponseString;
    private String mockedEmptySearchResponseString;
    private String mockedNeuralSparseSearchResponseString;
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

        try (InputStream searchResponseIns = AbstractRetrieverTool.class.getResourceAsStream("neural_sparse_tool_search_response.json")) {
            if (searchResponseIns != null) {
                mockedNeuralSparseSearchResponseString = new String(searchResponseIns.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        client = mock(Client.class);
        listener = mock(ActionListener.class);
        RAGTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);
        VectorDBTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);
        NeuralSparseSearchTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);
        params = new HashMap<>();
        params.put(RAGTool.INDEX_FIELD, TEST_INDEX);
        params.put(RAGTool.EMBEDDING_FIELD, TEST_EMBEDDING_FIELD);
        params.put(RAGTool.SOURCE_FIELD, gson.toJson(TEST_SOURCE_FIELDS));
        params.put(RAGTool.EMBEDDING_MODEL_ID_FIELD, TEST_EMBEDDING_MODEL_ID);
        params.put(RAGTool.INFERENCE_MODEL_ID_FIELD, TEST_INFERENCE_MODEL_ID);
        params.put(RAGTool.DOC_SIZE_FIELD, AbstractRetrieverToolTests.TEST_DOC_SIZE.toString());
        params.put(RAGTool.K_FIELD, DEFAULT_K.toString());
        params.put(RAGTool.QUERY_TYPE, TEST_NEURAL_QUERY_TYPE);
        params.put(RAGTool.CONTENT_GENERATION_FIELD, "true");
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
        assertEquals(ragTool.getInferenceModelId(), TEST_INFERENCE_MODEL_ID);
    }

    @Test
    public void testSetName() {
        assertEquals(ragTool.getName(), RAGTool.TYPE);
        ragTool.setName("test-tool");
        assertEquals(ragTool.getName(), "test-tool");
    }

    @Test
    public void testOutputParser() throws IOException {

        NamedXContentRegistry mockNamedXContentRegistry = getQueryNamedXContentRegistry();
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
        ragTool.run(Map.of(INPUT_FIELD, TEST_QUERY_TEXT), listener);

        verify(client).search(any(), any());
        verify(client).execute(any(), any(), any());
    }

    @Test
    public void testRunWithEmptySearchResponse() throws IOException {
        NamedXContentRegistry mockNamedXContentRegistry = getQueryNamedXContentRegistry();
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
        ragTool.run(Map.of(INPUT_FIELD, TEST_QUERY_TEXT), listener);
        verify(client).search(any(), any());
        verify(client).execute(any(), any(), any());
    }

    @Test
    public void testRunWithNeuralSparseQueryType() throws IOException {

        Map<String, Object> paramsWithNeuralSparse = new HashMap<>(params);
        paramsWithNeuralSparse.put(RAGTool.QUERY_TYPE, TEST_NEURAL_SPARSE_QUERY_TYPE);

        RAGTool rAGtoolWithNeuralSparseQuery = RAGTool.Factory.getInstance().create(paramsWithNeuralSparse);

        NamedXContentRegistry mockNamedXContentRegistry = getQueryNamedXContentRegistry();
        rAGtoolWithNeuralSparseQuery.setXContentRegistry(mockNamedXContentRegistry);

        ModelTensorOutput mlModelTensorOutput = getMlModelTensorOutput();
        SearchResponse mockedNeuralSparseSearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.IGNORE_DEPRECATIONS,
                        mockedNeuralSparseSearchResponseString
                    )
            );

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedNeuralSparseSearchResponse);
            return null;
        }).when(client).search(any(), any());

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(MLTaskResponse.builder().output(mlModelTensorOutput).build());
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
        rAGtoolWithNeuralSparseQuery.run(Map.of(INPUT_FIELD, TEST_QUERY_TEXT), listener);
        verify(client).search(any(), any());
        verify(client).execute(any(), any(), any());
    }

    @Test
    public void testRunWithInvalidQueryType() throws IOException {

        RAGTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);
        Map<String, Object> paramsWithInvalidQueryType = new HashMap<>(params);
        paramsWithInvalidQueryType.put(RAGTool.QUERY_TYPE, "sparse");
        try {
            RAGTool rAGtoolWithInvalidQueryType = RAGTool.Factory.getInstance().create(paramsWithInvalidQueryType);
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to read queryType, please input neural_sparse or neural.", e.getMessage());
        }

    }

    @Test
    public void testRunWithQuestionJson() throws IOException {
        NamedXContentRegistry mockNamedXContentRegistry = getQueryNamedXContentRegistry();
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
        ragTool.run(Map.of(INPUT_FIELD, "{question:'what is the population in seattle?'}"), listener);
        verify(client).search(any(), any());
        verify(client).execute(any(), any(), any());
    }

    @Test
    public void testRunEmptyResponseWithNotEnableContentGeneration() throws IOException {
        ActionListener<String> mockListener = mock(ActionListener.class);
        Map<String, Object> paramsWithNotEnableContentGeneration = new HashMap<>(params);
        paramsWithNotEnableContentGeneration.put(RAGTool.CONTENT_GENERATION_FIELD, "false");

        RAGTool rAGtoolWithNotEnableContentGeneration = RAGTool.Factory.getInstance().create(paramsWithNotEnableContentGeneration);

        NamedXContentRegistry mockNamedXContentRegistry = getQueryNamedXContentRegistry();
        rAGtoolWithNotEnableContentGeneration.setXContentRegistry(mockNamedXContentRegistry);

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
        rAGtoolWithNotEnableContentGeneration.run(Map.of(INPUT_FIELD, "{question:'what is the population in seattle?'}"), mockListener);

        verify(client).search(any(), any());
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockListener).onResponse(responseCaptor.capture());
        assertEquals("Can not get any match from search result.", responseCaptor.getValue());

    }

    @Test
    public void testRunResponseWithNotEnableContentGeneration() throws IOException {
        ActionListener<String> mockListener = mock(ActionListener.class);
        Map<String, Object> paramsWithNotEnableContentGeneration = new HashMap<>(params);
        paramsWithNotEnableContentGeneration.put(RAGTool.CONTENT_GENERATION_FIELD, "false");

        RAGTool rAGtoolWithNotEnableContentGeneration = RAGTool.Factory.getInstance().create(paramsWithNotEnableContentGeneration);

        NamedXContentRegistry mockNamedXContentRegistry = getQueryNamedXContentRegistry();
        rAGtoolWithNotEnableContentGeneration.setXContentRegistry(mockNamedXContentRegistry);

        SearchResponse mockedNeuralSparseSearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.IGNORE_DEPRECATIONS,
                        mockedNeuralSparseSearchResponseString
                    )
            );

        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedNeuralSparseSearchResponse);
            return null;
        }).when(client).search(any(), any());
        rAGtoolWithNotEnableContentGeneration.run(Map.of(INPUT_FIELD, "{question:'what is the population in seattle?'}"), mockListener);

        verify(client).search(any(), any());
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockListener).onResponse(responseCaptor.capture());
        assertEquals(
            "{\"_index\":\"my-nlp-index\",\"_source\":{\"passage_text\":\"Hello world\",\"passage_embedding\":{\"!\":0.8708904,\"door\":0.8587369,\"hi\":2.3929274,\"worlds\":2.7839446,\"yes\":0.75845814,\"##world\":2.5432441,\"born\":0.2682308,\"nothing\":0.8625516,\"goodbye\":0.17146169,\"greeting\":0.96817183,\"birth\":1.2788506,\"come\":0.1623208,\"global\":0.4371151,\"it\":0.42951578,\"life\":1.5750692,\"thanks\":0.26481047,\"world\":4.7300377,\"tiny\":0.5462298,\"earth\":2.6555297,\"universe\":2.0308156,\"worldwide\":1.3903781,\"hello\":6.696973,\"so\":0.20279501,\"?\":0.67785245},\"id\":\"s1\"},\"_id\":\"1\",\"_score\":30.0029}\n"
                + "{\"_index\":\"my-nlp-index\",\"_source\":{\"passage_text\":\"Hi planet\",\"passage_embedding\":{\"hi\":4.338913,\"planets\":2.7755864,\"planet\":5.0969057,\"mars\":1.7405145,\"earth\":2.6087382,\"hello\":3.3210192},\"id\":\"s2\"},\"_id\":\"2\",\"_score\":16.480486}\n",
            responseCaptor.getValue()
        );

    }

    @Test
    @SneakyThrows
    public void testRunWithRuntimeExceptionDuringSearch() {
        NamedXContentRegistry mockNamedXContentRegistry = getQueryNamedXContentRegistry();
        ragTool.setXContentRegistry(mockNamedXContentRegistry);
        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new RuntimeException("Failed to search index"));
            return null;
        }).when(client).search(any(), any());
        ragTool.run(Map.of(INPUT_FIELD, TEST_QUERY_TEXT), listener);
        verify(listener).onFailure(any(RuntimeException.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to search index", argumentCaptor.getValue().getMessage());
    }

    @Test
    @SneakyThrows
    public void testRunWithRuntimeExceptionDuringExecute() {
        NamedXContentRegistry mockNamedXContentRegistry = getQueryNamedXContentRegistry();
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

        ragTool.run(Map.of(INPUT_FIELD, TEST_QUERY_TEXT), listener);
        verify(listener).onFailure(any(RuntimeException.class));
        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertEquals("Failed to run model " + TEST_INFERENCE_MODEL_ID, argumentCaptor.getValue().getMessage());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRunWithEmptyInput() {
        ActionListener listener = mock(ActionListener.class);
        ragTool.run(Map.of(INPUT_FIELD, ""), listener);
    }

    @Test
    public void testFactoryNeuralQuery() {
        RAGTool.Factory factoryMock = new RAGTool.Factory();
        RAGTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);
        factoryMock.init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);

        String defaultDescription = factoryMock.getDefaultDescription();
        assertEquals(RAGTool.DEFAULT_DESCRIPTION, defaultDescription);
        assertEquals(factoryMock.getDefaultType(), RAGTool.TYPE);
        assertEquals(factoryMock.getDefaultVersion(), null);
        assertNotNull(RAGTool.Factory.getInstance());

        params.put(VectorDBTool.NESTED_PATH_FIELD, TEST_NESTED_PATH);
        RAGTool rAGtool1 = factoryMock.create(params);
        VectorDBTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);
        params.put(VectorDBTool.MODEL_ID_FIELD, TEST_EMBEDDING_MODEL_ID);
        VectorDBTool queryTool = VectorDBTool.Factory.getInstance().create(params);
        RAGTool rAGtool2 = new RAGTool(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY, TEST_INFERENCE_MODEL_ID, true, queryTool);

        assertEquals(rAGtool1.getClient(), rAGtool2.getClient());
        assertEquals(rAGtool1.getInferenceModelId(), rAGtool2.getInferenceModelId());
        assertEquals(rAGtool1.getName(), rAGtool2.getName());
        assertEquals(rAGtool1.getQueryTool().getDocSize(), rAGtool2.getQueryTool().getDocSize());
        assertEquals(rAGtool1.getQueryTool().getIndex(), rAGtool2.getQueryTool().getIndex());
        assertEquals(rAGtool1.getQueryTool().getSourceFields(), rAGtool2.getQueryTool().getSourceFields());
        assertEquals(rAGtool1.getXContentRegistry(), rAGtool2.getXContentRegistry());
        assertEquals(rAGtool1.getQueryType(), rAGtool2.getQueryType());
        assertEquals(((VectorDBTool) rAGtool1.getQueryTool()).getNestedPath(), ((VectorDBTool) rAGtool2.getQueryTool()).getNestedPath());
    }

    @Test
    public void testFactoryNeuralSparseQuery() {
        RAGTool.Factory factoryMock = new RAGTool.Factory();
        RAGTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);
        factoryMock.init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);

        String defaultDescription = factoryMock.getDefaultDescription();
        assertEquals(RAGTool.DEFAULT_DESCRIPTION, defaultDescription);
        assertNotNull(RAGTool.Factory.getInstance());
        assertEquals(factoryMock.getDefaultType(), RAGTool.TYPE);
        assertEquals(factoryMock.getDefaultVersion(), null);

        params.put(NeuralSparseSearchTool.NESTED_PATH_FIELD, TEST_NESTED_PATH);
        params.put("query_type", "neural_sparse");
        RAGTool rAGtool1 = factoryMock.create(params);
        NeuralSparseSearchTool.Factory.getInstance().init(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY);
        NeuralSparseSearchTool queryTool = NeuralSparseSearchTool.Factory.getInstance().create(params);
        RAGTool rAGtool2 = new RAGTool(client, TEST_XCONTENT_REGISTRY_FOR_NEURAL_QUERY, TEST_INFERENCE_MODEL_ID, true, queryTool);

        assertEquals(rAGtool1.getClient(), rAGtool2.getClient());
        assertEquals(rAGtool1.getInferenceModelId(), rAGtool2.getInferenceModelId());
        assertEquals(rAGtool1.getName(), rAGtool2.getName());
        assertEquals(rAGtool1.getQueryTool().getDocSize(), rAGtool2.getQueryTool().getDocSize());
        assertEquals(rAGtool1.getQueryTool().getIndex(), rAGtool2.getQueryTool().getIndex());
        assertEquals(rAGtool1.getQueryTool().getSourceFields(), rAGtool2.getQueryTool().getSourceFields());
        assertEquals(rAGtool1.getXContentRegistry(), rAGtool2.getXContentRegistry());
        assertEquals(rAGtool1.getQueryType(), rAGtool2.getQueryType());
        assertEquals(
            ((NeuralSparseSearchTool) rAGtool1.getQueryTool()).getNestedPath(),
            ((NeuralSparseSearchTool) rAGtool2.getQueryTool()).getNestedPath()
        );
    }

    private static NamedXContentRegistry getQueryNamedXContentRegistry() {
        QueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();

        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        NamedXContentRegistry.Entry neural_query_entry = new NamedXContentRegistry.Entry(
            QueryBuilder.class,
            new ParseField("neural"),
            (p, c) -> {
                p.map();
                return matchAllQueryBuilder;
            }
        );
        entries.add(neural_query_entry);
        NamedXContentRegistry.Entry neural_sparse_query_entry = new NamedXContentRegistry.Entry(
            QueryBuilder.class,
            new ParseField("neural_sparse"),
            (p, c) -> {
                p.map();
                return matchAllQueryBuilder;
            }
        );
        entries.add(neural_sparse_query_entry);
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
