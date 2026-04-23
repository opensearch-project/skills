/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.jsoup.helper.Validate.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.mockito.ArgumentCaptor;
import org.opensearch.agent.tools.utils.AnomalyDetectorToolHelper;
import org.opensearch.ml.common.indexInsight.IndexInsight;
import org.opensearch.ml.common.indexInsight.IndexInsightTaskStatus;
import org.opensearch.ml.common.indexInsight.MLIndexInsightType;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetAction;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetResponse;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.apache.lucene.search.TotalHits;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import com.google.common.collect.ImmutableMap;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class CreateAnomalyDetectorToolEnhancedTests {
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
    @Mock
    private NamedWriteableRegistry namedWriteableRegistry;

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
    private String mockedResponse = "{category_field=|aggregation_field=response,responseLatency|aggregation_method=count,avg|interval=10}";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        createMappings();

        // Setup mapping mocks
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
        CreateAnomalyDetectorToolEnhanced.Factory.getInstance().init(client, namedWriteableRegistry);
    }

    @Test
    public void testModelIdIsNullOrEmpty() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> CreateAnomalyDetectorToolEnhanced.Factory.getInstance().create(ImmutableMap.of("model_id", ""))
        );
        assertEquals("model_id cannot be empty.", exception.getMessage());
    }

    @Test
    public void testInvalidModelType() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> CreateAnomalyDetectorToolEnhanced.Factory
                .getInstance()
                .create(ImmutableMap.of("model_id", "modelId", "model_type", "unknown"))
        );
        assertEquals("Unsupported model_type: unknown", exception.getMessage());
    }

    @Test
    public void testValidModelTypes() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "model_type", "openai"));
        assertEquals(CreateAnomalyDetectorToolEnhanced.TYPE, tool.getName());
        assertEquals("modelId", tool.getModelId());
        assertEquals("OPENAI", tool.getModelType().toString());

        tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "model_type", "claude"));
        assertEquals(CreateAnomalyDetectorToolEnhanced.TYPE, tool.getName());
        assertEquals("modelId", tool.getModelId());
        assertEquals("CLAUDE", tool.getModelType().toString());
    }

    @Test
    public void testDefaultModelType() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));
        assertEquals("CLAUDE", tool.getModelType().toString());
    }

    @Test
    public void testEmptyModelType() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "model_type", ""));
        assertEquals("CLAUDE", tool.getModelType().toString());
    }

    @Test
    public void testCustomPrompt() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "custom prompt"));
        assertEquals("custom prompt", tool.getContextPrompt());
    }

    @Test
    public void testIndexNameValidation_SystemIndex() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> tool
                .run(
                    ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(".system_index")))),
                    ActionListener.<String>wrap(response -> {}, e -> {
                        throw new IllegalArgumentException(e.getMessage());
                    })
                )
        );
        assertTrue(exception.getMessage().contains("System indices not supported"));
    }

    @Test
    public void testInputFormat_IndicesList() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        // This should extract first index from list
        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("indexName"));
                }, e -> log.error("Error: ", e))
            );
    }

    @Test
    public void testLLMResponseParsing_ValidFormat() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        // Test that all fields from LLM response are correctly parsed
        String validResponse = "{category_field=host|aggregation_field=response,responseLatency|aggregation_method=count,avg|interval=15}";
        modelReturns = Collections.singletonMap("response", validResponse);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("\"status\":\"success\""));
                    assertTrue(response.contains("detectorId"));
                    assertTrue(response.contains("detectorName"));
                    Map<String, Object> result = gson.fromJson(response, Map.class);
                    assertEquals("success", result.get("status"));
                }, e -> fail("Should successfully parse valid LLM response: " + e.getMessage()))
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
                        "host",
                        ImmutableMap.of("type", "keyword"),
                        "date",
                        ImmutableMap.of("type", "date")
                    )
            );
        mockedMappings = new HashMap<>();
        mockedMappings.put(mockedIndexName, mappingMetadata);

        modelReturns = Collections.singletonMap("response", mockedResponse);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
    }

    @Test
    public void testLLMResponseParsing_InvalidFormat() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        modelReturns = Collections.singletonMap("response", "invalid format without curly braces");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("\"status\":\"failed_validation\""));
                    assertTrue(response.contains("Cannot parse LLM response after"));
                }, e -> fail("Should return JSON response, not throw exception: " + e.getMessage()))
            );
    }

    @Test
    public void testLLMResponseParsing_EmptyResponse() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        modelReturns = Collections.singletonMap("response", "");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("\"status\":\"failed_validation\""));
                    assertTrue(response.contains("Remote endpoint fails to inference, no response found"));
                }, e -> fail("Should return JSON response, not throw exception: " + e.getMessage()))
            );
    }

    @Test
    public void testLLMResponseParsing_NullResponse() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        modelReturns = Collections.singletonMap("response", null);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("\"status\":\"failed_validation\""));
                    assertTrue(response.contains("Remote endpoint fails to inference, no response found"));
                }, e -> fail("Should return JSON response, not throw exception: " + e.getMessage()))
            );
    }

    @Test
    public void testIntervalParsing_ValidInterval() {
        // Test that interval is correctly parsed from LLM response
        String responseWithInterval = "{category_field=|aggregation_field=response|aggregation_method=count|interval=15}";
        modelReturns = Collections.singletonMap("response", responseWithInterval);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue("Response should contain success status", response.contains("\"status\":\"success\""));
                    assertTrue("Response should contain detector configuration", response.contains("detectorName"));
                }, e -> fail("Should not throw exception: " + e.getMessage()))
            );
    }

    @Test
    public void testIntervalParsing_DefaultInterval() {
        // Test that default interval (10) is used when not specified
        String responseWithoutInterval = "{category_field=|aggregation_field=response|aggregation_method=count|interval=}";
        modelReturns = Collections.singletonMap("response", responseWithoutInterval);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue("Response should contain success status", response.contains("\"status\":\"success\""));
                    assertTrue("Response should contain detector configuration", response.contains("detectorName"));
                }, e -> fail("Should not throw exception: " + e.getMessage()))
            );
    }

    @Test
    public void testCategoryField_Empty() {
        // Test single-entity detector (no category field)
        String responseNoCategoryField = "{category_field=|aggregation_field=response|aggregation_method=count|interval=10}";
        modelReturns = Collections.singletonMap("response", responseNoCategoryField);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("\"status\":\"success\""));
                    assertTrue(response.contains("detectorName"));
                }, e -> fail("Should not throw exception: " + e.getMessage()))
            );
    }

    @Test
    public void testCategoryField_WithValue() {
        // Test multi-entity detector (with category field)
        String responseWithCategoryField = "{category_field=host|aggregation_field=response|aggregation_method=count|interval=10}";
        modelReturns = Collections.singletonMap("response", responseWithCategoryField);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
                ActionListener.<String>wrap(response -> {
                    assertTrue("Response should contain success status", response.contains("\"status\":\"success\""));
                    assertTrue("Response should contain detector configuration", response.contains("detectorName"));
                }, e -> fail("Should not throw exception: " + e.getMessage()))
            );
    }

    @Test
    public void testIndexNotFound() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        // Mock IndexNotFoundException
        doAnswer(invocation -> {
            ActionListener<GetMappingsResponse> listener = (ActionListener<GetMappingsResponse>) invocation.getArguments()[1];
            listener.onFailure(new Exception("IndexNotFoundException[no such index [nonexistent]]"));
            return null;
        }).when(indicesAdminClient).getMappings(any(), any());

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList("nonexistent")))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("\"status\":\"failed_validation\""));
                    assertTrue(response.contains("does not exist") || response.contains("no such index"));
                }, e -> fail("Should return JSON response, not throw exception: " + e.getMessage()))
            );
    }

    @Test
    public void testMultipleIndices() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Arrays.asList("index1", "index2")))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("index1"));
                    assertTrue(response.contains("index2"));
                }, e -> log.error("Error: ", e))
            );
    }

    @Test
    public void testIndexWithMultipleDateFields() {
        Map<String, Object> multiDateMappings = new HashMap<>();
        multiDateMappings
            .put(
                "properties",
                ImmutableMap
                    .of(
                        "timestamp1",
                        ImmutableMap.of("type", "date"),
                        "timestamp2",
                        ImmutableMap.of("type", "date"),
                        "created_at",
                        ImmutableMap.of("type", "date"),
                        "updated_at",
                        ImmutableMap.of("type", "date"),
                        "event_time",
                        ImmutableMap.of("type", "date_nanos"),
                        "response",
                        ImmutableMap.of("type", "integer")
                    )
            );

        when(mappingMetadata.getSourceAsMap()).thenReturn(multiDateMappings);

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList("multi_date_index")))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("indexName"));
                }, e -> log.error("Error: ", e))
            );
    }

    @Test
    public void testIndexWithNoDateFields() {
        Map<String, Object> noDateMappings = new HashMap<>();
        noDateMappings
            .put("properties", ImmutableMap.of("response", ImmutableMap.of("type", "integer"), "host", ImmutableMap.of("type", "keyword")));

        when(mappingMetadata.getSourceAsMap()).thenReturn(noDateMappings);

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool
            .run(
                ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList("no_date_index")))),
                ActionListener.<String>wrap(response -> {
                    assertTrue(response.contains("\"status\":\"failed_validation\""));
                    assertTrue(response.contains("has no date fields"));
                }, e -> fail("Should return JSON response, not throw exception: " + e.getMessage()))
            );
    }


    // ===== NEW TESTS FOR COMMIT 1 CHANGES =====

    @Test
    public void testGetAggMethod_ReturnsActualType() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        // Build features with different aggregation types and verify getAggMethod extracts them correctly
        AggregationBuilder avgAgg = AnomalyDetectorToolHelper.createAggregationBuilder("avg", "bytes");
        Feature avgFeature = new Feature("id1", "feature_bytes", true, avgAgg);
        assertEquals("avg", tool.getAggMethod(avgFeature));

        AggregationBuilder sumAgg = AnomalyDetectorToolHelper.createAggregationBuilder("sum", "bytes");
        Feature sumFeature = new Feature("id2", "feature_bytes", true, sumAgg);
        assertEquals("sum", tool.getAggMethod(sumFeature));

        // count maps to value_count internally — verify it maps back to "count"
        AggregationBuilder countAgg = AnomalyDetectorToolHelper.createAggregationBuilder("count", "requests");
        Feature countFeature = new Feature("id3", "feature_requests", true, countAgg);
        assertEquals("count", tool.getAggMethod(countFeature));

        AggregationBuilder maxAgg = AnomalyDetectorToolHelper.createAggregationBuilder("max", "latency");
        Feature maxFeature = new Feature("id4", "feature_latency", true, maxAgg);
        assertEquals("max", tool.getAggMethod(maxFeature));

        AggregationBuilder minAgg = AnomalyDetectorToolHelper.createAggregationBuilder("min", "latency");
        Feature minFeature = new Feature("id5", "feature_latency", true, minAgg);
        assertEquals("min", tool.getAggMethod(minFeature));
    }


    @Test
    public void testIndexInsightSuccess_InsightReachesLLMPrompt() throws Exception {
        // Mock Index Insight to return content
        mockFullDetectorCreationChain();
        String insightContent = "dataSource: web_logs, recommendedFeatures: bytes_sent";
        IndexInsight insight = IndexInsight.builder()
            .index(mockedIndexName)
            .content(insightContent)
            .status(IndexInsightTaskStatus.COMPLETED)
            .taskType(MLIndexInsightType.ALL)
            .lastUpdatedTime(Instant.now())
            .build();
        MLIndexInsightGetResponse insightResponse = new MLIndexInsightGetResponse(insight);

        doAnswer(invocation -> {
            ActionListener<MLIndexInsightGetResponse> listener = (ActionListener<MLIndexInsightGetResponse>) invocation.getArguments()[2];
            listener.onResponse(insightResponse);
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        // Capture the LLM prompt to verify insight was injected
        AtomicReference<String> capturedPrompt = new AtomicReference<>();

        // Re-mock LLM call to capture the request AND return a valid response
        String validResponse = "{category_field=|aggregation_field=response|aggregation_method=count|interval=10}";
        modelReturns = Collections.singletonMap("response", validResponse);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        // Override LLM mock to also capture the prompt
        doAnswer(invocation -> {
            MLPredictionTaskRequest req = (MLPredictionTaskRequest) invocation.getArguments()[1]; RemoteInferenceInputDataSet ds = (RemoteInferenceInputDataSet) req.getMlInput().getInputDataset(); capturedPrompt.set(ds.getParameters().toString());
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            listener.onResponse(mlTaskResponse);
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(response -> {
                responseRef.set(response);
                latch.countDown();
            }, e -> {
                responseRef.set("ERROR: " + e.getMessage());
                latch.countDown();
            })
        );

        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        // The key assertion: the insight content must appear in the prompt sent to the LLM
        Assert.assertNotNull("LLM should have been called", capturedPrompt.get());
        Assert.assertTrue(
            "Prompt must contain the Index Insight content, but was: " + capturedPrompt.get(),
            capturedPrompt.get().contains("INDEX ANALYSIS") || capturedPrompt.get().contains(insightContent)
        );
    }

    @Test
    public void testIndexInsightFailure_StillCreatesDetector() throws Exception {
        // Mock Index Insight to fail
        mockFullDetectorCreationChain();
        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("Index Insight not available"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        // Capture the LLM prompt to verify NO insight was injected
        AtomicReference<String> capturedPrompt = new AtomicReference<>();

        String validResponse = "{category_field=|aggregation_field=response|aggregation_method=count|interval=10}";
        modelReturns = Collections.singletonMap("response", validResponse);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        doAnswer(invocation -> {
            MLPredictionTaskRequest req = (MLPredictionTaskRequest) invocation.getArguments()[1]; RemoteInferenceInputDataSet ds = (RemoteInferenceInputDataSet) req.getMlInput().getInputDataset(); capturedPrompt.set(ds.getParameters().toString());
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            listener.onResponse(mlTaskResponse);
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(response -> {
                responseRef.set(response);
                latch.countDown();
            }, e -> {
                responseRef.set("ERROR: " + e.getMessage());
                latch.countDown();
            })
        );

        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        // Verify the LLM was still called (tool didn't abort)
        Assert.assertNotNull("LLM should have been called despite insight failure", capturedPrompt.get());
        // Verify no insight was injected into the prompt
        Assert.assertFalse(
            "Prompt must NOT contain INDEX ANALYSIS when insight failed",
            capturedPrompt.get().contains("INDEX ANALYSIS")
        );
    }

    @Test
    public void testTemplateVariableLeak_ReplacedWithActualField() throws Exception {
        // LLM returns literal ${dateFields} instead of the actual date field name
        mockFullDetectorCreationChain();
        String leakyResponse = "{category_field=|aggregation_field=${dateFields},responseLatency|aggregation_method=count,avg|interval=10}";
        modelReturns = Collections.singletonMap("response", leakyResponse);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        // Mock Index Insight to fail fast (skip insight, go straight to LLM)
        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(response -> {
                responseRef.set(response);
                latch.countDown();
            }, e -> {
                responseRef.set("ERROR: " + e.getMessage());
                latch.countDown();
            })
        );

        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);
        String response = responseRef.get();
        Assert.assertNotNull("Should get a response", response);
        // The response must NOT contain the literal template variable — it should have been replaced
        Assert.assertFalse(
            "Template variable ${dateFields} should have been replaced with actual date field, but response was: " + response,
            response.contains("${dateFields}")
        );
        Assert.assertFalse(
            "Template variable ${indexInfo.dateFields} should not appear in output",
            response.contains("${indexInfo.dateFields}")
        );
    }

    @Test
    public void testEmptyAggregationFields_FailsWithMessage() throws Exception {
        // LLM returns empty aggregation fields — all fields are blank after split
        mockFullDetectorCreationChain();
        String emptyFieldsResponse = "{category_field=|aggregation_field=,|aggregation_method=,|interval=10}";
        modelReturns = Collections.singletonMap("response", emptyFieldsResponse);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();

        // Mock Index Insight to fail fast
        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(response -> {
                responseRef.set(response);
                latch.countDown();
            }, e -> {
                responseRef.set("ERROR: " + e.getMessage());
                latch.countDown();
            })
        );

        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);
        String response = responseRef.get();
        Assert.assertNotNull("Should get a response", response);
        // Must NOT report success — empty features should fail
        Assert.assertFalse(
            "Empty aggregation fields must not produce a successful detector",
            response.contains("\"status\":\"success\"")
        );
    }


    // ===== COMMIT 2: OTel Fast-Path Tests =====

    @Test
    public void testOtelTraceMapping_CreatesTwoDetectors_SkipsLLM() throws Exception {
        // Set up OTel trace mapping with the 4 required signature fields
        Map<String, Object> otelTraceMapping = new HashMap<>();
        otelTraceMapping.put("properties", Map.of(
            "traceId", ImmutableMap.of("type", "keyword"),
            "spanId", ImmutableMap.of("type", "keyword"),
            "durationInNanos", ImmutableMap.of("type", "long"),
            "serviceName", ImmutableMap.of("type", "keyword"),
            "startTime", ImmutableMap.of("type", "date_nanos"),
            "status", ImmutableMap.of("type", "object", "properties", Map.of(
                "code", ImmutableMap.of("type", "integer")
            ))
        ));
        when(mappingMetadata.getSourceAsMap()).thenReturn(otelTraceMapping);
        mockedMappings.put("otel-traces", mappingMetadata);

        // Mock suggest (returns null interval = use default) + create + start
        mockOtelDetectorCreationChain();

        // Mock Index Insight to fail fast
        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList("otel-traces")))),
            ActionListener.<String>wrap(r -> { responseRef.set(r); latch.countDown(); },
                                        e -> { responseRef.set("ERROR: " + e.getMessage()); latch.countDown(); })
        );
        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        String response = responseRef.get();
        Assert.assertNotNull("Should get a response", response);
        Assert.assertFalse("Should not error", response.startsWith("ERROR"));

        // Parse outer result map
        Map<String, Object> results = gson.fromJson(response, Map.class);
        Object otelResult = results.get("otel-traces");
        Assert.assertNotNull("Should have result for otel-traces", otelResult);

        // OTel path returns a list of detector results
        Assert.assertTrue("OTel result should be a List, got: " + otelResult.getClass(), otelResult instanceof List);
        List<?> detectors = (List<?>) otelResult;
        Assert.assertEquals("Should create exactly 2 detectors for traces", 2, detectors.size());

        // Verify LLM was never called (OTel path bypasses LLM)
        org.mockito.Mockito.verify(client, org.mockito.Mockito.never())
            .execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
    }

    @Test
    public void testOtelLogMapping_CreatesTwoDetectors_SkipsLLM() throws Exception {
        Map<String, Object> otelLogMapping = new HashMap<>();
        otelLogMapping.put("properties", Map.of(
            "severityNumber", ImmutableMap.of("type", "integer"),
            "severityText", ImmutableMap.of("type", "keyword"),
            "time", ImmutableMap.of("type", "date"),
            "resource", ImmutableMap.of("type", "object", "properties", Map.of(
                "attributes", ImmutableMap.of("type", "object", "properties", Map.of(
                    "service.name", ImmutableMap.of("type", "keyword")
                ))
            ))
        ));
        when(mappingMetadata.getSourceAsMap()).thenReturn(otelLogMapping);
        mockedMappings.put("otel-logs", mappingMetadata);

        mockOtelDetectorCreationChain();

        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList("otel-logs")))),
            ActionListener.<String>wrap(r -> { responseRef.set(r); latch.countDown(); },
                                        e -> { responseRef.set("ERROR: " + e.getMessage()); latch.countDown(); })
        );
        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        String response = responseRef.get();
        Map<String, Object> results = gson.fromJson(response, Map.class);
        Object otelResult = results.get("otel-logs");
        Assert.assertTrue("OTel log result should be a List", otelResult instanceof List);
        Assert.assertEquals("Should create exactly 2 detectors for logs", 2, ((List<?>) otelResult).size());

        org.mockito.Mockito.verify(client, org.mockito.Mockito.never())
            .execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
    }

    @Test
    public void testNonOtelMapping_FallsThrough_ToLLM() throws Exception {
        // Default mapping (response:integer, host:keyword, date:date) is NOT OTel
        mockFullDetectorCreationChain();

        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(r -> { responseRef.set(r); latch.countDown(); },
                                        e -> { responseRef.set("ERROR: " + e.getMessage()); latch.countDown(); })
        );
        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        // LLM SHOULD have been called (non-OTel falls through)
        org.mockito.Mockito.verify(client, org.mockito.Mockito.atLeastOnce())
            .execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
    }

    @Test
    public void testPartialOtelMapping_DoesNotTriggerFastPath() throws Exception {
        // Has durationInNanos + serviceName but missing spanId — should NOT detect as OTel
        Map<String, Object> partialMapping = new HashMap<>();
        partialMapping.put("properties", Map.of(
            "durationInNanos", ImmutableMap.of("type", "long"),
            "serviceName", ImmutableMap.of("type", "keyword"),
            "timestamp", ImmutableMap.of("type", "date"),
            "responseCode", ImmutableMap.of("type", "integer")
        ));
        when(mappingMetadata.getSourceAsMap()).thenReturn(partialMapping);
        mockedMappings.put("partial-otel", mappingMetadata);

        mockFullDetectorCreationChain();

        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList("partial-otel")))),
            ActionListener.<String>wrap(r -> { responseRef.set(r); latch.countDown(); },
                                        e -> { responseRef.set("ERROR: " + e.getMessage()); latch.countDown(); })
        );
        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        // LLM should have been called — partial OTel should NOT trigger fast-path
        org.mockito.Mockito.verify(client, org.mockito.Mockito.atLeastOnce())
            .execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
    }

    // ===== COMMIT 5: Sequential Multi-Detector Tests =====

    @Test
    public void testFilterExpression_AppliedToDetector() throws Exception {
        // LLM returns a response with filter=status:gte:400
        String responseWithFilter = "{category_field=host|aggregation_field=status|aggregation_method=count|filter=status:gte:400|interval=10}";
        modelReturns = Collections.singletonMap("response", responseWithFilter);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();
        mockFullDetectorCreationChain();

        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(r -> { responseRef.set(r); latch.countDown(); },
                                        e -> { responseRef.set("ERROR: " + e.getMessage()); latch.countDown(); })
        );
        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        String response = responseRef.get();
        Assert.assertNotNull("Should get a response", response);
        Assert.assertFalse("Should not error", response.startsWith("ERROR"));
        // The detector should have been created (filter parsed successfully)
        Assert.assertTrue("Should contain success status", response.contains("success"));
    }

    @Test
    public void testNoneSignal_StopsLoop() throws Exception {
        // First call returns a valid detector, second call returns {NONE}
        mockFullDetectorCreationChain();

        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        // Track LLM call count and return {NONE} on second call
        final int[] callCount = {0};
        doAnswer(invocation -> {
            callCount[0]++;
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            if (callCount[0] == 1) {
                // First call: valid response
                modelReturns = Collections.singletonMap("response",
                    "{category_field=host|aggregation_field=response|aggregation_method=count|filter=|interval=10}");
                modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
                when(modelTensors.getMlModelTensors()).thenReturn(Collections.singletonList(modelTensor));
                when(modelTensorOutput.getMlModelOutputs()).thenReturn(Collections.singletonList(modelTensors));
                when(mlTaskResponse.getOutput()).thenReturn(modelTensorOutput);
            } else {
                // Second call: {NONE}
                modelReturns = Collections.singletonMap("response", "{NONE}");
                modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
                when(modelTensors.getMlModelTensors()).thenReturn(Collections.singletonList(modelTensor));
                when(modelTensorOutput.getMlModelOutputs()).thenReturn(Collections.singletonList(modelTensors));
                when(mlTaskResponse.getOutput()).thenReturn(modelTensorOutput);
            }
            listener.onResponse(mlTaskResponse);
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(r -> { responseRef.set(r); latch.countDown(); },
                                        e -> { responseRef.set("ERROR: " + e.getMessage()); latch.countDown(); })
        );
        latch.await(10, java.util.concurrent.TimeUnit.SECONDS);

        String response = responseRef.get();
        Assert.assertNotNull("Should get a response", response);
        // Should have exactly 1 detector (first call succeeded, second returned NONE)
        Assert.assertTrue("Should contain success", response.contains("success"));
        // LLM was called exactly 2 times (once for detector, once got NONE)
        Assert.assertEquals("LLM should be called exactly 2 times", 2, callCount[0]);
    }

    @Test
    public void testInvalidFilter_GracefulDegradation() throws Exception {
        // LLM returns an invalid filter expression — should create detector without filter
        String responseWithBadFilter = "{category_field=|aggregation_field=response|aggregation_method=count|filter=invalid_no_colons|interval=10}";
        modelReturns = Collections.singletonMap("response", responseWithBadFilter);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();
        mockFullDetectorCreationChain();

        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(r -> { responseRef.set(r); latch.countDown(); },
                                        e -> { responseRef.set("ERROR: " + e.getMessage()); latch.countDown(); })
        );
        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        String response = responseRef.get();
        Assert.assertNotNull("Should get a response", response);
        // Should still create the detector — invalid filter is silently ignored
        Assert.assertTrue("Should contain success despite bad filter", response.contains("success"));
    }

    @Test
    public void testOldFormatWithoutFilter_StillWorks() throws Exception {
        // LLM returns old format without filter= field — should fall back to old regex
        String oldFormatResponse = "{category_field=host|aggregation_field=response|aggregation_method=count|interval=10}";
        modelReturns = Collections.singletonMap("response", oldFormatResponse);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, modelReturns);
        initMLTensors();
        mockFullDetectorCreationChain();

        doAnswer(invocation -> {
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("skip"));
            return null;
        }).when(client).execute(eq(MLIndexInsightGetAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> responseRef = new AtomicReference<>();

        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        tool.run(
            ImmutableMap.of("input", gson.toJson(ImmutableMap.of("indices", Collections.singletonList(mockedIndexName)))),
            ActionListener.<String>wrap(r -> { responseRef.set(r); latch.countDown(); },
                                        e -> { responseRef.set("ERROR: " + e.getMessage()); latch.countDown(); })
        );
        latch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        String response = responseRef.get();
        Assert.assertNotNull("Should get a response", response);
        Assert.assertTrue("Old format should still work via fallback regex", response.contains("success"));
    }

    @Test
    public void testParseFilterExpression() {
        CreateAnomalyDetectorToolEnhanced tool = CreateAnomalyDetectorToolEnhanced.Factory
            .getInstance().create(ImmutableMap.of("model_id", "modelId"));

        // Valid range operators
        Assert.assertNotNull("gte should parse", tool.parseFilterExpression("status:gte:400"));
        Assert.assertNotNull("gt should parse", tool.parseFilterExpression("latency:gt:5000"));
        Assert.assertNotNull("lte should parse", tool.parseFilterExpression("severity:lte:3"));
        Assert.assertNotNull("lt should parse", tool.parseFilterExpression("count:lt:10"));

        // Valid term operator
        Assert.assertNotNull("eq should parse", tool.parseFilterExpression("status.code:eq:2"));

        // Null/empty → null
        Assert.assertNull("null input", tool.parseFilterExpression(null));
        Assert.assertNull("empty input", tool.parseFilterExpression(""));

        // Invalid format → null (graceful)
        Assert.assertNull("no colons", tool.parseFilterExpression("invalid_no_colons"));
        Assert.assertNull("one colon", tool.parseFilterExpression("field:value"));
        Assert.assertNull("unknown operator", tool.parseFilterExpression("field:between:1"));
    }

    /** Mocks suggest + create + start for OTel path (no validate needed). */
    private void mockOtelDetectorCreationChain() {
        // Validate detector — return no issues
        doAnswer(invocation -> {
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new org.opensearch.timeseries.transport.ValidateConfigResponse((org.opensearch.timeseries.model.ConfigValidationIssue) null));
            return null;
        }).when(client).execute(eq(org.opensearch.ad.transport.ValidateAnomalyDetectorAction.INSTANCE), any(), any());

        // Suggest — return null interval (use default)
        doAnswer(invocation -> {
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new org.opensearch.timeseries.transport.SuggestConfigParamResponse(null, null, null, null));
            return null;
        }).when(client).execute(eq(org.opensearch.ad.transport.SuggestAnomalyDetectorParamAction.INSTANCE), any(), any());

        // Create detector
        doAnswer(invocation -> {
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new org.opensearch.ad.transport.IndexAnomalyDetectorResponse(
                "otel-detector-id", 1L, 1L, 1L, null, org.opensearch.core.rest.RestStatus.CREATED));
            return null;
        }).when(client).execute(eq(org.opensearch.ad.transport.IndexAnomalyDetectorAction.INSTANCE), any(), any());

        // Start detector
        doAnswer(invocation -> {
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new org.opensearch.timeseries.transport.JobResponse("otel-detector-id"));
            return null;
        }).when(client).execute(eq(org.opensearch.ad.transport.AnomalyDetectorJobAction.INSTANCE), any(), any());
    }

    private void mockSearchForDateFieldSelection() {
        SearchResponse searchResponse = org.mockito.Mockito.mock(SearchResponse.class);
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.0f);
        when(searchResponse.getHits()).thenReturn(searchHits);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
    }

    /**
     * Mocks the full async chain: search (date field selection), validate, suggest, create, start.
     */
    private void mockFullDetectorCreationChain() {
        mockSearchForDateFieldSelection();

        // Validate detector — return no issues
        doAnswer(invocation -> {
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new org.opensearch.timeseries.transport.ValidateConfigResponse((org.opensearch.timeseries.model.ConfigValidationIssue) null));
            return null;
        }).when(client).execute(eq(org.opensearch.ad.transport.ValidateAnomalyDetectorAction.INSTANCE), any(), any());

        // Suggest hyperparameters — return defaults
        doAnswer(invocation -> {
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new org.opensearch.timeseries.transport.SuggestConfigParamResponse(null, null, null, null));
            return null;
        }).when(client).execute(eq(org.opensearch.ad.transport.SuggestAnomalyDetectorParamAction.INSTANCE), any(), any());

        // Create detector
        doAnswer(invocation -> {
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new org.opensearch.ad.transport.IndexAnomalyDetectorResponse(
                "test-detector-id", 1L, 1L, 1L, null, org.opensearch.core.rest.RestStatus.CREATED));
            return null;
        }).when(client).execute(eq(org.opensearch.ad.transport.IndexAnomalyDetectorAction.INSTANCE), any(), any());

        // Start detector
        doAnswer(invocation -> {
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onResponse(new org.opensearch.timeseries.transport.JobResponse("test-detector-id"));
            return null;
        }).when(client).execute(eq(org.opensearch.ad.transport.AnomalyDetectorJobAction.INSTANCE), any(), any());
    }

    private void initMLTensors() {
        when(modelTensors.getMlModelTensors()).thenReturn(Collections.singletonList(modelTensor));
        when(modelTensorOutput.getMlModelOutputs()).thenReturn(Collections.singletonList(modelTensors));
        when(mlTaskResponse.getOutput()).thenReturn(modelTensorOutput);

        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            listener.onResponse(mlTaskResponse);
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
    }
}
