/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.CommonValue.ML_CONNECTOR_INDEX;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import com.google.common.collect.ImmutableMap;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class PPLToolTests {
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

    private SearchHits searchHits;

    private SearchHit hit;
    @Mock
    private SearchResponse searchResponse;

    private Map<String, Object> sampleMapping;

    @Mock
    private MLTaskResponse mlTaskResponse;
    @Mock
    private ModelTensorOutput modelTensorOutput;
    @Mock
    private ModelTensors modelTensors;

    private ModelTensor modelTensor;

    private Map<String, ?> pplReturns;

    @Mock
    private TransportPPLQueryResponse transportPPLQueryResponse;

    private String mockedIndexName = "demo";

    private String pplResult = "ppl result";

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
        // mockedMappings (index name, mappingmetadata)

        // search result

        when(searchResponse.getHits()).thenReturn(searchHits);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        initMLTensors();

        when(transportPPLQueryResponse.getResult()).thenReturn(pplResult);

        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onResponse(transportPPLQueryResponse);
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());
        PPLTool.Factory.getInstance().init(client);
    }

    @Test
    public void testTool_WithoutModelId() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> PPLTool.Factory.getInstance().create(ImmutableMap.of("prompt", "contextPrompt"))
        );
        assertEquals("PPL tool needs non blank model id.", exception.getMessage());
    }

    @Test
    public void testTool_WithBlankModelId() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "   "))
        );
        assertEquals("PPL tool needs non blank model id.", exception.getMessage());
    }

    @Test
    public void testTool_WithNonIntegerHead() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "demo", "head", "11.5"))
        );
        assertEquals("PPL tool parameter head must be integer.", exception.getMessage());
    }

    @Test
    public void testTool_WithNonBooleanExecute() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "demo", "execute", "hello"))
        );
        assertEquals("PPL tool parameter execute must be false or true", exception.getMessage());
    }

    @Test
    public void testTool() {
        PPLTool tool = PPLTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "head", "100"));
        assertEquals(PPLTool.TYPE, tool.getName());

        tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
            assertEquals("ppl result", returnResults.get("executionResult"));
            assertEquals("source=demo| head 1", returnResults.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testToolWhenGettingSagemakerError() {
        PPLTool tool = PPLTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "head", "100"));
        assertEquals(PPLTool.TYPE, tool.getName());
        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            OpenSearchStatusException exception = new OpenSearchStatusException(
                "Error from remote service: {\"ErrorCode\":\"CLIENT_ERROR_FROM_MODEL\",\"LogStreamArn\":\"arn:aws:logs:us-east-1:12345678:log-group:/aws/sagemaker/Endpoints/demo-test-name\",\"Message\":\"Received client error (404) from primary with message \\\"{\\n \\\"code\\\":404,\\n \\\"message\\\":\\\"prediction failure\\\",\\n \\\"error\\\":\\\"Input token limit exceeded. The model only supports schemas with less than 1000-1500 fields, and has optimal performance for 350 fields or fewer.\\\"\\n}\\\". See https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group=/aws/sagemaker/Endpoints/demo-test-name in account 12345678 for more information.\",\"OriginalMessage\":\"{\\n \\\"code\\\":500,\\n \\\"message\\\":\\\"prediction failure\\\",\\n \\\"error\\\":\\\"Input token limit exceeded. The model only supports schemas with less than 1000-1500 fields, and has optimal performance for 350 fields or fewer.\\\"\\n}\",\"OriginalStatusCode\":404}",
                RestStatus.fromCode(404)
            );

            listener.onFailure(exception);
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());
        OpenSearchStatusException exception = assertThrows(
            OpenSearchStatusException.class,
            () -> tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
                Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
                assertEquals("ppl result", returnResults.get("executionResult"));
                assertEquals("source=demo| head 1", returnResults.get("ppl"));
            }, e -> { throw new OpenSearchStatusException(e.getMessage(), ((OpenSearchStatusException) e).status()); }))
        );
        assertTrue(exception.getMessage().contains("<SAGEMAKER_ENDPOINT>"));
        assertFalse(exception.getMessage().contains("demo-test-name"));
        assertFalse(exception.getMessage().contains("12345678"));

    }

    @Test
    public void testTool_ForSparkInputWithWrongSchema() {
        PPLTool tool = PPLTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "head", "100"));
        assertEquals(PPLTool.TYPE, tool.getName());
        List<Object> samples = List.of(Map.of("headers", List.of(Map.of("name", "X-Forwarded-For", "value", "34.210.155.133"))));
        String wrongSchema = "array<structss<name:string,value:string>>";
        Map<String, Object> schema = Map.of("headers", Map.of("col_name", "headers", "data_type", wrongSchema));
        Exception exception = assertThrows(
            IllegalStateException.class,
            () -> tool
                .run(
                    ImmutableMap
                        .of(
                            "index",
                            "demo",
                            "question",
                            "demo",
                            "samples",
                            gson.toJson(samples),
                            "schema",
                            gson.toJson(schema),
                            "type",
                            "s3"
                        ),
                    ActionListener.<String>wrap(executePPLResult -> {
                        Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
                        assertEquals("ppl result", returnResults.get("executionResult"));
                        assertEquals("source=demo| head 1", returnResults.get("ppl"));
                    }, e -> { throw new IllegalStateException(e.getMessage()); })
                )
        );
        assertEquals("Unable to extract field types from schema " + wrongSchema, exception.getMessage());

    }

    @Test
    public void testTool_ForSparkInputWithArrayInput() {
        PPLTool tool = PPLTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "head", "100"));
        assertEquals(PPLTool.TYPE, tool.getName());
        List<Object> samples = List.of(Map.of("headers", List.of(Map.of("name", "X-Forwarded-For", "value", "34.210.155.133"))));
        Map<String, Object> schema = Map
            .of("headers", Map.of("col_name", "headers", "data_type", "array<struct<name:string,value:string>>"));
        tool
            .run(
                ImmutableMap
                    .of("index", "demo", "question", "demo", "samples", gson.toJson(samples), "schema", gson.toJson(schema), "type", "s3"),
                ActionListener.<String>wrap(executePPLResult -> {
                    Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
                    assertEquals("ppl result", returnResults.get("executionResult"));
                    assertEquals("source=demo| head 1", returnResults.get("ppl"));
                }, e -> { log.info(e); })
            );

    }

    @Test
    public void testTool_ForSparkInputWithArrayString() {
        PPLTool tool = PPLTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "head", "100"));
        assertEquals(PPLTool.TYPE, tool.getName());
        List<Object> samples = List.of(Map.of("headers", List.of(Map.of("name", "X-Forwarded-For", "value", "34.210.155.133"))));
        Map<String, Object> schema = Map.of("headers", Map.of("col_name", "headers", "data_type", "array<string>"));
        tool
            .run(
                ImmutableMap
                    .of("index", "demo", "question", "demo", "samples", gson.toJson(samples), "schema", gson.toJson(schema), "type", "s3"),
                ActionListener.<String>wrap(executePPLResult -> {
                    Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
                    assertEquals("ppl result", returnResults.get("executionResult"));
                    assertEquals("source=demo| head 1", returnResults.get("ppl"));
                }, e -> { log.info(e); })
            );

    }

    @Test
    public void testTool_ForSparkInputWithStructInput() {
        PPLTool tool = PPLTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "head", "100"));
        assertEquals(PPLTool.TYPE, tool.getName());
        List<Object> samples = List
            .of(
                Map
                    .of(
                        "httpMethod",
                        "POST",
                        "httpRequest",
                        Map.of("headers", List.of(Map.of("name", "X-Forwarded-For", "value", "34.210.155.133")))
                    )
            );
        Map<String, Object> schema = Map
            .of(
                "httpRequest",
                Map.of("col_name", "httpRequest", "data_type", "struct<headers:array<struct<name:string,value:string>>,httpMethod:string>"),
                "httpMethod",
                Map.of("col_name", "httpMethod", "data_type", "string")
            );
        tool
            .run(
                ImmutableMap
                    .of("index", "demo", "question", "demo", "samples", gson.toJson(samples), "schema", gson.toJson(schema), "type", "s3"),
                ActionListener.<String>wrap(executePPLResult -> {
                    Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
                    assertEquals("ppl result", returnResults.get("executionResult"));
                    assertEquals("source=demo| head 1", returnResults.get("ppl"));
                }, e -> { log.info(e); })
            );

    }

    @Test
    public void testTool_withPreviousInput() {
        PPLTool tool = PPLTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "previous_tool_name", "previousTool", "head", "-5"));
        assertEquals(PPLTool.TYPE, tool.getName());

        tool.run(ImmutableMap.of("previousTool.output", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
            assertEquals("ppl result", returnResults.get("executionResult"));
            assertEquals("source=demo| head 1", returnResults.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testTool_withHEADButIgnore() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "head", "5"));
        assertEquals(PPLTool.TYPE, tool.getName());

        tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
            assertEquals("ppl result", returnResults.get("executionResult"));
            assertEquals("source=demo| head 1", returnResults.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testTool_withHEAD() {
        pplReturns = Collections.singletonMap("response", "source=demo");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, pplReturns);
        initMLTensors();

        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt", "head", "5"));
        assertEquals(PPLTool.TYPE, tool.getName());

        tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
            assertEquals("ppl result", returnResults.get("executionResult"));
            assertEquals("source=demo | head 5", returnResults.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testTool_with_WithoutExecution() {
        PPLTool tool = PPLTool.Factory
            .getInstance()
            .create(ImmutableMap.of("model_id", "modelId", "model_type", "claude", "execute", "false"));
        assertEquals(PPLTool.TYPE, tool.getName());

        tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> ret = gson.fromJson(executePPLResult, Map.class);
            assertEquals("source=demo| head 1", ret.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testTool_with_DefaultPrompt() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "model_type", "claude"));
        assertEquals(PPLTool.TYPE, tool.getName());

        tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
            assertEquals("ppl result", returnResults.get("executionResult"));
            assertEquals("source=demo| head 1", returnResults.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testTool_withPPLTag() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());

        pplReturns = Collections.singletonMap("response", "<ppl>source=demo\n|\n\rhead 1</ppl>");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, pplReturns);
        initMLTensors();

        tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
            assertEquals("ppl result", returnResults.get("executionResult"));
            assertEquals("source=demo|head 1", returnResults.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testTool_withWrongEndpointInference() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());

        pplReturns = Collections.singletonMap("code", "424");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, pplReturns);
        initMLTensors();

        Exception exception = assertThrows(
            IllegalStateException.class,
            () -> tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(ppl -> {
                assertEquals(pplResult, "ppl result");
            }, e -> { throw new IllegalStateException(e.getMessage()); }))
        );
        assertEquals("Remote endpoint fails to inference.", exception.getMessage());
    }

    @Test
    public void testTool_withWrongEndpointInferenceWithNullResponse() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());

        pplReturns = Collections.singletonMap("response", null);
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, pplReturns);
        initMLTensors();

        Exception exception = assertThrows(
            IllegalStateException.class,
            () -> tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(ppl -> {
                assertEquals(pplResult, "ppl result");
            }, e -> { throw new IllegalStateException(e.getMessage()); }))
        );
        assertEquals("Remote endpoint fails to inference.", exception.getMessage());
    }

    @Test
    public void testTool_withDescribeStartPPL() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());

        pplReturns = Collections.singletonMap("response", "describe demo");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, pplReturns);
        initMLTensors();

        tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
            assertEquals("ppl result", returnResults.get("executionResult"));
            assertEquals("describe demo", returnResults.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testTool_querySystemIndex() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> tool.run(ImmutableMap.of("index", ML_CONNECTOR_INDEX, "question", "demo"), ActionListener.<String>wrap(ppl -> {
                assertEquals(pplResult, "ppl result");
            }, e -> { assertEquals("We cannot search system indices " + ML_CONNECTOR_INDEX, e.getMessage()); }))
        );
        assertEquals(
            "PPLTool doesn't support searching indices starting with '.' since it could be system index, current searching index name: "
                + ML_CONNECTOR_INDEX,
            exception.getMessage()
        );
    }

    @Test
    public void testTool_queryEmptyIndex() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> tool.run(ImmutableMap.of("question", "demo"), ActionListener.<String>wrap(ppl -> {
                assertEquals(pplResult, "ppl result");
            }, e -> { assertEquals("We cannot search system indices " + ML_CONNECTOR_INDEX, e.getMessage()); }))
        );
        assertEquals(
            "Return this final answer to human directly and do not use other tools: 'Please provide index name'. Please try to directly send this message to human to ask for index name",
            exception.getMessage()
        );
    }

    @Test
    public void testTool_WrongModelType() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "model_type", "wrong_model_type"));
        assertEquals(PPLTool.PPLModelType.CLAUDE, tool.getPplModelType());
    }

    @Test
    public void testTool_getMappingFailure() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());
        Exception exception = new Exception("get mapping error");
        doAnswer(invocation -> {
            ActionListener<GetMappingsResponse> listener = (ActionListener<GetMappingsResponse>) invocation.getArguments()[1];
            listener.onFailure(exception);
            return null;
        }).when(indicesAdminClient).getMappings(any(), any());

        tool
            .run(
                ImmutableMap.of("index", "demo", "question", "demo"),
                ActionListener.<String>wrap(ppl -> { assertEquals(pplResult, "ppl result"); }, e -> {
                    assertEquals("get mapping error", e.getMessage());
                })
            );
    }

    @Test
    public void testTool_predictModelFailure() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());
        Exception exception = new Exception("predict model error");
        doAnswer(invocation -> {
            ActionListener<MLTaskResponse> listener = (ActionListener<MLTaskResponse>) invocation.getArguments()[2];
            listener.onFailure(exception);
            return null;
        }).when(client).execute(eq(MLPredictionTaskAction.INSTANCE), any(), any());

        tool
            .run(
                ImmutableMap.of("index", "demo", "question", "demo"),
                ActionListener.<String>wrap(ppl -> { assertEquals(pplResult, "ppl result"); }, e -> {
                    assertEquals("predict model error", e.getMessage());
                })
            );
    }

    @Test
    public void testTool_searchFailure() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());
        Exception exception = new Exception("search error");
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onFailure(exception);
            return null;
        }).when(client).search(any(), any());

        tool
            .run(
                ImmutableMap.of("index", "demo", "question", "demo"),
                ActionListener.<String>wrap(ppl -> { assertEquals(pplResult, "ppl result"); }, e -> {
                    assertEquals("search error", e.getMessage());
                })
            );
    }

    @Test
    public void testTool_executePPLFailure() {
        PPLTool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());
        Exception exception = new Exception("execute ppl error");
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onFailure(exception);
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        tool
            .run(
                ImmutableMap.of("index", "demo", "question", "demo"),
                ActionListener.<String>wrap(ppl -> { assertEquals(pplResult, "ppl result"); }, e -> {
                    assertEquals("execute ppl:source=demo| head 1, get error: execute ppl error", e.getMessage());
                })
            );
    }

    private void createMappings() {
        indexMappings = new HashMap<>();
        indexMappings
            .put(
                "properties",
                ImmutableMap
                    .of(
                        "demoFields",
                        ImmutableMap.of("type", "text"),
                        "demoNested",
                        ImmutableMap
                            .of(
                                "properties",
                                ImmutableMap.of("nest1", ImmutableMap.of("type", "text"), "nest2", ImmutableMap.of("type", "text"))
                            )
                    )
            );
        mockedMappings = new HashMap<>();
        mockedMappings.put(mockedIndexName, mappingMetadata);

        BytesReference bytesArray = new BytesArray("{\"demoFields\":\"111\", \"demoNested\": {\"nest1\": \"222\", \"nest2\": \"333\"}}");
        hit = new SearchHit(1);
        hit.sourceRef(bytesArray);
        searchHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        pplReturns = Collections.singletonMap("response", "source=demo| head 1");
        modelTensor = new ModelTensor("tensor", new Number[0], new long[0], MLResultDataType.STRING, null, null, pplReturns);

    }

    private void initMLTensors(){
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
