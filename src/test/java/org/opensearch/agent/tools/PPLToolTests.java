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
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.ml.common.output.model.MLResultDataType;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;

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
    public void testTool() {
        Tool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());

        tool.run(ImmutableMap.of("index", "demo", "question", "demo"), ActionListener.<String>wrap(executePPLResult -> {
            Map<String, String> returnResults = gson.fromJson(executePPLResult, Map.class);
            assertEquals("ppl result", returnResults.get("executionResult"));
            assertEquals("source=demo| head 1", returnResults.get("ppl"));
        }, e -> { log.info(e); }));

    }

    @Test
    public void testTool_withPPLTag() {
        Tool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
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
    public void testTool_querySystemIndex() {
        Tool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
        assertEquals(PPLTool.TYPE, tool.getName());
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> tool.run(ImmutableMap.of("index", ML_CONNECTOR_INDEX, "question", "demo"), ActionListener.<String>wrap(ppl -> {
                assertEquals(pplResult, "ppl result");
            }, e -> { assertEquals("We cannot search system indices " + ML_CONNECTOR_INDEX, e.getMessage()); }))
        );
        assertEquals(
            "PPLTool doesn't support searching system indices, current searching index name: " + ML_CONNECTOR_INDEX,
            exception.getMessage()
        );
    }

    @Test
    public void testTool_getMappingFailure() {
        Tool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
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
        Tool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
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
        Tool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
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
        Tool tool = PPLTool.Factory.getInstance().create(ImmutableMap.of("model_id", "modelId", "prompt", "contextPrompt"));
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
