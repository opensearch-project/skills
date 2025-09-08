/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import lombok.SneakyThrows;
import org.apache.lucene.search.TotalHits;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.opensearch.ml.common.utils.StringUtils.gson;

public class DataDistributionToolTests {

    private Map<String, Object> params = new HashMap<>();
    private final Client client = mock(Client.class);
    @Mock
    private AdminClient adminClient;
    @Mock
    private IndicesAdminClient indicesAdminClient;
    @Mock
    private GetMappingsResponse getMappingsResponse;
    @Mock
    private MappingMetadata mappingMetadata;
    @Mock
    private SearchResponse searchResponse;
    @Mock
    private TransportPPLQueryResponse pplQueryResponse;

    @SneakyThrows
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        setupMockMappings();
        DataDistributionTool.Factory.getInstance().init(client);
    }

    private void mockSearchResponse() {
        SearchHit[] hits = createSampleHits();
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        when(searchResponse.getHits()).thenReturn(searchHits);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
    }

    private void mockPPLInvocation(String response) {
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onResponse(pplQueryResponse);
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());
        when(pplQueryResponse.getResult()).thenReturn(response);
    }

    @Test
    @SneakyThrows
    public void testCreateTool() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        assertEquals("DataDistributionTool", tool.getType());
        assertEquals("DataDistributionTool", tool.getName());
        assertEquals(DataDistributionTool.Factory.getInstance().getDefaultDescription(), tool.getDescription());
        assertNull(DataDistributionTool.Factory.getInstance().getDefaultVersion());
    }

    @Test
    public void testValidate() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        // Valid parameters
        assertTrue(
            tool
                .validate(
                    Map
                        .of(
                            "index",
                            "test_index",
                            "selectionTimeRangeStart",
                            "2025-01-15 10:00:00",
                            "selectionTimeRangeEnd",
                            "2025-01-15 11:00:00"
                        )
                )
        );

        // Valid parameters with new fields
        assertTrue(
            tool
                .validate(
                    Map
                        .of(
                            "index",
                            "test_index",
                            "selectionTimeRangeStart",
                            "2025-01-15 10:00:00",
                            "selectionTimeRangeEnd",
                            "2025-01-15 11:00:00",
                            "filter",
                            "[\"{'term': {'status': 'error'}}\"]",
                            "ppl",
                            "source=logs-* | where status='error'"
                        )
                )
        );

        // Missing required parameters
        assertFalse(tool.validate(Map.of("index", "test_index")));
        assertFalse(tool.validate(Map.of()));

        // Missing selectionTimeRangeStart
        assertFalse(tool.validate(Map.of("index", "test_index", "selectionTimeRangeEnd", "2025-01-15 11:00:00")));

        // Missing selectionTimeRangeEnd
        assertFalse(tool.validate(Map.of("index", "test_index", "selectionTimeRangeStart", "2025-01-15 10:00:00")));

        // Valid with default queryType and timeField
        assertTrue(
            tool
                .validate(
                    Map
                        .of(
                            "index",
                            "test_index",
                            "selectionTimeRangeStart",
                            "2025-01-15 10:00:00",
                            "selectionTimeRangeEnd",
                            "2025-01-15 11:00:00"
                        )
                )
        );

        // Valid with explicit queryType and timeField
        assertTrue(
            tool
                .validate(
                    Map
                        .of(
                            "index",
                            "test_index",
                            "selectionTimeRangeStart",
                            "2025-01-15 10:00:00",
                            "selectionTimeRangeEnd",
                            "2025-01-15 11:00:00",
                            "queryType",
                            "ppl",
                            "timeField",
                            "timestamp"
                        )
                )
        );
    }

    @Test
    @SneakyThrows
    public void testDSLSingleAnalysis() {
        mockSearchResponse();
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("DSL Single Analysis Response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("singleAnalysis"));

                    // Verify the analysis contains field distribution data
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue(singleAnalysis.isJsonArray());
                    assertTrue(singleAnalysis.getAsJsonArray().size() > 0);
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testDSLWithFilter() {
        mockSearchResponse();
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl",
                        "filter",
                        "[\"{'term': {'status': 'error'}}\"]"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("DSL With Filter Response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("singleAnalysis"));

                    // Verify filter was applied (should still have analysis data)
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue(singleAnalysis.isJsonArray());
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testDSLWithMultipleFilters() {
        mockSearchResponse();
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl",
                        "filter",
                        "[\"{'term': {'status': 'error'}}\", \"{'range': {'level': {'gte': 3}}}\"]"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("DSL With Multiple Filters Response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("singleAnalysis"));

                    // Verify multiple filters were applied
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue(singleAnalysis.isJsonArray());
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testPPLSingleAnalysis() {
        String pplResponse =
            """
                {"schema":[{"name":"status","type":"keyword"},{"name":"level","type":"integer"},{"name":"host","type":"keyword"}],
                "datarows":[["error",3,"server-01"],["info",1,"server-02"],["warning",2,"server-03"],["error",4,"server-01"],["debug",1,"server-02"]],
                "total":5,"size":5}
                """;

        mockPPLInvocation(pplResponse);
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "ppl"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("PPL Single Analysis Response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("singleAnalysis"));

                    // Verify PPL data was processed correctly
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue(singleAnalysis.isJsonArray());
                    assertTrue(singleAnalysis.getAsJsonArray().size() > 0);
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testPPLWithCustomStatement() {
        String pplResponse = """
            {"schema":[{"name":"status","type":"keyword"},{"name":"host","type":"keyword"},{"name":"count","type":"long"}],
            "datarows":[["error","server-01",15],["error","server-02",8],["warning","server-01",3]],
            "total":3,"size":3}
            """;

        mockPPLInvocation(pplResponse);
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "ppl",
                        "ppl",
                        "source=logs-* | where status='error' | stats count() by host"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("PPL Custom Statement Response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("singleAnalysis"));

                    // Verify custom PPL statement was processed
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue(singleAnalysis.isJsonArray());
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testComparisonAnalysis() {
        // Mock different responses for baseline and selection data
        SearchHit[] baselineHits = createBaselineHits();
        SearchHit[] selectionHits = createSelectionHits();

        SearchHits baselineSearchHits = new SearchHits(baselineHits, new TotalHits(baselineHits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchHits selectionSearchHits = new SearchHits(
            selectionHits,
            new TotalHits(selectionHits.length, TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        // Mock sequential search calls - first selection, then baseline (based on new implementation)
        when(searchResponse.getHits())
            .thenReturn(selectionSearchHits)   // First call returns selection data
            .thenReturn(baselineSearchHits);   // Second call returns baseline data

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "baselineTimeRangeStart",
                        "2025-01-15 08:00:00",
                        "baselineTimeRangeEnd",
                        "2025-01-15 09:00:00",
                        "queryType",
                        "dsl"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("Comparison Analysis Response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("comparisonAnalysis"));

                    // Verify comparison analysis contains divergence data
                    JsonElement comparisonAnalysis = result.getAsJsonObject().get("comparisonAnalysis");
                    assertTrue(comparisonAnalysis.isJsonArray());
                    assertTrue(comparisonAnalysis.getAsJsonArray().size() > 0);
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testPPLComparisonAnalysis() {
        String baseResponse = """
            {"schema":[{"name":"status","type":"keyword"},{"name":"level","type":"integer"}],
            "datarows":[["info",1],["warning",2],["debug",1]],
            "total":3,"size":3}
            """;

        String selectionResponse = """
            {"schema":[{"name":"status","type":"keyword"},{"name":"level","type":"integer"}],
            "datarows":[["error",3],["error",4],["warning",2]],
            "total":3,"size":3}
            """;

        // Mock sequential PPL calls - first selection, then baseline (based on new implementation)
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onResponse(pplQueryResponse);
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        when(pplQueryResponse.getResult())
            .thenReturn(selectionResponse) // First call returns selection data
            .thenReturn(baseResponse);     // Second call returns baseline data

        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "baselineTimeRangeStart",
                        "2025-01-15 08:00:00",
                        "baselineTimeRangeEnd",
                        "2025-01-15 09:00:00",
                        "queryType",
                        "ppl",
                        "ppl",
                        "source=logs-* | where level > 1"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("PPL Comparison Analysis Response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("comparisonAnalysis"));

                    // Verify comparison shows differences between baseline and selection
                    JsonElement comparisonAnalysis = result.getAsJsonObject().get("comparisonAnalysis");
                    assertTrue(comparisonAnalysis.isJsonArray());
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithInvalidParameters() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap.of("index", "test_index"),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with invalid parameters"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("Missing required parameters"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithInvalidFilter() {
        mockSearchResponse();
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl",
                        "filter",
                        "invalid-json"
                    ),
                ActionListener.<String>wrap(response -> fail("Should have failed with invalid filter JSON"), e -> {
                    MatcherAssert.assertThat(e.getMessage(), containsString("Invalid 'filter' parameter"));
                    MatcherAssert.assertThat(e.getMessage(), containsString("must be a valid JSON array of strings"));
                })
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithCustomTimeField() {
        mockSearchResponse();
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "custom_timestamp",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("Custom Time Field Response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("singleAnalysis"));

                    // Verify custom time field was used
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue(singleAnalysis.isJsonArray());
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionFailedInSearch() {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onFailure(new Exception("Search execution failed"));
            return null;
        }).when(client).search(any(), any());

        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("Search execution failed"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionFailedInPPL() {
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onFailure(new Exception("PPL execution failed"));
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "ppl"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("PPL execution failed"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithEmptyPPLResponse() {
        String emptyResponse = "";
        mockPPLInvocation(emptyResponse);
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "ppl"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with empty response"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("Empty PPL response"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithPPLErrorResponse() {
        String errorResponse = "{\"error\":{\"type\":\"parsing_exception\",\"reason\":\"Syntax error in PPL query\"}}";
        mockPPLInvocation(errorResponse);
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "ppl"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with PPL error response"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("PPL query error"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithNoData() {
        // Mock empty search response
        SearchHit[] emptyHits = new SearchHit[0];
        SearchHits emptySearchHits = new SearchHits(emptyHits, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0.0f);
        when(searchResponse.getHits()).thenReturn(emptySearchHits);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with no data"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("No data found for selection time range"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithInvalidTimeFormat() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "invalid-time-format",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with invalid time format"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("Invalid time format"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithInvalidSize() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "selectionTimeRangeStart",
                        "2025-01-15 10:00:00",
                        "selectionTimeRangeEnd",
                        "2025-01-15 11:00:00",
                        "queryType",
                        "dsl",
                        "size",
                        "not-a-number"
                    ),
                ActionListener.<String>wrap(response -> fail("Should have failed with invalid size"), e -> {
                    MatcherAssert.assertThat(e.getMessage(), containsString("Invalid 'size' parameter"));
                    MatcherAssert.assertThat(e.getMessage(), containsString("must be a valid integer"));
                })
            );
    }

    private void setupMockMappings() {
        Map<String, Object> indexMappings = Map
            .of(
                "properties",
                Map
                    .of(
                        "status",
                        Map.of("type", "keyword"),
                        "level",
                        Map.of("type", "integer"),
                        "@timestamp",
                        Map.of("type", "date"),
                        "message",
                        Map.of("type", "text"),
                        "host",
                        Map.of("type", "keyword"),
                        "service",
                        Map.of("type", "keyword")
                    )
            );
        Map<String, MappingMetadata> mockedMappings = Map.of("test_index", mappingMetadata);

        when(mappingMetadata.getSourceAsMap()).thenReturn(indexMappings);
        when(getMappingsResponse.getMappings()).thenReturn(mockedMappings);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        // Mock the ActionFuture returned by getMappings
        org.opensearch.common.action.ActionFuture<GetMappingsResponse> mockActionFuture = mock(
            org.opensearch.common.action.ActionFuture.class
        );
        when(mockActionFuture.actionGet()).thenReturn(getMappingsResponse);
        when(indicesAdminClient.getMappings(any())).thenReturn(mockActionFuture);

        doAnswer(invocation -> {
            ActionListener<GetMappingsResponse> listener = (ActionListener<GetMappingsResponse>) invocation.getArguments()[1];
            listener.onResponse(getMappingsResponse);
            return null;
        }).when(indicesAdminClient).getMappings(any(), any());
    }

    private SearchHit[] createSampleHits() {
        SearchHit[] hits = new SearchHit[20];
        String[] statuses = { "error", "info", "warning", "debug" };
        String[] hosts = { "server-01", "server-02", "server-03" };
        String[] services = { "auth", "payment", "notification" };
        int[] levels = { 1, 2, 3, 4, 5 };

        for (int i = 0; i < 20; i++) {
            SearchHit hit = new SearchHit(i + 1);
            String status = statuses[i % statuses.length];
            String host = hosts[i % hosts.length];
            String service = services[i % services.length];
            int level = levels[i % levels.length];

            String source = String
                .format(
                    "{\"status\":\"%s\",\"level\":%d,\"@timestamp\":\"2025-01-15T10:%02d:00Z\",\"host\":\"%s\",\"service\":\"%s\",\"message\":\"Sample message %d\"}",
                    status,
                    level,
                    30 + i,
                    host,
                    service,
                    i
                );

            BytesReference sourceRef = new BytesArray(source);
            hit.sourceRef(sourceRef);
            hits[i] = hit;
        }

        return hits;
    }

    private SearchHit[] createBaselineHits() {
        SearchHit[] hits = new SearchHit[10];
        // Baseline data: mostly info and warning
        String[] statuses = { "info", "warning" };
        String[] hosts = { "server-01", "server-02" };
        String[] services = { "auth", "payment" };
        int[] levels = { 1, 2 };

        for (int i = 0; i < 10; i++) {
            SearchHit hit = new SearchHit(i + 1);
            String status = statuses[i % statuses.length];
            String host = hosts[i % hosts.length];
            String service = services[i % services.length];
            int level = levels[i % levels.length];

            String source = String
                .format(
                    "{\"status\":\"%s\",\"level\":%d,\"@timestamp\":\"2025-01-15T08:%02d:00Z\",\"host\":\"%s\",\"service\":\"%s\",\"message\":\"Baseline message %d\"}",
                    status,
                    level,
                    30 + i,
                    host,
                    service,
                    i
                );

            BytesReference sourceRef = new BytesArray(source);
            hit.sourceRef(sourceRef);
            hits[i] = hit;
        }

        return hits;
    }

    private SearchHit[] createSelectionHits() {
        SearchHit[] hits = new SearchHit[10];
        // Selection data: mostly error and debug (different from baseline)
        String[] statuses = { "error", "debug" };
        String[] hosts = { "server-02", "server-03" };
        String[] services = { "payment", "notification" };
        int[] levels = { 3, 4, 5 };

        for (int i = 0; i < 10; i++) {
            SearchHit hit = new SearchHit(i + 1);
            String status = statuses[i % statuses.length];
            String host = hosts[i % hosts.length];
            String service = services[i % services.length];
            int level = levels[i % levels.length];

            String source = String
                .format(
                    "{\"status\":\"%s\",\"level\":%d,\"@timestamp\":\"2025-01-15T10:%02d:00Z\",\"host\":\"%s\",\"service\":\"%s\",\"message\":\"Selection message %d\"}",
                    status,
                    level,
                    30 + i,
                    host,
                    service,
                    i
                );

            BytesReference sourceRef = new BytesArray(source);
            hit.sourceRef(sourceRef);
            hits[i] = hit;
        }

        return hits;
    }
}
