/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.hamcrest.Matchers.containsString;
import static org.jsoup.helper.Validate.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;

import lombok.SneakyThrows;

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
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue("Response should contain singleAnalysis", result.getAsJsonObject().has("singleAnalysis"));

                    // Verify the analysis contains field distribution data
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
                    assertTrue("singleAnalysis should contain at least one field analysis", singleAnalysis.getAsJsonArray().size() > 0);

                    // Verify each field analysis has required structure (SummaryDataItem)
                    for (int i = 0; i < singleAnalysis.getAsJsonArray().size(); i++) {
                        JsonElement fieldAnalysis = singleAnalysis.getAsJsonArray().get(i);
                        assertTrue("Field analysis should be a JSON object", fieldAnalysis.isJsonObject());
                        assertTrue("Field analysis should have 'field' property", fieldAnalysis.getAsJsonObject().has("field"));
                        assertTrue("Field analysis should have 'divergence' property", fieldAnalysis.getAsJsonObject().has("divergence"));
                        assertTrue("Field analysis should have 'topChanges' property", fieldAnalysis.getAsJsonObject().has("topChanges"));
                        assertNotNull("Field name should not be null", fieldAnalysis.getAsJsonObject().get("field").getAsString());
                        assertTrue("TopChanges should be a JSON array", fieldAnalysis.getAsJsonObject().get("topChanges").isJsonArray());
                    }
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
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue("Response should contain singleAnalysis", result.getAsJsonObject().has("singleAnalysis"));

                    // Verify filter was applied (should still have analysis data)
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
                    assertTrue("singleAnalysis should contain field analyses even with filter", singleAnalysis.getAsJsonArray().size() > 0);

                    // Verify structure is maintained with filter
                    JsonElement firstField = singleAnalysis.getAsJsonArray().get(0);
                    assertTrue("Field analysis should have proper structure with filter", firstField.getAsJsonObject().has("field"));
                    assertTrue("Field analysis should have topChanges with filter", firstField.getAsJsonObject().has("topChanges"));
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
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue("Response should contain singleAnalysis", result.getAsJsonObject().has("singleAnalysis"));

                    // Verify multiple filters were applied
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
                    assertTrue("singleAnalysis should contain analyses with multiple filters", singleAnalysis.getAsJsonArray().size() > 0);

                    // Verify each field has proper structure with multiple filters
                    for (int i = 0; i < singleAnalysis.getAsJsonArray().size(); i++) {
                        JsonElement fieldAnalysis = singleAnalysis.getAsJsonArray().get(i);
                        assertTrue(
                            "Field analysis should maintain structure with multiple filters",
                            fieldAnalysis.getAsJsonObject().has("field")
                        );
                        assertTrue(
                            "Field analysis should have topChanges with multiple filters",
                            fieldAnalysis.getAsJsonObject().has("topChanges")
                        );
                    }
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
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue("Response should contain singleAnalysis", result.getAsJsonObject().has("singleAnalysis"));

                    // Verify PPL data was processed correctly
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
                    assertTrue("Should have at least one field from PPL response", singleAnalysis.getAsJsonArray().size() > 0);

                    // Verify each field has proper structure
                    for (int i = 0; i < singleAnalysis.getAsJsonArray().size(); i++) {
                        JsonElement fieldAnalysis = singleAnalysis.getAsJsonArray().get(i);
                        assertTrue("Field analysis should have field property", fieldAnalysis.getAsJsonObject().has("field"));
                        assertTrue("Field analysis should have divergence property", fieldAnalysis.getAsJsonObject().has("divergence"));
                        assertTrue("Field analysis should have topChanges property", fieldAnalysis.getAsJsonObject().has("topChanges"));
                        assertTrue("TopChanges should be an array", fieldAnalysis.getAsJsonObject().get("topChanges").isJsonArray());
                    }
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
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue("Response should contain singleAnalysis", result.getAsJsonObject().has("singleAnalysis"));

                    // Verify custom PPL statement was processed
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
                    assertTrue("Should have at least one field from custom PPL response", singleAnalysis.getAsJsonArray().size() > 0);

                    // Verify each field has proper structure
                    for (int i = 0; i < singleAnalysis.getAsJsonArray().size(); i++) {
                        JsonElement fieldAnalysis = singleAnalysis.getAsJsonArray().get(i);
                        assertTrue("Field analysis should have field property", fieldAnalysis.getAsJsonObject().has("field"));
                        assertTrue("Field analysis should have divergence property", fieldAnalysis.getAsJsonObject().has("divergence"));
                        assertTrue("Field analysis should have topChanges property", fieldAnalysis.getAsJsonObject().has("topChanges"));
                    }
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
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue("Response should contain comparisonAnalysis", result.getAsJsonObject().has("comparisonAnalysis"));

                    // Verify comparison analysis contains divergence data
                    JsonElement comparisonAnalysis = result.getAsJsonObject().get("comparisonAnalysis");
                    assertTrue("comparisonAnalysis should be a JSON array", comparisonAnalysis.isJsonArray());
                    assertTrue("comparisonAnalysis should contain field comparisons", comparisonAnalysis.getAsJsonArray().size() > 0);

                    // Verify each comparison has required structure (SummaryDataItem)
                    for (int i = 0; i < comparisonAnalysis.getAsJsonArray().size(); i++) {
                        JsonElement fieldComparison = comparisonAnalysis.getAsJsonArray().get(i);
                        assertTrue("Field comparison should be a JSON object", fieldComparison.isJsonObject());
                        assertTrue("Field comparison should have 'field' property", fieldComparison.getAsJsonObject().has("field"));
                        assertTrue(
                            "Field comparison should have 'divergence' property",
                            fieldComparison.getAsJsonObject().has("divergence")
                        );
                        assertTrue(
                            "Field comparison should have 'topChanges' property",
                            fieldComparison.getAsJsonObject().has("topChanges")
                        );

                        // Verify divergence is a valid number
                        assertTrue("Divergence should be a number", fieldComparison.getAsJsonObject().get("divergence").isJsonPrimitive());
                        double divergence = fieldComparison.getAsJsonObject().get("divergence").getAsDouble();
                        assertTrue("Divergence should be non-negative", divergence >= 0.0);

                        // Verify topChanges structure
                        JsonElement topChanges = fieldComparison.getAsJsonObject().get("topChanges");
                        assertTrue("TopChanges should be a JSON array", topChanges.isJsonArray());
                    }
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
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue("Response should contain comparisonAnalysis", result.getAsJsonObject().has("comparisonAnalysis"));

                    // Verify comparison shows differences between baseline and selection
                    JsonElement comparisonAnalysis = result.getAsJsonObject().get("comparisonAnalysis");
                    assertTrue("comparisonAnalysis should be a JSON array", comparisonAnalysis.isJsonArray());
                    assertTrue("Should have at least one field from PPL comparison", comparisonAnalysis.getAsJsonArray().size() > 0);

                    // Verify each field has proper structure
                    for (int i = 0; i < comparisonAnalysis.getAsJsonArray().size(); i++) {
                        JsonElement fieldComparison = comparisonAnalysis.getAsJsonArray().get(i);
                        assertTrue("Field comparison should have field property", fieldComparison.getAsJsonObject().has("field"));
                        assertTrue("Field comparison should have divergence property", fieldComparison.getAsJsonObject().has("divergence"));
                        assertTrue("Field comparison should have topChanges property", fieldComparison.getAsJsonObject().has("topChanges"));

                        // Verify divergence is a valid number
                        double divergence = fieldComparison.getAsJsonObject().get("divergence").getAsDouble();
                        assertTrue("Divergence should be non-negative", divergence >= 0.0);
                    }
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
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue("Response should contain singleAnalysis", result.getAsJsonObject().has("singleAnalysis"));

                    // Verify custom time field was used
                    JsonElement singleAnalysis = result.getAsJsonObject().get("singleAnalysis");
                    assertTrue("singleAnalysis should be a JSON array", singleAnalysis.isJsonArray());
                    assertTrue(
                        "singleAnalysis should contain field analyses with custom time field",
                        singleAnalysis.getAsJsonArray().size() > 0
                    );

                    // Verify that the custom time field doesn't appear in the analysis (it's used for filtering, not analysis)
                    for (int i = 0; i < singleAnalysis.getAsJsonArray().size(); i++) {
                        JsonElement fieldAnalysis = singleAnalysis.getAsJsonArray().get(i);
                        String fieldName = fieldAnalysis.getAsJsonObject().get("field").getAsString();
                        assertFalse("Custom time field should not appear in analysis results", "custom_timestamp".equals(fieldName));
                        assertTrue("Field analysis should have topChanges property", fieldAnalysis.getAsJsonObject().has("topChanges"));
                    }
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
        when(mockActionFuture.actionGet(anyLong())).thenReturn(getMappingsResponse);
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

    @Test
    @SneakyThrows
    public void testGetUsefulFieldsWithValidMapping() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        List<Map<String, Object>> testData = createTestDataForFieldAnalysis();

        java.lang.reflect.Method getUsefulFieldsMethod = DataDistributionTool.class
            .getDeclaredMethod("getUsefulFields", List.class, String.class);
        getUsefulFieldsMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> usefulFields = (List<String>) getUsefulFieldsMethod.invoke(tool, testData, "test_index");

        assertNotNull(usefulFields);
        assertFalse(usefulFields.isEmpty());
        assertTrue(usefulFields.contains("status"));
        assertTrue(usefulFields.contains("level"));
        assertTrue(usefulFields.contains("host"));
        assertTrue(usefulFields.contains("service"));
        assertFalse(usefulFields.contains("@timestamp"));
    }

    @Test
    @SneakyThrows
    public void testGetUsefulFieldsWithEmptyMapping() {
        when(getMappingsResponse.getMappings()).thenReturn(Map.of());
        
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        List<Map<String, Object>> testData = createTestDataForFieldAnalysis();
        
        java.lang.reflect.Method getUsefulFieldsMethod = DataDistributionTool.class
            .getDeclaredMethod("getUsefulFields", List.class, String.class);
        getUsefulFieldsMethod.setAccessible(true);
        
        @SuppressWarnings("unchecked")
        List<String> usefulFields = (List<String>) getUsefulFieldsMethod.invoke(tool, testData, "test_index");
        
        assertNotNull(usefulFields);
        assertTrue(usefulFields.size() > 0);
        assertFalse(usefulFields.contains("@timestamp"));
    }

    @Test
    @SneakyThrows
    public void testGetUsefulFieldsWithMappingException() {
        when(client.admin().indices().getMappings(any())).thenThrow(new RuntimeException("Mapping retrieval failed"));
        
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        List<Map<String, Object>> testData = createTestDataForFieldAnalysis();
        
        java.lang.reflect.Method getUsefulFieldsMethod = DataDistributionTool.class
            .getDeclaredMethod("getUsefulFields", List.class, String.class);
        getUsefulFieldsMethod.setAccessible(true);
        
        @SuppressWarnings("unchecked")
        List<String> usefulFields = (List<String>) getUsefulFieldsMethod.invoke(tool, testData, "test_index");
        
        assertNotNull(usefulFields);
        assertTrue(usefulFields.size() > 0);
        assertFalse(usefulFields.contains("@timestamp"));
        assertFalse(usefulFields.contains("_id"));
        assertFalse(usefulFields.contains("_index"));
    }

    @Test
    @SneakyThrows
    public void testGetUsefulFieldsWithHighCardinalityFields() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        List<Map<String, Object>> testData = createHighCardinalityTestData();

        java.lang.reflect.Method getUsefulFieldsMethod = DataDistributionTool.class
            .getDeclaredMethod("getUsefulFields", List.class, String.class);
        getUsefulFieldsMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> usefulFields = (List<String>) getUsefulFieldsMethod.invoke(tool, testData, "test_index");

        assertNotNull(usefulFields);
        assertFalse(usefulFields.contains("unique_id"));
        assertTrue(usefulFields.contains("status"));
    }

    @Test
    @SneakyThrows
    public void testGetUsefulFieldsWithEmptyData() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        List<Map<String, Object>> emptyData = List.of();

        java.lang.reflect.Method getUsefulFieldsMethod = DataDistributionTool.class
            .getDeclaredMethod("getUsefulFields", List.class, String.class);
        getUsefulFieldsMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<String> usefulFields = (List<String>) getUsefulFieldsMethod.invoke(tool, emptyData, "test_index");

        assertNotNull(usefulFields);
        assertTrue(usefulFields.size() > 0);
    }

    private List<Map<String, Object>> createTestDataForFieldAnalysis() {
        List<Map<String, Object>> data = new ArrayList<>();
        String[] statuses = { "error", "info", "warning" };
        String[] hosts = { "server-01", "server-02" };
        String[] services = { "auth", "payment" };

        for (int i = 0; i < 10; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("status", statuses[i % statuses.length]);
            doc.put("level", i % 5 + 1);
            doc.put("host", hosts[i % hosts.length]);
            doc.put("service", services[i % services.length]);
            doc.put("@timestamp", "2025-01-15T10:" + String.format("%02d", 30 + i) + ":00Z");
            doc.put("message", "Test message " + i);
            data.add(doc);
        }
        return data;
    }

    private List<Map<String, Object>> createHighCardinalityTestData() {
        List<Map<String, Object>> data = new ArrayList<>();
        String[] statuses = { "error", "info" };

        for (int i = 0; i < 20; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("status", statuses[i % statuses.length]);
            doc.put("unique_id", "id_" + i);
            doc.put("@timestamp", "2025-01-15T10:" + String.format("%02d", 30 + i) + ":00Z");
            data.add(doc);
        }
        return data;
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithTermQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("status", Map.of("term", "error"));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("term"));
        assertTrue(queryBuilder.toString().contains("status"));
        assertTrue(queryBuilder.toString().contains("error"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithRangeQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("level", Map.of("range", Map.of("gte", 3, "lte", 5)));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("range"));
        assertTrue(queryBuilder.toString().contains("level"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithMatchQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("message", Map.of("match", "test message"));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("match"));
        assertTrue(queryBuilder.toString().contains("message"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithExistsQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("status", Map.of("exists", true));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("exists"));
        assertTrue(queryBuilder.toString().contains("status"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithDirectTermQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("status", "error");
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("term"));
        assertTrue(queryBuilder.toString().contains("status"));
        assertTrue(queryBuilder.toString().contains("error"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithMatchPhraseQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("message", Map.of("match_phrase", "exact phrase"));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("match_phrase"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithPrefixQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("host", Map.of("prefix", "server"));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("prefix"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithWildcardQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("host", Map.of("wildcard", "server*"));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("wildcard"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithWildcardMapQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("host", Map.of("wildcard", Map.of("value", "server*")));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("wildcard"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithRegexpQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("host", Map.of("regexp", "server-[0-9]+"));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("regexp"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithRegexpMapQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("host", Map.of("regexp", Map.of("value", "server-[0-9]+")));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("regexp"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithComplexRangeQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("level", Map.of("range", Map.of("gt", 1, "lt", 10)));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
        assertTrue(queryBuilder.toString().contains("range"));
    }

    @Test
    @SneakyThrows
    public void testBuildQueryFromMapWithUnsupportedOperator() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method buildQueryMethod = DataDistributionTool.class
            .getDeclaredMethod("buildQueryFromMap", Map.class, org.opensearch.index.query.BoolQueryBuilder.class);
        buildQueryMethod.setAccessible(true);

        Map<String, Object> filterMap = Map.of("status", Map.of("unsupported_op", "value"));
        org.opensearch.index.query.BoolQueryBuilder queryBuilder = org.opensearch.index.query.QueryBuilders.boolQuery();

        buildQueryMethod.invoke(tool, filterMap, queryBuilder);

        assertNotNull(queryBuilder);
    }

    @Test
    @SneakyThrows
    public void testGroupNumericKeysWithManyNumericValues() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method groupNumericKeysMethod = DataDistributionTool.class
            .getDeclaredMethod("groupNumericKeys", Map.class, Map.class);
        groupNumericKeysMethod.setAccessible(true);

        Map<String, Double> selectionDist = new HashMap<>();
        Map<String, Double> baselineDist = new HashMap<>();

        for (int i = 1; i <= 15; i++) {
            selectionDist.put(String.valueOf(i), 0.1);
            baselineDist.put(String.valueOf(i + 5), 0.1);
        }

        Object result = groupNumericKeysMethod.invoke(tool, selectionDist, baselineDist);

        assertNotNull(result);
        java.lang.reflect.Method groupedSelectionDistMethod = result.getClass().getDeclaredMethod("groupedSelectionDist");
        @SuppressWarnings("unchecked")
        Map<String, Double> groupedSelection = (Map<String, Double>) groupedSelectionDistMethod.invoke(result);

        assertEquals(5, groupedSelection.size());
        assertTrue(groupedSelection.keySet().stream().allMatch(key -> key.contains("-")));
    }

    @Test
    @SneakyThrows
    public void testGroupNumericKeysWithFewNumericValues() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method groupNumericKeysMethod = DataDistributionTool.class
            .getDeclaredMethod("groupNumericKeys", Map.class, Map.class);
        groupNumericKeysMethod.setAccessible(true);

        Map<String, Double> selectionDist = Map.of("1", 0.3, "2", 0.4, "3", 0.3);
        Map<String, Double> baselineDist = Map.of("1", 0.2, "2", 0.5, "3", 0.3);

        Object result = groupNumericKeysMethod.invoke(tool, selectionDist, baselineDist);

        assertNotNull(result);
        java.lang.reflect.Method groupedSelectionDistMethod = result.getClass().getDeclaredMethod("groupedSelectionDist");
        @SuppressWarnings("unchecked")
        Map<String, Double> groupedSelection = (Map<String, Double>) groupedSelectionDistMethod.invoke(result);

        assertEquals(selectionDist, groupedSelection);
    }

    @Test
    @SneakyThrows
    public void testGroupNumericKeysWithNonNumericValues() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method groupNumericKeysMethod = DataDistributionTool.class
            .getDeclaredMethod("groupNumericKeys", Map.class, Map.class);
        groupNumericKeysMethod.setAccessible(true);

        Map<String, Double> selectionDist = new HashMap<>();
        Map<String, Double> baselineDist = new HashMap<>();

        for (int i = 1; i <= 15; i++) {
            selectionDist.put(String.valueOf(i), 0.1);
        }
        selectionDist.put("error", 0.2);
        selectionDist.put("warning", 0.3);

        Object result = groupNumericKeysMethod.invoke(tool, selectionDist, baselineDist);

        assertNotNull(result);
        java.lang.reflect.Method groupedSelectionDistMethod = result.getClass().getDeclaredMethod("groupedSelectionDist");
        @SuppressWarnings("unchecked")
        Map<String, Double> groupedSelection = (Map<String, Double>) groupedSelectionDistMethod.invoke(result);

        assertEquals(selectionDist, groupedSelection);
    }

    @Test
    @SneakyThrows
    public void testGetNumberFieldsWithValidMapping() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);

        java.lang.reflect.Method getNumberFieldsMethod = DataDistributionTool.class.getDeclaredMethod("getNumberFields", String.class);
        getNumberFieldsMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        java.util.Set<String> numberFields = (java.util.Set<String>) getNumberFieldsMethod.invoke(tool, "test_index");

        assertNotNull(numberFields);
        assertTrue(numberFields.contains("level"));
        assertFalse(numberFields.contains("status"));
        assertFalse(numberFields.contains("host"));
    }

    @Test
    @SneakyThrows
    public void testGetNumberFieldsWithEmptyMapping() {
        when(getMappingsResponse.getMappings()).thenReturn(Map.of());
        
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        
        java.lang.reflect.Method getNumberFieldsMethod = DataDistributionTool.class
            .getDeclaredMethod("getNumberFields", String.class);
        getNumberFieldsMethod.setAccessible(true);
        
        @SuppressWarnings("unchecked")
        java.util.Set<String> numberFields = (java.util.Set<String>) getNumberFieldsMethod.invoke(tool, "test_index");
        
        assertNotNull(numberFields);
        assertTrue(numberFields.isEmpty());
    }

    @Test
    @SneakyThrows
    public void testGetNumberFieldsWithMappingException() {
        when(client.admin().indices().getMappings(any())).thenThrow(new RuntimeException("Mapping failed"));
        
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        
        java.lang.reflect.Method getNumberFieldsMethod = DataDistributionTool.class
            .getDeclaredMethod("getNumberFields", String.class);
        getNumberFieldsMethod.setAccessible(true);
        
        @SuppressWarnings("unchecked")
        java.util.Set<String> numberFields = (java.util.Set<String>) getNumberFieldsMethod.invoke(tool, "test_index");
        
        assertNotNull(numberFields);
        assertTrue(numberFields.isEmpty());
    }

    @Test
    @SneakyThrows
    public void testGetNumberFieldsWithNullActionFuture() {
        when(client.admin().indices().getMappings(any())).thenReturn(null);
        
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        
        java.lang.reflect.Method getNumberFieldsMethod = DataDistributionTool.class
            .getDeclaredMethod("getNumberFields", String.class);
        getNumberFieldsMethod.setAccessible(true);
        
        @SuppressWarnings("unchecked")
        java.util.Set<String> numberFields = (java.util.Set<String>) getNumberFieldsMethod.invoke(tool, "test_index");
        
        assertNotNull(numberFields);
        assertTrue(numberFields.isEmpty());
    }

    @Test
    @SneakyThrows
    public void testGetPPLQueryWithTimeRangeEmptyQuery() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        
        java.lang.reflect.Method getPPLQueryWithTimeRangeMethod = DataDistributionTool.class
            .getDeclaredMethod("getPPLQueryWithTimeRange", String.class, String.class, String.class, String.class);
        getPPLQueryWithTimeRangeMethod.setAccessible(true);
        
        String result = (String) getPPLQueryWithTimeRangeMethod.invoke(tool, "", "2025-01-15 10:00:00", "2025-01-15 11:00:00", "@timestamp");
        
        assertEquals("WHERE `@timestamp` >= '2025-01-15 10:00:00' AND `@timestamp` <= '2025-01-15 11:00:00'", result);
    }


    @Test
    @SneakyThrows
    public void testGetPPLQueryWithTimeRangeEmptyTimeField() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        
        java.lang.reflect.Method getPPLQueryWithTimeRangeMethod = DataDistributionTool.class
            .getDeclaredMethod("getPPLQueryWithTimeRange", String.class, String.class, String.class, String.class);
        getPPLQueryWithTimeRangeMethod.setAccessible(true);
        
        String result = (String) getPPLQueryWithTimeRangeMethod.invoke(tool, "source=logs-*", "2025-01-15 10:00:00", "2025-01-15 11:00:00", "");

        assertEquals("source=logs-*", result);
    }

    @Test
    @SneakyThrows
    public void testGetPPLQueryWithTimeRangeExistingWhere() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        
        java.lang.reflect.Method getPPLQueryWithTimeRangeMethod = DataDistributionTool.class
            .getDeclaredMethod("getPPLQueryWithTimeRange", String.class, String.class, String.class, String.class);
        getPPLQueryWithTimeRangeMethod.setAccessible(true);
        
        String result = (String) getPPLQueryWithTimeRangeMethod.invoke(tool, "source=logs-* | where status='error'", "2025-01-15 10:00:00", "2025-01-15 11:00:00", "@timestamp");
        
        assertEquals("source=logs-* | where status='error' AND `@timestamp` >= '2025-01-15 10:00:00' AND `@timestamp` <= '2025-01-15 11:00:00'", result);
    }

    @Test
    @SneakyThrows
    public void testGetPPLQueryWithTimeRangeNoExistingWhere() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        
        java.lang.reflect.Method getPPLQueryWithTimeRangeMethod = DataDistributionTool.class
            .getDeclaredMethod("getPPLQueryWithTimeRange", String.class, String.class, String.class, String.class);
        getPPLQueryWithTimeRangeMethod.setAccessible(true);
        
        String result = (String) getPPLQueryWithTimeRangeMethod.invoke(tool, "source=logs-* | stats count() by status", "2025-01-15 10:00:00", "2025-01-15 11:00:00", "@timestamp");
        
        assertEquals("source=logs-* | WHERE `@timestamp` >= '2025-01-15 10:00:00' AND `@timestamp` <= '2025-01-15 11:00:00' | stats count() by status", result);
    }


    @Test
    @SneakyThrows
    public void testGetPPLQueryWithTimeRangeMultipleWhereClausesFirstOne() {
        DataDistributionTool tool = DataDistributionTool.Factory.getInstance().create(params);
        
        java.lang.reflect.Method getPPLQueryWithTimeRangeMethod = DataDistributionTool.class
            .getDeclaredMethod("getPPLQueryWithTimeRange", String.class, String.class, String.class, String.class);
        getPPLQueryWithTimeRangeMethod.setAccessible(true);
        
        String result = (String) getPPLQueryWithTimeRangeMethod.invoke(tool, "source=logs-* | where status='error' | stats count() | where count > 10", "2025-01-15 10:00:00", "2025-01-15 11:00:00", "@timestamp");
        
        assertEquals("source=logs-* | where status='error' AND `@timestamp` >= '2025-01-15 10:00:00' AND `@timestamp` <= '2025-01-15 11:00:00' | stats count() | where count > 10", result);
    }
}