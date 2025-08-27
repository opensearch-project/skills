/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;

import lombok.SneakyThrows;

public class LogPatternAnalysisToolTests {

    private Map<String, Object> params = new HashMap<>();
    private final Client client = mock(Client.class);
    @Mock
    private TransportPPLQueryResponse pplQueryResponse;

    @SneakyThrows
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        LogPatternAnalysisTool.Factory.getInstance().init(client);
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
        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);
        assertEquals("LogPatternAnalysisTool", tool.getType());
        assertEquals("LogPatternAnalysisTool", tool.getName());
        assertEquals(LogPatternAnalysisTool.Factory.getInstance().getDefaultDescription(), tool.getDescription());
        assertNull(LogPatternAnalysisTool.Factory.getInstance().getDefaultVersion());
    }

    @Test
    public void testValidate() {
        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        // Valid parameters
        assertTrue(
            tool
                .validate(
                    Map
                        .of(
                            "index",
                            "test_index",
                            "timeField",
                            "@timestamp",
                            "logFieldName",
                            "message",
                            "selectionTimeRangeStart",
                            "2025-01-01T00:00:00Z",
                            "selectionTimeRangeEnd",
                            "2025-01-01T01:00:00Z"
                        )
                )
        );

        // Missing required parameters
        assertFalse(tool.validate(Map.of("index", "test_index")));
        assertFalse(tool.validate(Map.of()));
    }

    @Test
    @SneakyThrows
    public void testLogInsightExecution() {
        String pplResponse =
            """
                {"schema":[{"name":"patterns_field","type":"string"},{"name":"pattern_count","type":"long"},{"name":"sample_logs","type":"array"}],
                "datarows":[["Error in processing <*>",5,["Error in processing request","Error in processing data"]],
                ["Failed to connect <*>",3,["Failed to connect to database","Failed to connect to server"]]],
                "total":2,"size":2}
                """;

        mockPPLInvocation(pplResponse);
        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "selectionTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println(response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("logInsights"));
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testLogPatternDiffAnalysis() {
        // Mock different responses for base and selection time ranges
        String baseResponse = """
            {"schema":[{"name":"cnt","type":"long"},{"name":"patterns_field","type":"string"}],
            "datarows":[[100,"User login successful"],[20,"Database query executed"],[10,"Cache hit"]],
            "total":3,"size":3}
            """;

        String selectionResponse =
            """
                {"schema":[{"name":"cnt","type":"long"},{"name":"patterns_field","type":"string"}],
                "datarows":[[50,"User login successful"],[80,"Error in authentication <*>"],[15,"Connection timeout <*>"],[5,"Database query executed"]],
                "total":4,"size":4}
                """;

        // Mock sequential PPL calls - first base, then selection
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onResponse(pplQueryResponse);
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        when(pplQueryResponse.getResult())
            .thenReturn(baseResponse)  // First call returns base data
            .thenReturn(selectionResponse);  // Second call returns selection data

        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "baseTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "baseTimeRangeEnd",
                        "2025-01-01T01:00:00Z",
                        "selectionTimeRangeStart",
                        "2025-01-01T01:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T02:00:00Z"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("Pattern diff response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("patternMapDifference"));
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testLogSequenceAnalysis() {
        // Mock different responses for base and selection time ranges
        String baseResponse =
            """
                {"schema":[{"name":"traceId","type":"string"},{"name":"patterns_field","type":"string"},{"name":"@timestamp","type":"timestamp"}],
                "datarows":[["trace1","User login attempt","2025-01-01T00:00:00Z"],["trace1","Authentication successful","2025-01-01T00:00:01Z"],["trace1","Session created","2025-01-01T00:00:02Z"],
                ["trace2","User login attempt","2025-01-01T00:00:10Z"],["trace2","Authentication successful","2025-01-01T00:00:11Z"],["trace2","Session created","2025-01-01T00:00:12Z"]],
                "total":6,"size":6}
                """;

        String selectionResponse =
            """
                {"schema":[{"name":"traceId","type":"string"},{"name":"patterns_field","type":"string"},{"name":"@timestamp","type":"timestamp"}],
                "datarows":[["trace3","User login attempt","2025-01-01T01:00:00Z"],["trace3","Authentication failed","2025-01-01T01:00:01Z"],["trace3","Account locked","2025-01-01T01:00:02Z"],
                ["trace4","Database connection timeout","2025-01-01T01:00:10Z"],["trace4","Retry connection","2025-01-01T01:00:11Z"],["trace4","Connection failed","2025-01-01T01:00:12Z"],
                ["trace5","User login attempt","2025-01-01T01:00:20Z"],["trace5","Authentication successful","2025-01-01T01:00:21Z"],["trace5","Session created","2025-01-01T01:00:22Z"]],
                "total":9,"size":9}
                """;

        // Mock sequential PPL calls - first base, then selection
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onResponse(pplQueryResponse);
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        when(pplQueryResponse.getResult())
            .thenReturn(baseResponse)      // First call returns base data
            .thenReturn(selectionResponse); // Second call returns selection data

        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "traceFieldName",
                        "traceId",
                        "baseTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "baseTimeRangeEnd",
                        "2025-01-01T01:00:00Z",
                        "selectionTimeRangeStart",
                        "2025-01-01T01:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T02:00:00Z"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("Sequence analysis response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("BASE") || result.getAsJsonObject().has("EXCEPTIONAL"));
                }, e -> fail("Tool execution failed: " + e.getMessage()))
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithInvalidParameters() {
        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

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
    public void testExecutionWithEmptyPPLResponse() {
        String emptyResponse = """
            {"schema":[],"datarows":[],"total":0,"size":0}
            """;

        mockPPLInvocation(emptyResponse);
        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "selectionTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
                    ),
                ActionListener.<String>wrap(response -> {
                    System.out.println("response: " + response);
                    JsonElement result = gson.fromJson(response, JsonElement.class);
                    assertTrue(result.getAsJsonObject().has("logInsights"));
                }, e -> fail("Tool execution failed: " + e.getMessage()))
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

        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "selectionTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("PPL execution failed:"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithIndexNotFound() {
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onFailure(new Exception("IndexNotFoundException: no such index"));
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "nonexistent_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "selectionTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with IndexNotFoundException"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("IndexNotFoundException"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithEmptyPPLResult() {
        String emptyResponse = "";
        mockPPLInvocation(emptyResponse);
        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "selectionTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
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
    public void testExecutionWithInvalidPPLResponse() {
        String invalidResponse = "{\"invalid\":\"response\"}";
        mockPPLInvocation(invalidResponse);
        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "selectionTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with invalid response"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("Invalid PPL response"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithNonExistentIndex() {
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onFailure(new Exception("no such index [nonexistent_index]"));
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "nonexistent_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "selectionTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with non-existent index"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("no such index"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithNonExistentLogField() {
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onFailure(new Exception("Unknown field [nonexistent_field]"));
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "nonexistent_field",
                        "selectionTimeRangeStart",
                        "2025-01-01T00:00:00Z",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with non-existent field"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("Unknown field"))
                    )
            );
    }

    @Test
    @SneakyThrows
    public void testExecutionWithInvalidTimeFormat() {
        doAnswer(invocation -> {
            ActionListener<TransportPPLQueryResponse> listener = (ActionListener<TransportPPLQueryResponse>) invocation.getArguments()[2];
            listener.onFailure(new Exception("Invalid date format: invalid-time-format"));
            return null;
        }).when(client).execute(eq(PPLQueryAction.INSTANCE), any(), any());

        LogPatternAnalysisTool tool = LogPatternAnalysisTool.Factory.getInstance().create(params);

        tool
            .run(
                ImmutableMap
                    .of(
                        "index",
                        "test_index",
                        "timeField",
                        "@timestamp",
                        "logFieldName",
                        "message",
                        "selectionTimeRangeStart",
                        "invalid-time-format",
                        "selectionTimeRangeEnd",
                        "2025-01-01T01:00:00Z"
                    ),
                ActionListener
                    .<String>wrap(
                        response -> fail("Should have failed with invalid time format"),
                        e -> MatcherAssert.assertThat(e.getMessage(), containsString("Invalid date format"))
                    )
            );
    }
}
