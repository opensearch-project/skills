/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.agent.TestHelpers;
import org.opensearch.agent.tools.utils.ToolConstants.DetectorStateString;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.search.SearchHit;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;

public class SearchAnomalyDetectorsToolTests {
    @Mock
    private NamedWriteableRegistry namedWriteableRegistry;
    @Mock
    private NodeClient nodeClient;

    private Map<String, String> nullParams;
    private Map<String, String> emptyParams;
    private Map<String, String> nonEmptyParams;

    private AnomalyDetector testDetector;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        SearchAnomalyDetectorsTool.Factory.getInstance().init(nodeClient, namedWriteableRegistry);

        nullParams = null;
        emptyParams = Collections.emptyMap();
        nonEmptyParams = Map.of("detectorName", "foo");

        testDetector = new AnomalyDetector(
            "foo-id",
            1L,
            "foo-name",
            "foo-description",
            "foo-time-field",
            new ArrayList<String>(Arrays.asList("foo-index")),
            Collections.emptyList(),
            null,
            new IntervalTimeConfiguration(5, ChronoUnit.MINUTES),
            null,
            1,
            Collections.emptyMap(),
            1,
            Instant.now(),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    @Test
    public void testRunWithNoDetectors() throws Exception {
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(new SearchHit[0]);
        String expectedResponseStr = String.format(Locale.getDefault(), "AnomalyDetectors=[]TotalAnomalyDetectors=0");

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getDetectorsResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(emptyParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testRunWithSingleAnomalyDetector() throws Exception {
        final String detectorName = "detector-1";
        final String detectorId = "detector-1-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        XContentBuilder content = XContentBuilder.builder(XContentType.JSON.xContent());
        content.startObject();
        content.field("name", testDetector.getName());
        content.field("detector_type", testDetector.getDetectorType());
        content.field("description", testDetector.getDescription());
        content.field("indices", testDetector.getIndices().get(0));
        content.field("last_update_time", testDetector.getLastUpdateTime().toEpochMilli());
        content.endObject();
        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(0, testDetector.getId(), null, null).sourceRef(BytesReference.bytes(content));
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        String expectedResponseStr = getExpectedResponseString(testDetector);

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getDetectorsResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(emptyParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testRunWithRunningDetectorTrue() throws Exception {
        final String detectorName = "detector-1";
        final String detectorId = "detector-1-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[1];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName, detectorId);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(new String[] { detectorName }, new String[] { DetectorStateString.Running.name() });
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("running", "true"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        String response = responseCaptor.getValue();
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName)));
        assertTrue(response.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", hits.length)));
    }

    @Test
    public void testRunWithRunningDetectorFalse() throws Exception {
        final String detectorName = "detector-1";
        final String detectorId = "detector-1-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[1];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName, detectorId);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(new String[] { detectorName }, new String[] { DetectorStateString.Running.name() });
        String expectedResponseStr = "AnomalyDetectors=[]TotalAnomalyDetectors=0";
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("running", "false"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testRunWithRunningDetectorUndefined() throws Exception {
        final String detectorName = "detector-1";
        final String detectorId = "detector-1-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[1];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName, detectorId);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(new String[] { detectorName }, new String[] { DetectorStateString.Running.name() });
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("foo", "bar"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        String response = responseCaptor.getValue();
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName)));
        assertTrue(response.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", hits.length)));
    }

    @Test
    public void testRunWithNullRealtimeTask() throws Exception {
        final String detectorName = "detector-1";
        final String detectorId = "detector-1-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[1];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName, detectorId);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(new String[] { detectorName }, new String[] { DetectorStateString.Running.name() });
        // Overriding the mocked response to realtime task and setting to null. This occurs when
        // a detector is created but is never started.
        when(getDetectorProfileResponse.getRealtimeAdTask()).thenReturn(null);
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("running", "false"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        String response = responseCaptor.getValue();
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName)));
        assertTrue(response.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", hits.length)));
    }

    @Test
    public void testRunWithTaskStateCreated() throws Exception {
        final String detectorName = "detector-1";
        final String detectorId = "detector-1-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[1];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName, detectorId);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(new String[] { detectorName }, new String[] { DetectorStateString.Running.name() });
        // Overriding the mocked response to set realtime task state to CREATED
        when(getDetectorProfileResponse.getRealtimeAdTask().getState()).thenReturn("CREATED");
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("running", "true"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        String response = responseCaptor.getValue();
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName)));
        assertTrue(response.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", hits.length)));
    }

    @Test
    public void testRunWithTaskStateVariousFailed() throws Exception {
        final String detectorName1 = "detector-1";
        final String detectorId1 = "detector-1-id";
        final String detectorName2 = "detector-2";
        final String detectorId2 = "detector-2-id";
        final String detectorName3 = "detector-3";
        final String detectorId3 = "detector-3-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[3];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName1, detectorId1);
        hits[1] = TestHelpers.generateSearchDetectorHit(detectorName2, detectorId2);
        hits[2] = TestHelpers.generateSearchDetectorHit(detectorName3, detectorId3);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(
                new String[] { detectorName1, detectorName2, detectorName3 },
                new String[] { "INIT_FAILURE", "UNEXPECTED_FAILURE", "FAILED" }
            );
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("failed", "true"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        String response = responseCaptor.getValue();
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId1)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName1)));
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId2)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName2)));
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId3)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName3)));
        assertTrue(response.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", hits.length)));
    }

    @Test
    public void testRunWithCombinedDetectorStatesTrue() throws Exception {
        final String detectorName1 = "detector-1";
        final String detectorId1 = "detector-1-id";
        final String detectorName2 = "detector-2";
        final String detectorId2 = "detector-2-id";
        final String detectorName3 = "detector-3";
        final String detectorId3 = "detector-3-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[3];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName1, detectorId1);
        hits[1] = TestHelpers.generateSearchDetectorHit(detectorName2, detectorId2);
        hits[2] = TestHelpers.generateSearchDetectorHit(detectorName3, detectorId3);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(
                new String[] { detectorName1, detectorName2, detectorName3 },
                new String[] { DetectorStateString.Running.name(), DetectorStateString.Disabled.name(), DetectorStateString.Failed.name() }
            );
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("running", "true", "failed", "true"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        String response = responseCaptor.getValue();
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId1)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName1)));
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId3)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName3)));
        assertTrue(response.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 2)));
    }

    @Test
    public void testRunWithCombinedDetectorStatesFalse() throws Exception {
        final String detectorName1 = "detector-1";
        final String detectorId1 = "detector-1-id";
        final String detectorName2 = "detector-2";
        final String detectorId2 = "detector-2-id";
        final String detectorName3 = "detector-3";
        final String detectorId3 = "detector-3-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[3];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName1, detectorId1);
        hits[1] = TestHelpers.generateSearchDetectorHit(detectorName2, detectorId2);
        hits[2] = TestHelpers.generateSearchDetectorHit(detectorName3, detectorId3);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(
                new String[] { detectorName1, detectorName2, detectorName3 },
                new String[] { DetectorStateString.Running.name(), DetectorStateString.Disabled.name(), DetectorStateString.Failed.name() }
            );
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("running", "false", "failed", "false"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertTrue(responseCaptor.getValue().contains("TotalAnomalyDetectors=1"));
    }

    @Test
    public void testRunWithCombinedDetectorStatesMixed() throws Exception {
        final String detectorName1 = "detector-1";
        final String detectorId1 = "detector-1-id";
        final String detectorName2 = "detector-2";
        final String detectorId2 = "detector-2-id";
        final String detectorName3 = "detector-3";
        final String detectorId3 = "detector-3-id";
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());

        // Generate mock values and responses
        SearchHit[] hits = new SearchHit[3];
        hits[0] = TestHelpers.generateSearchDetectorHit(detectorName1, detectorId1);
        hits[1] = TestHelpers.generateSearchDetectorHit(detectorName2, detectorId2);
        hits[2] = TestHelpers.generateSearchDetectorHit(detectorName3, detectorId3);
        SearchResponse getDetectorsResponse = TestHelpers.generateSearchResponse(hits);
        GetAnomalyDetectorResponse getDetectorProfileResponse = TestHelpers
            .generateGetAnomalyDetectorResponses(
                new String[] { detectorName1, detectorName2, detectorName3 },
                new String[] { DetectorStateString.Running.name(), DetectorStateString.Disabled.name(), DetectorStateString.Failed.name() }
            );
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        mockProfileApiCalls(getDetectorsResponse, getDetectorProfileResponse);

        tool.run(Map.of("running", "true", "failed", "false"), listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        String response = responseCaptor.getValue();
        assertTrue(response.contains(String.format(Locale.ROOT, "id=%s", detectorId1)));
        assertTrue(response.contains(String.format(Locale.ROOT, "name=%s", detectorName1)));
        assertTrue(response.contains(String.format(Locale.ROOT, "TotalAnomalyDetectors=%d", 1)));
    }

    @Test
    public void testParseParams() throws Exception {
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());
        Map<String, String> validParams = new HashMap<String, String>();
        validParams.put("detectorName", "foo");
        validParams.put("indices", "foo");
        validParams.put("highCardinality", "false");
        validParams.put("lastUpdateTime", "1234");
        validParams.put("sortOrder", "foo");
        validParams.put("size", "10");
        validParams.put("startIndex", "0");
        validParams.put("running", "false");

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        assertDoesNotThrow(() -> tool.run(validParams, listener));
        assertDoesNotThrow(() -> tool.run(Map.of("detectorNamePattern", "foo*"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("sortOrder", "AsC"), listener));
    }

    @Test
    public void testValidate() {
        Tool tool = SearchAnomalyDetectorsTool.Factory.getInstance().create(Collections.emptyMap());
        assertEquals(SearchAnomalyDetectorsTool.TYPE, tool.getType());
        assertTrue(tool.validate(emptyParams));
        assertTrue(tool.validate(nonEmptyParams));
        assertTrue(tool.validate(nullParams));
    }

    private void mockProfileApiCalls(SearchResponse getDetectorsResponse, GetAnomalyDetectorResponse getDetectorProfileResponse) {
        // Mock return from initial search call
        doAnswer((invocation) -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getDetectorsResponse);
            return null;
        }).when(nodeClient).execute(any(SearchAnomalyDetectorAction.class), any(), any());

        // Mock return from secondary detector profile call
        doAnswer((invocation) -> {
            ActionListener<GetAnomalyDetectorResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getDetectorProfileResponse);
            return null;
        }).when(nodeClient).execute(any(GetAnomalyDetectorAction.class), any(), any());
    }

    private String getExpectedResponseString(AnomalyDetector testDetector) {
        return String
            .format(
                "AnomalyDetectors=[{id=%s,name=%s,type=%s,description=%s,index=%s,lastUpdateTime=%d}]TotalAnomalyDetectors=%d",
                testDetector.getId(),
                testDetector.getName(),
                testDetector.getDetectorType(),
                testDetector.getDescription(),
                testDetector.getIndices().get(0),
                testDetector.getLastUpdateTime().toEpochMilli(),
                1
            );

    }
}
