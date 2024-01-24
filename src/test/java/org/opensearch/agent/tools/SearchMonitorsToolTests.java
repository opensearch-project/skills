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

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.client.AdminClient;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.alerting.action.GetMonitorResponse;
import org.opensearch.commons.alerting.model.CronSchedule;
import org.opensearch.commons.alerting.model.DataSources;
import org.opensearch.commons.alerting.model.Monitor;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;

public class SearchMonitorsToolTests {
    @Mock
    private NodeClient nodeClient;
    @Mock
    private AdminClient adminClient;
    @Mock
    private IndicesAdminClient indicesAdminClient;
    @Mock
    private ClusterAdminClient clusterAdminClient;

    private Map<String, String> nullParams;
    private Map<String, String> emptyParams;
    private Map<String, String> nonEmptyParams;
    private Map<String, String> monitorIdParams;

    private Monitor testMonitor;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        SearchMonitorsTool.Factory.getInstance().init(nodeClient);

        nullParams = null;
        emptyParams = Collections.emptyMap();
        nonEmptyParams = Map.of("monitorName", "foo");
        monitorIdParams = Map.of("monitorId", "foo");
        testMonitor = new Monitor(
            "monitor-1-id",
            0L,
            "monitor-1",
            true,
            new CronSchedule("31 * * * *", ZoneId.of("Asia/Kolkata"), null),
            Instant.now(),
            Instant.now(),
            Monitor.MonitorType.QUERY_LEVEL_MONITOR,
            new User("test-user", Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
            0,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            new DataSources(),
            ""
        );
    }

    @Test
    public void testRunWithNoMonitors() throws Exception {
        Tool tool = SearchMonitorsTool.Factory.getInstance().create(Collections.emptyMap());

        SearchHit[] hits = new SearchHit[0];

        TotalHits totalHits = new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO);

        SearchResponse getMonitorsResponse = new SearchResponse(
            new SearchResponseSections(new SearchHits(hits, totalHits, 0), new Aggregations(new ArrayList<>()), null, false, null, null, 0),
            null,
            0,
            0,
            0,
            0,
            null,
            null
        );
        String expectedResponseStr = String.format("Monitors=[]TotalMonitors=%d", hits.length);

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getMonitorsResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(emptyParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testRunWithMonitorId() throws Exception {
        Tool tool = SearchMonitorsTool.Factory.getInstance().create(Collections.emptyMap());

        GetMonitorResponse getMonitorResponse = new GetMonitorResponse(
            testMonitor.getId(),
            1L,
            2L,
            0L,
            testMonitor,
            Collections.emptyList()
        );
        String expectedResponseStr = getExpectedResponseString(testMonitor);
        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<GetMonitorResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getMonitorResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(monitorIdParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testRunWithMonitorIdNotFound() throws Exception {
        Tool tool = SearchMonitorsTool.Factory.getInstance().create(Collections.emptyMap());

        GetMonitorResponse responseWithNullMonitor = new GetMonitorResponse(testMonitor.getId(), 1L, 2L, 0L, null, Collections.emptyList());
        String expectedResponseStr = String.format("Monitors=[]TotalMonitors=0");

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<GetMonitorResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(responseWithNullMonitor);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(monitorIdParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testRunWithSingleMonitor() throws Exception {
        Tool tool = SearchMonitorsTool.Factory.getInstance().create(Collections.emptyMap());

        XContentBuilder content = XContentBuilder.builder(XContentType.JSON.xContent());
        content.startObject();
        content.field("name", testMonitor.getName());
        content.field("type", testMonitor.getType());
        content.field("enabled", Boolean.toString(testMonitor.getEnabled()));
        content.field("enabled_time", Long.toString(testMonitor.getEnabledTime().toEpochMilli()));
        content.field("last_update_time", Long.toString(testMonitor.getLastUpdateTime().toEpochMilli()));
        content.endObject();
        SearchHit[] hits = new SearchHit[1];
        hits[0] = new SearchHit(0, testMonitor.getId(), null, null).sourceRef(BytesReference.bytes(content));

        TotalHits totalHits = new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO);

        SearchResponse getMonitorsResponse = new SearchResponse(
            new SearchResponseSections(new SearchHits(hits, totalHits, 0), new Aggregations(new ArrayList<>()), null, false, null, null, 0),
            null,
            0,
            0,
            0,
            0,
            null,
            null
        );
        String expectedResponseStr = getExpectedResponseString(testMonitor);

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getMonitorsResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(emptyParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testParseParams() throws Exception {
        Tool tool = SearchMonitorsTool.Factory.getInstance().create(Collections.emptyMap());
        Map<String, String> validParams = new HashMap<String, String>();
        validParams.put("monitorName", "foo");
        validParams.put("enabled", "true");
        validParams.put("hasTriggers", "true");
        validParams.put("indices", "bar");
        validParams.put("sortOrder", "ASC");
        validParams.put("sortString", "baz");
        validParams.put("size", "10");
        validParams.put("startIndex", "0");

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        assertDoesNotThrow(() -> tool.run(validParams, listener));
        assertDoesNotThrow(() -> tool.run(Map.of("hasTriggers", "false"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("monitorNamePattern", "foo*"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("detectorId", "foo"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("sortOrder", "AsC"), listener));
    }

    @Test
    public void testValidate() {
        Tool tool = SearchMonitorsTool.Factory.getInstance().create(Collections.emptyMap());
        assertEquals(SearchMonitorsTool.TYPE, tool.getType());
        assertTrue(tool.validate(emptyParams));
        assertTrue(tool.validate(nonEmptyParams));
        assertTrue(tool.validate(monitorIdParams));
        assertTrue(tool.validate(nullParams));
    }

    private String getExpectedResponseString(Monitor testMonitor) {
        return String
            .format(
                "Monitors=[{id=%s,name=%s,type=%s,enabled=%s,enabledTime=%d,lastUpdateTime=%d}]TotalMonitors=%d",
                testMonitor.getId(),
                testMonitor.getName(),
                testMonitor.getType(),
                testMonitor.getEnabled(),
                testMonitor.getEnabledTime().toEpochMilli(),
                testMonitor.getLastUpdateTime().toEpochMilli(),
                1
            );

    }
}
