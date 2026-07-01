/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.ml.common.CommonValue.TOOL_INPUT_SCHEMA_FIELD;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionType;
import org.opensearch.commons.alerting.action.GetAlertsResponse;
import org.opensearch.commons.alerting.model.Alert;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.ClusterAdminClient;
import org.opensearch.transport.client.IndicesAdminClient;
import org.opensearch.transport.client.node.NodeClient;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class SearchAlertsToolTests {
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

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        SearchAlertsTool.Factory.getInstance().init(nodeClient);

        nullParams = null;
        emptyParams = Collections.emptyMap();
        nonEmptyParams = Map.of("searchString", "foo");
    }

    @Test
    public void testRunWithNoAlerts() throws Exception {
        Tool tool = SearchAlertsTool.Factory.getInstance().create(Collections.emptyMap());
        GetAlertsResponse getAlertsResponse = new GetAlertsResponse(Collections.emptyList(), 0);
        String expectedResponseStr = "Alerts=[]TotalAlerts=0";

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<GetAlertsResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getAlertsResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(nonEmptyParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testRunWithAlerts() throws Exception {
        Tool tool = SearchAlertsTool.Factory.getInstance().create(Collections.emptyMap());
        Alert alert1 = new Alert(
            "alert-id-1",
            1234,
            1,
            "monitor-id",
            "workflow-id",
            "workflow-name",
            "monitor-name",
            1234,
            null,
            "trigger-id",
            "trigger-name",
            Collections.emptyList(),
            Collections.emptyList(),
            Alert.State.ACKNOWLEDGED,
            Instant.now(),
            null,
            null,
            null,
            null,
            Collections.emptyList(),
            "test-severity",
            Collections.emptyList(),
            null,
            null,
            Collections.emptyList(),
            null
        );
        Alert alert2 = new Alert(
            "alert-id-2",
            1234,
            1,
            "monitor-id",
            "workflow-id",
            "workflow-name",
            "monitor-name",
            1234,
            null,
            "trigger-id",
            "trigger-name",
            Collections.emptyList(),
            Collections.emptyList(),
            Alert.State.ACKNOWLEDGED,
            Instant.now(),
            null,
            null,
            null,
            null,
            Collections.emptyList(),
            "test-severity",
            Collections.emptyList(),
            null,
            null,
            Collections.emptyList(),
            null
        );
        List<Alert> mockAlerts = List.of(alert1, alert2);

        GetAlertsResponse getAlertsResponse = new GetAlertsResponse(mockAlerts, mockAlerts.size());
        String expectedResponseStr = new StringBuilder()
            .append("Alerts=[")
            .append(alert1.toString())
            .append(alert2.toString())
            .append("]TotalAlerts=2")
            .toString();

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<GetAlertsResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(getAlertsResponse);
            return null;
        }).when(nodeClient).execute(any(ActionType.class), any(), any());

        tool.run(nonEmptyParams, listener);
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(expectedResponseStr, responseCaptor.getValue());
    }

    @Test
    public void testParseParams() throws Exception {
        Tool tool = SearchAlertsTool.Factory.getInstance().create(Collections.emptyMap());
        Map<String, String> validParams = new HashMap<String, String>();
        validParams.put("sortOrder", "asc");
        validParams.put("sortString", "foo.bar");
        validParams.put("size", "10");
        validParams.put("startIndex", "0");
        validParams.put("searchString", "foo");
        validParams.put("severityLevel", "ALL");
        validParams.put("alertState", "ALL");
        validParams.put("monitorId", "foo");
        validParams.put("alertIndex", "foo");

        @SuppressWarnings("unchecked")
        ActionListener<String> listener = Mockito.mock(ActionListener.class);
        assertDoesNotThrow(() -> tool.run(validParams, listener));
        assertDoesNotThrow(() -> tool.run(Map.of("monitorIds", "[]"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("monitorIds", "[foo]"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("workflowIds", "[]"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("workflowIds", "[foo]"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("alertIds", "[]"), listener));
        assertDoesNotThrow(() -> tool.run(Map.of("alertIds", "[foo]"), listener));
    }

    @Test
    public void testValidate() {
        Tool tool = SearchAlertsTool.Factory.getInstance().create(Collections.emptyMap());
        assertEquals(SearchAlertsTool.TYPE, tool.getType());
        assertTrue(tool.validate(emptyParams));
        assertTrue(tool.validate(nonEmptyParams));
        assertTrue(tool.validate(nullParams));
    }

    // ========== Input Schema Tests ==========

    @Test
    public void testDefaultInputSchemaIsValidJson() {
        Gson gson = new Gson();
        Map<String, Object> schema = gson.fromJson(
            SearchAlertsTool.DEFAULT_INPUT_SCHEMA,
            new TypeToken<Map<String, Object>>() {}.getType()
        );
        assertNotNull("DEFAULT_INPUT_SCHEMA must parse to a non-null map", schema);
        assertEquals("object", schema.get("type"));
    }

    @Test
    public void testDefaultInputSchemaContainsAllProperties() {
        Gson gson = new Gson();
        Map<String, Object> schema = gson.fromJson(
            SearchAlertsTool.DEFAULT_INPUT_SCHEMA,
            new TypeToken<Map<String, Object>>() {}.getType()
        );
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
        assertNotNull(properties);

        Set<String> expectedProperties = Set.of(
            "alertIds",
            "alertIndex",
            "monitorId",
            "monitorIds",
            "workflowIds",
            "alertState",
            "severityLevel",
            "searchString",
            "sortOrder",
            "sortString",
            "size",
            "startIndex"
        );
        assertEquals("Schema must define exactly 12 properties", 12, properties.size());
        for (String key : expectedProperties) {
            assertTrue("Schema must contain property: " + key, properties.containsKey(key));
        }
    }

    @Test
    public void testDefaultInputSchemaPropertyTypes() {
        Gson gson = new Gson();
        Map<String, Object> schema = gson.fromJson(
            SearchAlertsTool.DEFAULT_INPUT_SCHEMA,
            new TypeToken<Map<String, Object>>() {}.getType()
        );
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> properties = (Map<String, Map<String, Object>>) schema.get("properties");

        assertEquals("string", properties.get("alertIndex").get("type"));
        assertEquals("string", properties.get("monitorId").get("type"));
        assertEquals("string", properties.get("alertState").get("type"));
        assertEquals("string", properties.get("severityLevel").get("type"));
        assertEquals("string", properties.get("searchString").get("type"));
        assertEquals("string", properties.get("sortOrder").get("type"));
        assertEquals("string", properties.get("sortString").get("type"));

        assertEquals("array", properties.get("alertIds").get("type"));
        assertEquals("array", properties.get("monitorIds").get("type"));
        assertEquals("array", properties.get("workflowIds").get("type"));

        assertEquals("integer", properties.get("size").get("type"));
        assertEquals("integer", properties.get("startIndex").get("type"));
    }

    @Test
    public void testDefaultInputSchemaRequiredIsEmpty() {
        Gson gson = new Gson();
        Map<String, Object> schema = gson.fromJson(
            SearchAlertsTool.DEFAULT_INPUT_SCHEMA,
            new TypeToken<Map<String, Object>>() {}.getType()
        );
        @SuppressWarnings("unchecked")
        List<Object> required = (List<Object>) schema.get("required");
        assertNotNull(required);
        assertTrue("required array must be empty", required.isEmpty());
    }

    @Test
    public void testDefaultInputSchemaAdditionalPropertiesFalse() {
        Gson gson = new Gson();
        Map<String, Object> schema = gson.fromJson(
            SearchAlertsTool.DEFAULT_INPUT_SCHEMA,
            new TypeToken<Map<String, Object>>() {}.getType()
        );
        assertEquals(Boolean.FALSE, schema.get("additionalProperties"));
    }

    // ========== Attributes Tests ==========

    @Test
    public void testDefaultAttributesContainsInputSchema() {
        assertNotNull(SearchAlertsTool.DEFAULT_ATTRIBUTES);
        assertTrue(
            "DEFAULT_ATTRIBUTES must contain key '" + TOOL_INPUT_SCHEMA_FIELD + "'",
            SearchAlertsTool.DEFAULT_ATTRIBUTES.containsKey(TOOL_INPUT_SCHEMA_FIELD)
        );
        assertEquals(SearchAlertsTool.DEFAULT_INPUT_SCHEMA, SearchAlertsTool.DEFAULT_ATTRIBUTES.get(TOOL_INPUT_SCHEMA_FIELD));
    }

    @Test
    public void testToolInstanceAttributesMatchDefaults() {
        SearchAlertsTool tool = (SearchAlertsTool) SearchAlertsTool.Factory.getInstance().create(Collections.emptyMap());
        assertNotNull(tool.getAttributes());
        assertTrue(tool.getAttributes().containsKey(TOOL_INPUT_SCHEMA_FIELD));
        assertEquals(SearchAlertsTool.DEFAULT_INPUT_SCHEMA, tool.getAttributes().get(TOOL_INPUT_SCHEMA_FIELD));
    }

    @Test
    public void testToolInstanceAttributesCanBeOverridden() {
        SearchAlertsTool tool = (SearchAlertsTool) SearchAlertsTool.Factory.getInstance().create(Collections.emptyMap());
        Map<String, Object> customAttributes = Map.of(TOOL_INPUT_SCHEMA_FIELD, "custom-schema");
        tool.setAttributes(customAttributes);
        assertEquals("custom-schema", tool.getAttributes().get(TOOL_INPUT_SCHEMA_FIELD));
    }

    @Test
    public void testFactoryDefaultAttributes() {
        SearchAlertsTool.Factory factory = SearchAlertsTool.Factory.getInstance();
        assertNotNull(factory.getDefaultAttributes());
        assertTrue(factory.getDefaultAttributes().containsKey(TOOL_INPUT_SCHEMA_FIELD));
        assertEquals(SearchAlertsTool.DEFAULT_INPUT_SCHEMA, factory.getDefaultAttributes().get(TOOL_INPUT_SCHEMA_FIELD));
    }
}
