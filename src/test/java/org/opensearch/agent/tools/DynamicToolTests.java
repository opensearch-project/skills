/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ml.common.CommonValue.TOOL_INPUT_SCHEMA_FIELD;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.rest.DynamicRestRequestCreator;
import org.opensearch.rest.DynamicToolExecutor;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableMap;

public class DynamicToolTests {

    @Mock
    private DynamicToolExecutor dynamicToolExecutor;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private ThreadContext threadContext;
    @Mock
    private Client client;
    @Mock
    private NamedXContentRegistry xContentRegistry;
    @Mock
    private ActionListener<String> listener;
    @Mock
    private DynamicRestRequestCreator dynamicRestRequestCreator;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(threadContext.getHeaders()).thenReturn(ImmutableMap.of());

        BytesReference mockRequestBody = BytesReference
            .fromByteBuffer(ByteBuffer.wrap("mock request body".getBytes(StandardCharsets.UTF_8)));
        RestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(mockRequestBody, MediaType.fromMediaType(XContentType.JSON.mediaType()))
            .build();
        when(dynamicRestRequestCreator.createRestRequest(any(), any(), any(), any(), any())).thenReturn(restRequest);
        DynamicTool.Factory.getInstance().init(client, dynamicToolExecutor, dynamicRestRequestCreator, xContentRegistry);
    }

    @Test
    public void test_createTool_successful() {
        DynamicTool tool = DynamicTool.Factory.getInstance().create(ImmutableMap.of("uri", "/my_index/_search", "method", "POST"));
        assertNotNull(tool);
    }

    @Test
    public void test_createTool_missUri() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> DynamicTool.Factory.getInstance().create(ImmutableMap.of())
        );
        assertEquals("valid uri is required in DynamicTool configuration!", exception.getMessage());
    }

    @Test
    public void test_createTool_invalidUri() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> DynamicTool.Factory.getInstance().create(ImmutableMap.of("uri", ""))
        );
        assertEquals("valid uri is required in DynamicTool configuration!", exception.getMessage());
    }

    @Test
    public void test_createTool_missMethod() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> DynamicTool.Factory.getInstance().create(ImmutableMap.of("uri", "/my_index/_search"))
        );
        assertEquals("method is required and not null in DynamicTool configuration!", exception.getMessage());
    }

    @Test
    public void test_createTool_invalidMethod() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> DynamicTool.Factory.getInstance().create(ImmutableMap.of("uri", "/my_index/_search", "method", "NULL"))
        );
        assertEquals("valid method value is required in DynamicTool configuration!", exception.getMessage());
    }

    @Test
    public void test_run_withoutResponseFilter_successful() throws Exception {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        registerAgentParameters.put("request_body", "{\"query\": {\"match\": {\"name\": \"${parameters.search_content}\"}}}");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        registerAgentParameters.put("search_content", "test");
        doAnswer(invocationOnMock -> {
            ActionListener<RestResponse> actionListener = invocationOnMock.getArgument(1);
            RestResponse restResponse = mock(RestResponse.class);
            BytesReference mockResponseBody = BytesReference
                .fromByteBuffer(ByteBuffer.wrap("mock response body".getBytes(StandardCharsets.UTF_8)));
            when(restResponse.content()).thenReturn(mockResponseBody);
            actionListener.onResponse(restResponse);
            return null;
        }).when(dynamicToolExecutor).execute(any(), isA(ActionListener.class));
        tool.run(StringUtils.getParameterMap(registerAgentParameters), listener);
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener).onResponse(argumentCaptor.capture());
        assertNotNull(argumentCaptor.getValue());
        assertEquals("mock response body", argumentCaptor.getValue());
    }

    @Test
    public void test_run_requestBodyNullOrNotExist_successful() throws Exception {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        registerAgentParameters.put("request_body", null);
        DynamicTool tool0 = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        assertNotNull(tool0);
        registerAgentParameters.remove("request_body");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        registerAgentParameters.put("search_content", "test");
        doAnswer(invocationOnMock -> {
            ActionListener<RestResponse> actionListener = invocationOnMock.getArgument(1);
            RestResponse restResponse = mock(RestResponse.class);
            BytesReference mockResponseBody = BytesReference
                .fromByteBuffer(ByteBuffer.wrap("mock response body".getBytes(StandardCharsets.UTF_8)));
            when(restResponse.content()).thenReturn(mockResponseBody);
            actionListener.onResponse(restResponse);
            return null;
        }).when(dynamicToolExecutor).execute(any(), isA(ActionListener.class));
        tool.run(StringUtils.getParameterMap(registerAgentParameters), listener);
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener).onResponse(argumentCaptor.capture());
        assertNotNull(argumentCaptor.getValue());
        assertEquals("mock response body", argumentCaptor.getValue());
    }

    @Test
    public void test_run_withResponseFilter_successful() throws Exception {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        registerAgentParameters.put("request_body", "{\"query\": {\"match\": {\"name\": \"${parameters.search_content}\"}}}");
        registerAgentParameters.put("response_filter", "$.name");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        registerAgentParameters.put("search_content", "test");
        doAnswer(invocationOnMock -> {
            ActionListener<RestResponse> actionListener = invocationOnMock.getArgument(1);
            RestResponse restResponse = mock(RestResponse.class);
            BytesReference mockResponseBody = BytesReference
                .fromByteBuffer(ByteBuffer.wrap("{\"name\": \"This is a mock value\"}".getBytes(StandardCharsets.UTF_8)));
            when(restResponse.content()).thenReturn(mockResponseBody);
            actionListener.onResponse(restResponse);
            return null;
        }).when(dynamicToolExecutor).execute(any(), isA(ActionListener.class));
        tool.run(StringUtils.getParameterMap(registerAgentParameters), listener);
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(listener).onResponse(argumentCaptor.capture());
        assertNotNull(argumentCaptor.getValue());
        assertEquals("This is a mock value", argumentCaptor.getValue());
    }

    @Test
    public void test_run_failureOnToolExecutor() throws Exception {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        registerAgentParameters.put("request_body", "{\"query\": {\"match\": {\"name\": \"${parameters.search_content}\"}}}");
        registerAgentParameters.put("response_filter", "$.name");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        registerAgentParameters.put("search_content", "test");
        doAnswer(invocationOnMock -> {
            ActionListener<RestResponse> actionListener = invocationOnMock.getArgument(1);
            actionListener.onFailure(new RuntimeException("System Error"));
            return null;
        }).when(dynamicToolExecutor).execute(any(), isA(ActionListener.class));
        tool.run(StringUtils.getParameterMap(registerAgentParameters), listener);
        ArgumentCaptor<RuntimeException> argumentCaptor = ArgumentCaptor.forClass(RuntimeException.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertNotNull(argumentCaptor.getValue());
        assertEquals("System Error", argumentCaptor.getValue().getMessage());
    }

    @Test
    public void test_run_exceptionOnToolExecutor() throws Exception {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        registerAgentParameters.put("request_body", "{\"query\": {\"match\": {\"name\": \"${parameters.search_content}\"}}}");
        registerAgentParameters.put("response_filter", "$.name");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        registerAgentParameters.put("search_content", "test");
        doThrow(new RuntimeException("System Error")).when(dynamicToolExecutor).execute(any(), any());
        tool.run(StringUtils.getParameterMap(registerAgentParameters), listener);
        ArgumentCaptor<RuntimeException> argumentCaptor = ArgumentCaptor.forClass(RuntimeException.class);
        verify(listener).onFailure(argumentCaptor.capture());
        assertNotNull(argumentCaptor.getValue());
        assertEquals("System Error", argumentCaptor.getValue().getMessage());
    }

    @Test
    public void test_factory_getDefaultDescription() {
        String description = DynamicTool.Factory.getInstance().getDefaultDescription();
        assertNotNull(description);
        assertEquals(
            "This is a template tool to enable OpenSearch APIs as tool, this tool accepts several parameters: uri, method, request_body and response_filter. uri represents the OpenSearch API uri, method represents the"
                + "OpenSearch API method, request_body represents the OpenSearch API request body and response_filter is a json path expression so that target fields can be extracted from OpenSearch API response. Most OpenSearch APIs"
                + "can be configured with this tool, during agent execution the configured API will be invoked and the response/filtered response will be returned as tool's response.",
            description
        );
    }

    @Test
    public void test_factory_getDefaultType() {
        String type = DynamicTool.Factory.getInstance().getDefaultType();
        assertNotNull(type);
        assertEquals("DynamicTool", type);
    }

    @Test
    public void test_factory_getDefaultVersion() {
        String version = DynamicTool.Factory.getInstance().getDefaultVersion();
        assertNull(version);
    }

    @Test
    public void test_tool_getType() {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        assertEquals("DynamicTool", tool.getType());
    }

    @Test
    public void test_tool_getVersion() {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        assertNull(tool.getVersion());
    }

    @Test
    public void test_tool_getName() {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        tool.setName("test");
        assertEquals("test", tool.getName());
    }

    @Test
    public void test_tool_getDescription() {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        assertEquals(
            "This is a template tool to enable OpenSearch APIs as tool, this tool accepts several parameters: uri, method, request_body and response_filter. uri represents the OpenSearch API uri, method represents the"
                + "OpenSearch API method, request_body represents the OpenSearch API request body and response_filter is a json path expression so that target fields can be extracted from OpenSearch API response. Most OpenSearch APIs"
                + "can be configured with this tool, during agent execution the configured API will be invoked and the response/filtered response will be returned as tool's response.",
            tool.getDescription()
        );
    }

    @Test
    public void test_tool_getAttributes() {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        tool.setAttributes(ImmutableMap.of("test_input", "{}"));
        Map<String, Object> attributes = tool.getAttributes();
        assertNotNull(attributes);
        assertEquals(ImmutableMap.of("test_input", "{}"), attributes.get(TOOL_INPUT_SCHEMA_FIELD));
    }

    @Test
    public void test_tool_setDescription() {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        tool.setDescription("test description");
        assertEquals("test description", tool.getDescription());
    }

    @Test
    public void test_tool_validate() {
        Map<String, Object> registerAgentParameters = new HashMap<>();
        registerAgentParameters.put("uri", "/my_index/_search");
        registerAgentParameters.put("method", "POST");
        DynamicTool tool = DynamicTool.Factory.getInstance().create(registerAgentParameters);
        Map<String, String> runtimeParameters = ImmutableMap.of("search_content", "test");
        assertTrue(tool.validate(runtimeParameters));
    }

}
