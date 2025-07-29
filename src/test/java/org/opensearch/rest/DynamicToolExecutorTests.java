/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableMap;

public class DynamicToolExecutorTests {
    @Mock
    private AtomicReference<RestController> restControllerRef;
    @Mock
    private RestController restController;
    @Mock
    private NodeClient client;
    @Mock
    private ActionListener<RestResponse> listener;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(restControllerRef.get()).thenReturn(restController);
    }

    @Test
    public void test_constructor() {
        DynamicToolExecutor executor = new DynamicToolExecutor(restControllerRef, client);
        assertNotNull(executor);
    }

    @Test
    public void test_execute_successful() throws Exception {
        Optional<RestHandler> restHandler = Optional.of((request, channel, client) -> channel.sendResponse(new RestResponse() {
            @Override
            public String contentType() {
                return "text/plain";
            }

            @Override
            public BytesReference content() {
                return BytesReference.fromByteBuffer(ByteBuffer.wrap("mock response body".getBytes(StandardCharsets.UTF_8)));
            }

            @Override
            public RestStatus status() {
                return RestStatus.OK;
            }
        }));
        when(restController.dispatchHandler(any(), any(), any(), any())).thenReturn(restHandler);
        BytesReference mockRequestBody = BytesReference
            .fromByteBuffer(ByteBuffer.wrap("mock request body".getBytes(StandardCharsets.UTF_8)));
        RestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/my_index/_search")
            .withParams(ImmutableMap.of("allow_no_indices", "true"))
            .withContent(mockRequestBody, MediaType.fromMediaType(XContentType.JSON.mediaType()))
            .build();
        new DynamicToolExecutor(restControllerRef, client).execute(restRequest, listener);
        ArgumentCaptor<RestResponse> argumentCaptor = ArgumentCaptor.forClass(RestResponse.class);
        verify(listener).onResponse(argumentCaptor.capture());
        assertNotNull(argumentCaptor.getValue());
    }

    @Test
    public void test_execute_restHandlerNotFound() throws Exception {
        Optional<RestHandler> restHandler = Optional.empty();
        when(restController.dispatchHandler(any(), any(), any(), any())).thenReturn(restHandler);
        BytesReference mockRequestBody = BytesReference
            .fromByteBuffer(ByteBuffer.wrap("mock request body".getBytes(StandardCharsets.UTF_8)));
        RestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/my_index/_search")
            .withParams(ImmutableMap.of("allow_no_indices", "true"))
            .withContent(mockRequestBody, MediaType.fromMediaType(XContentType.JSON.mediaType()))
            .build();
        new DynamicToolExecutor(restControllerRef, client).execute(restRequest, listener);
        ArgumentCaptor<RestResponse> argumentCaptor = ArgumentCaptor.forClass(RestResponse.class);
        verify(listener).onResponse(argumentCaptor.capture());
        assertNotNull(argumentCaptor.getValue());
        RestResponse restResponse = argumentCaptor.getValue();
        assertEquals(
            "No handler found for /my_index/_search, please check your agent configuration!",
            restResponse.content().utf8ToString()
        );
        assertEquals(RestStatus.BAD_REQUEST, restResponse.status());
        assertEquals("text/plain", restResponse.contentType());
    }
}
