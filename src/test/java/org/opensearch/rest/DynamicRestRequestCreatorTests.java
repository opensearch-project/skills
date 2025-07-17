/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.rest;

import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;

import com.google.common.collect.ImmutableMap;

public class DynamicRestRequestCreatorTests {

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void test_createRestRequest() {
        DynamicRestRequestCreator dynamicRestRequestCreator = new DynamicRestRequestCreator();
        RestRequest restRequest = dynamicRestRequestCreator
            .createRestRequest(
                null,
                RestRequest.Method.GET,
                "/_search",
                null,
                ImmutableMap.of("Content-Type", List.of("application/json"))
            );
        assert restRequest != null;
        assert restRequest.path().equals("/_search");
        assert restRequest.method().equals(RestRequest.Method.GET);
        assert restRequest.content() == null;
        assert restRequest.getHeaders().size() == 1;

        restRequest.getHttpRequest().release();
        restRequest.getHttpChannel().close();
        restRequest.getHttpChannel().addCloseListener(mock(ActionListener.class));
        restRequest.getHttpChannel().sendResponse(mock(HttpResponse.class), mock(ActionListener.class));

        assert restRequest.getHttpRequest().removeHeader("Content-Type") == restRequest.getHttpRequest();
        assert restRequest.getHttpRequest().strictCookies().isEmpty();
        assert restRequest.getHttpRequest().protocolVersion().equals(HttpRequest.HttpVersion.HTTP_1_0);
        assert restRequest
            .getHttpRequest()
            .createResponse(
                RestStatus.BAD_REQUEST,
                BytesReference.fromByteBuffer(ByteBuffer.wrap("mock response body".getBytes(StandardCharsets.UTF_8)))
            ) == null;
        assert restRequest.getHttpRequest().getInboundException() == null;
        assert restRequest.getHttpRequest().releaseAndCopy() != null;

        assert restRequest.getHttpChannel().isOpen();
        assert restRequest.getHttpChannel().getLocalAddress() == null;
        assert restRequest.getHttpChannel().getRemoteAddress() == null;
    }
}
