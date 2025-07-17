/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.rest;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;

import com.google.common.net.HttpHeaders;

public class DynamicRestRequestCreator {
    public RestRequest createRestRequest(
        NamedXContentRegistry namedXContentRegistry,
        RestRequest.Method method,
        String uri,
        BytesReference content,
        Map<String, List<String>> headers
    ) {
        HttpRequest httpRequest = new HttpRequest() {
            @Override
            public RestRequest.Method method() {
                return method;
            }

            @Override
            public String uri() {
                return uri;
            }

            @Override
            public BytesReference content() {
                return content;
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                // The transport action needs correct headers to work, e.g. credentials so passing the original headers to the created
                // request.
                Map<String, List<String>> internalRequestHeaders = new HashMap<>(headers);
                internalRequestHeaders.put(HttpHeaders.CONTENT_TYPE, List.of("application/json"));
                return internalRequestHeaders;
            }

            @Override
            public List<String> strictCookies() {
                return List.of();
            }

            @Override
            public HttpVersion protocolVersion() {
                // This doesn't have actual impact only to ensure no NPE in corner cases.
                return HttpRequest.HttpVersion.HTTP_1_0;
            }

            @Override
            public HttpRequest removeHeader(String s) {
                return this;
            }

            @Override
            public HttpResponse createResponse(RestStatus restStatus, BytesReference bytesReference) {
                // An example of overriding this method is:
                // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/http/DefaultRestChannel.java#L145-L174
                // After the response been created, it's been sent to the RestChannel, but in our case the rest channel is mock and
                // sendResponse method will be used to send response to a listener,
                // So this method never been invoked, so returning null here.
                return null;
            }

            @Override
            public Exception getInboundException() {
                return null;
            }

            @Override
            public void release() {
                // Nothing needs to be released, for other implementation like:
                // https://github.com/opensearch-project/OpenSearch/blob/main/modules/transport-netty4/src/main/java/org/opensearch/http/netty4/Netty4HttpRequest.java#L64
                // It needs to release the internal FullHttpRequest resources.
            }

            @Override
            public HttpRequest releaseAndCopy() {
                // Some handlers can't handle pooled buffer correctly then it'll override the allowUnsafeBuffers method, e.g.:
                // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/rest/action/document/RestBulkAction.java#L128
                // This HttpRequest is not created from the pooled buffer, so it's safe to return itself.
                return this;
            }
        };
        HttpChannel httpChannel = new HttpChannel() {
            @Override
            public void sendResponse(HttpResponse httpResponse, ActionListener<Void> actionListener) {
                // This is used in:
                // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/http/DefaultRestChannel.java#L145-L174,
                // But since this method is mainly invoked by
                // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/rest/action/RestResponseListener.java#L52
                // and since the RestChannel.sendResponse has been rewritten to invoke actionListener, so this method won't be invoked in
                // current case.
            }

            @Override
            public InetSocketAddress getLocalAddress() {
                // Just for logging in Netty4HttpChannel, safe to return null.
                return null;
            }

            @Override
            public InetSocketAddress getRemoteAddress() {
                // Just for logging in Netty4HttpChannel, safe to return null.
                return null;
            }

            @Override
            public void close() {
                // Close resources that needs to be closed which hold by the channel, in this case nothing to close.
            }

            @Override
            public void addCloseListener(ActionListener<Void> actionListener) {
                // No resources need to add listener
            }

            @Override
            public boolean isOpen() {
                return true;
            }
        };
        return RestRequest.request(namedXContentRegistry, httpRequest, httpChannel);
    }

}
