/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.rest;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;

public class DynamicToolExecutor {
    private final AtomicReference<RestController> restControllerRef;
    private final Client client;

    public DynamicToolExecutor(AtomicReference<RestController> restControllerRef, Client nodeClient) {
        this.restControllerRef = restControllerRef;
        this.client = nodeClient;
    }

    public void execute(RestRequest request, ActionListener<RestResponse> listener) throws Exception {
        String rawPath = request.rawPath();
        String uri = request.uri();
        RestRequest.Method requestMethod = request.method();

        Optional<RestHandler> restHandler = restControllerRef.get().dispatchHandler(uri, rawPath, requestMethod, request.params());
        RestChannel dummyChannel = new AbstractRestChannel(request, true) {
            @Override
            public void sendResponse(RestResponse response) {
                // This supposes to be the API's response, and will be encapsulated in the agent response, so either the API succeed or not,
                // we use onResponse.
                listener.onResponse(response);
            }
        };
        if (restHandler.isEmpty()) {
            listener.onResponse(new RestResponse() {
                @Override
                public String contentType() {
                    return "text/plain";
                }

                @Override
                public BytesReference content() {
                    String errorMessage = String
                        .format(Locale.ROOT, "No handler found for %s, please check your agent configuration!", uri);
                    return BytesReference.fromByteBuffer(ByteBuffer.wrap(errorMessage.getBytes(StandardCharsets.UTF_8)));
                }

                @Override
                public RestStatus status() {
                    return RestStatus.BAD_REQUEST;
                }
            });
        } else {
            restHandler.get().handleRequest(request, dummyChannel, (NodeClient) client);
        }
    }
}
