/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.rest.DynamicRestRequestCreator;
import org.opensearch.rest.DynamicToolExecutor;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.transport.client.Client;

import com.jayway.jsonpath.JsonPath;

@ToolAnnotation(DynamicTool.TYPE)
public class DynamicTool implements Tool {

    private static final Logger log = LogManager.getLogger(DynamicTool.class);
    public static final String TYPE = "DynamicTool";
    private static final String URI_KEY = "uri";
    private static final String METHOD_KEY = "method";
    private static final String REQUEST_BODY_KEY = "request_body";
    private static final String RESPONSE_FILTER_KEY = "response_filter";
    private static final String DEFAULT_DESCRIPTION =
        "This is a template tool to enable OpenSearch APIs as tool, this tool accepts several parameters: uri, method, request_body and response_filter. uri represents the OpenSearch API uri, method represents the"
            + "OpenSearch API method, request_body represents the OpenSearch API request body and response_filter is a json path expression so that target fields can be extracted from OpenSearch API response. Most OpenSearch APIs"
            + "can be configured with this tool, during agent execution the configured API will be invoked and the response/filtered response will be returned as tool's response.";

    private final Client client;
    private final DynamicToolExecutor toolExecutor;
    private final DynamicRestRequestCreator dynamicRestRequestCreator;
    private final NamedXContentRegistry namedXContentRegistry;
    private String name = TYPE;
    private String description;
    private Map<String, Object> attributes;

    public DynamicTool(
        Client client,
        DynamicToolExecutor toolExecutor,
        DynamicRestRequestCreator dynamicRestRequestCreator,
        NamedXContentRegistry namedXContentRegistry
    ) {
        this.client = client;
        this.toolExecutor = toolExecutor;
        this.dynamicRestRequestCreator = dynamicRestRequestCreator;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String s) {
        this.name = s;
    }

    @Override
    public String getDescription() {
        return Optional.ofNullable(description).orElse(DEFAULT_DESCRIPTION);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public void setAttributes(Map<String, Object> map) {
        this.attributes = new HashMap<>();
        this.attributes.putAll(map);
    }

    @Override
    public void setDescription(String s) {
        this.description = s;
    }

    @Override
    public boolean validate(Map<String, String> map) {
        return true;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        RestRequest.Method method = RestRequest.Method.valueOf(parameters.get(METHOD_KEY));
        String uri = parameters.get(URI_KEY);
        String requestBody = parameters.get(REQUEST_BODY_KEY);
        String responseFileter = parameters.get(RESPONSE_FILTER_KEY);
        StringSubstitutor substitution = new StringSubstitutor(parameters, "${parameters.", "}");
        uri = substitution.replace(uri);
        try {
            BytesReference content = null;
            if (notNullOrEmpty(requestBody)) {
                requestBody = substitution.replace(requestBody);
                XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
                XContentParser parser = MediaType
                    .fromMediaType("application/json")
                    .xContent()
                    .createParser(namedXContentRegistry, DeprecationHandler.IGNORE_DEPRECATIONS, requestBody);
                builder.copyCurrentStructure(parser);
                content = BytesReference.bytes(builder);
            }
            Map<String, List<String>> clientHeaders = client
                .threadPool()
                .getThreadContext()
                .getHeaders()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> List.of(entry.getValue())));
            RestRequest request = dynamicRestRequestCreator.createRestRequest(namedXContentRegistry, method, uri, content, clientHeaders);
            ActionListener<RestResponse> actionListener = ActionListener.wrap(r -> {
                if (notNullOrEmpty(responseFileter)) {
                    // fetch with jsonpath from response.
                    Object result = JsonPath.read(r.content().utf8ToString(), responseFileter);
                    listener.onResponse((T) String.valueOf(result));
                } else {
                    listener.onResponse((T) r.content().utf8ToString());
                }
            }, e -> {
                log.error("Failed to run ToolExecutor", e);
                listener.onFailure(e);
            });
            toolExecutor.execute(request, actionListener);
        } catch (Exception ex) {
            log.error("Failed to run DynamicTool", ex);
            listener.onFailure(ex);
        }
    }

    private boolean notNullOrEmpty(String s) {
        return s != null && !s.isEmpty() && !"null".equals(s);
    }

    public static class Factory implements Tool.Factory<DynamicTool> {
        private Client client;
        private DynamicToolExecutor toolExecutor;
        private DynamicRestRequestCreator dynamicRestRequestCreator;
        private NamedXContentRegistry namedXContentRegistry;

        private static DynamicTool.Factory INSTANCE;

        public static DynamicTool.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (DynamicTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new DynamicTool.Factory();
                return INSTANCE;
            }
        }

        public void init(
            Client client,
            DynamicToolExecutor toolExecutor,
            DynamicRestRequestCreator dynamicRestRequestCreator,
            NamedXContentRegistry namedXContentRegistry
        ) {
            this.client = client;
            this.toolExecutor = toolExecutor;
            this.dynamicRestRequestCreator = dynamicRestRequestCreator;
            this.namedXContentRegistry = namedXContentRegistry;
        }

        @Override
        public DynamicTool create(Map<String, Object> map) {
            if (!map.containsKey(URI_KEY) || StringUtils.isBlank(String.valueOf(map.get(URI_KEY)))) {
                throw new IllegalArgumentException("valid uri is required in DynamicTool configuration!");
            }
            if (!map.containsKey(METHOD_KEY) || map.get(METHOD_KEY) == null) {
                throw new IllegalArgumentException("method is required and not null in DynamicTool configuration!");
            } else {
                try {
                    RestRequest.Method.valueOf(String.valueOf(map.get(METHOD_KEY)));
                } catch (Exception e) {
                    throw new IllegalArgumentException("valid method value is required in DynamicTool configuration!");
                }
            }

            return new DynamicTool(client, toolExecutor, dynamicRestRequestCreator, namedXContentRegistry);
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }

    }
}
