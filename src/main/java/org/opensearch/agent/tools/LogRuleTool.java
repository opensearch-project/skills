/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.Map;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.transport.client.Client;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@ToolAnnotation(value = LogRuleTool.TYPE)
@Builder
public class LogRuleTool implements Tool {
    public static final String TYPE = "LogRuleTool";
    public static final String DEFAULT_DESCRIPTION = "LogRuleTool";

    private Client client;

    private NamedXContentRegistry xContentRegistry;

    @Getter
    private String version;

    @Getter
    @Setter
    private String name;

    @Getter
    @Setter
    private String description;

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        String startTime = parameters.get("startTime");
        String endTime = parameters.get("endTime");
        String index = parameters.get("index");
        String queryDSL = parameters.get("queryQSL");

    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return Map.of();
    }

    @Override
    public void setAttributes(Map<String, Object> map) {

    }

    @Override
    public boolean validate(Map<String, String> map) {
        return false;
    }

    public static class Factory implements Tool.Factory<LogRuleTool> {
        private static LogRuleTool.Factory INSTANCE;

        private Client client;
        private NamedXContentRegistry xContentRegistry;

        public static LogRuleTool.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (LogRuleTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new LogRuleTool.Factory();
                return INSTANCE;
            }
        }

        public void init(Client client, NamedXContentRegistry xContentRegistry) {
            this.client = client;
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public LogRuleTool create(Map<String, Object> params) {
            return LogRuleTool.builder().client(client).xContentRegistry(xContentRegistry).build();
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
