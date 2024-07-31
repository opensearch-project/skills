/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.Map;

import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
@ToolAnnotation(RCATool.TYPE)
public class RCATool implements Tool {

    public static final String TYPE = "RCATool";

    private String name = TYPE;

    private String description = DEFAULT_DESCRIPTION;

    public static final String INPUT_FIELD = "text";

    private Client client;

    private static final String DEFAULT_DESCRIPTION = "Use this tool to perform RCA analysis";

    public RCATool(Client client) {
        this.client = client;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        listener.onResponse((T) "hello");
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
    public boolean validate(Map<String, String> parameters) {
        return parameters != null;
    }

    public static class Factory implements Tool.Factory<RCATool> {
        private Client client;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (RCATool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public RCATool create(Map<String, Object> parameters) {
            return new RCATool(client);
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
