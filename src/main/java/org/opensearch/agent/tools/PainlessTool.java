/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.script.TemplateScript;

import com.google.gson.Gson;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * use case for this tool will only focus on flow agent
 */
@Log4j2
@Setter
@Getter
@ToolAnnotation(PainlessTool.TYPE)
public class PainlessTool implements Tool {
    public static final String TYPE = "PainlessTool";
    private static final String DEFAULT_DESCRIPTION = "Use this tool to execute painless script";

    @Setter
    @Getter
    private String name = TYPE;

    @Getter
    private String type = TYPE;

    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;

    @Getter
    private String version;

    private ScriptService scriptService;
    @Setter
    private String scriptCode;

    public PainlessTool(ScriptService scriptEngine, String script) {
        this.scriptService = scriptEngine;
        this.scriptCode = script;
    }

    private Gson gson = new Gson();

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        Script script = new Script(ScriptType.INLINE, "painless", scriptCode, Collections.emptyMap());
        Map<String, Object> flattenedParameters = new HashMap<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            // keep original values and flatten
            flattenedParameters.put(entry.getKey(), entry.getValue());
            // TODO default is json parser. we may support format
            try {
                String value = org.apache.commons.text.StringEscapeUtils.unescapeJson(entry.getValue());
                Map<String, ?> map = gson.fromJson(value, Map.class);
                flattenMap(map, flattenedParameters, entry.getKey());
            } catch (Throwable ignored) {}
        }
        TemplateScript templateScript = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(flattenedParameters);
        try {
            String result = templateScript.execute();
            listener.onResponse(result == null ? (T) "" : (T) result);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void flattenMap(Map<String, ?> map, Map<String, Object> flatMap, String prefix) {
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            String key = entry.getKey();
            if (prefix != null && !prefix.isEmpty()) {
                key = prefix + "." + entry.getKey();
            }
            Object value = entry.getValue();
            if (value instanceof Map) {
                flattenMap((Map<String, ?>) value, flatMap, key);
            } else {
                flatMap.put(key, value);
            }
        }
    }

    @Override
    public boolean validate(Map<String, String> map) {
        return true;
    }

    public static class Factory implements Tool.Factory<PainlessTool> {
        private ScriptService scriptService;

        private static PainlessTool.Factory INSTANCE;

        public static PainlessTool.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (PainlessTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new PainlessTool.Factory();
                return INSTANCE;
            }
        }

        public void init(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public PainlessTool create(Map<String, Object> map) {
            String script = (String) map.get("script");
            // TODO add script non null/empty check
            return new PainlessTool(scriptService, script);
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
