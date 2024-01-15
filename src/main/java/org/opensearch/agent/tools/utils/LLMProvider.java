/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import org.apache.commons.lang3.StringUtils;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Getter
@AllArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public enum LLMProvider {
    OPENAI("${prompt}"),
    ANTHROPIC("\n\nHuman: ${prompt} \n\nAssistant:"),
    MISTRAL("<s>[INST] ${prompt} [/INST]"),
    NONE("${prompt}");

    String promptFormat;

    public static LLMProvider fromProvider(String provider) {
        if (StringUtils.isBlank(provider)) {
            return NONE;
        }
        for (LLMProvider llmProvider : values()) {
            if (llmProvider.name().equalsIgnoreCase(provider)) {
                return llmProvider;
            }
        }
        return NONE;
    }
}
