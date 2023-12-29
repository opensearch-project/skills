/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.integtest;

import org.apache.commons.lang3.tuple.Pair;

import com.google.gson.annotations.SerializedName;

import lombok.Data;

public abstract class PromptHandler {

    boolean apply(String prompt) {
        return prompt.contains(questionAndInput().getKey());
    }

    abstract Pair<String, String> questionAndInput();

    @Data
    static class LLMResponse {
        String completion;
        @SerializedName("stop_reason")
        String stopReason = "stop_sequence";
        String stop = "\\n\\nHuman:";
    }
}
