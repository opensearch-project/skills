/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import com.google.gson.annotations.SerializedName;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class PromptHandler {

    boolean apply(String prompt) {
        return prompt.contains(toolInput().getQuestion());
    }

    ToolInput toolInput() {
        return new ToolInput();
    }

    String response(String prompt) {
        if (prompt.contains("TOOL RESPONSE: ")) {
            return "```json{\n"
                + "    \"thought\": \"Thought: Now I know the final answer\",\n"
                + "    \"final_answer\": \"final answer\"\n"
                + "}```";
        } else {
            return "```json{\n"
                + "    \"thought\": \"Thought: Let me use tool to figure out\",\n"
                + "    \"action\": \""
                + this.toolInput().getToolType()
                + "\",\n"
                + "    \"action_input\": \""
                + this.toolInput().getToolInput()
                + "\"\n"
                + "}```";
        }
    }

    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    static class ToolInput {
        String question;
        String toolType;
        String toolInput;
    }

    @Data
    static class LLMResponse {
        String completion;
        @SerializedName("stop_reason")
        String stopReason = "stop_sequence";
        String stop = "\\n\\nHuman:";
    }
}
