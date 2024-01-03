/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.junit.Assert;
import org.opensearch.agent.tools.VisualizationsTool;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

import com.google.gson.JsonParser;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class VisualizationsToolIT extends ToolIntegrationTest {
    @Override
    List<PromptHandler> promptHandlers() {
        return List.of(new PromptHandler() {
            @Override
            LLMThought llmThought() {
                return LLMThought
                    .builder()
                    .action(VisualizationsTool.TYPE)
                    .actionInput("RAM")
                    .question("can you show me RAM info with visualization?")
                    .build();
            }
        }, new PromptHandler() {
            @Override
            LLMThought llmThought() {
                return LLMThought
                    .builder()
                    .action(VisualizationsTool.TYPE)
                    .actionInput("sales")
                    .question("how about the sales about this month?")
                    .build();
            }
        });
    }

    String toolType() {
        return VisualizationsTool.TYPE;
    }

    public void testVisualizationNotFound() throws IOException {
        Request request = new Request("POST", "/_plugins/_ml/agents/" + agentId + "/_execute");
        request.setJsonEntity("{\"parameters\":{\"question\":\"can you show me RAM info with visualization?\"}}");
        Response response = executeRequest(request);
        String responseStr = readResponse(response);
        String toolOutput = extractAdditionalInfo(responseStr);
        Assert.assertEquals("No Visualization found", toolOutput);
    }

    public void testVisualizationFound() throws IOException {
        String title = "[eCommerce] Sales by Category";
        String id = UUID.randomUUID().toString();
        prepareVisualization(title, id);
        Request request = new Request("POST", "/_plugins/_ml/agents/" + agentId + "/_execute");
        request.setJsonEntity("{\"parameters\":{\"question\":\"how about the sales about this month?\"}}");
        Response response = executeRequest(request);
        String responseStr = readResponse(response);
        String toolOutput = extractAdditionalInfo(responseStr);
        Assert.assertEquals("Title,Id\n" + String.format(Locale.ROOT, "%s,%s\n", title, id), toolOutput);
    }

    private void prepareVisualization(String title, String id) {
        String body = "{\n"
            + "    \"visualization\": {\n"
            + "        \"title\": \""
            + title
            + "\"\n"
            + "    },\n"
            + "    \"type\": \"visualization\"\n"
            + "}";
        Response response = makeRequest(client(), "POST", String.format(Locale.ROOT, ".kibana/_doc/%s?refresh=true", id), null, body, null);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), RestStatus.CREATED.getStatus());
    }

    private String extractAdditionalInfo(String responseStr) {
        return JsonParser
            .parseString(responseStr)
            .getAsJsonObject()
            .get("inference_results")
            .getAsJsonArray()
            .get(0)
            .getAsJsonObject()
            .get("output")
            .getAsJsonArray()
            .get(0)
            .getAsJsonObject()
            .get("dataAsMap")
            .getAsJsonObject()
            .get("additional_info")
            .getAsJsonObject()
            .get(String.format(Locale.ROOT, "%s.output", toolType()))
            .getAsString();
    }
}
