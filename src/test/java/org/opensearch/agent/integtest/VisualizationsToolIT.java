/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.integtest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.opensearch.agent.tools.VisualizationsTool;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
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
            Pair<String, String> questionAndInput() {
                return Pair.of("can you show me RAM info with visualization?", "RAM");
            }
        }, new PromptHandler() {
            @Override
            Pair<String, String> questionAndInput() {
                return Pair.of("how about the sales about this month?", "sales");
            }
        });
    }

    String toolType() {
        return VisualizationsTool.TYPE;
    }

    public void testVisualizationNotFound() throws IOException {
        Request request = new Request("POST", "/_plugins/_ml/agents/" + agentId + "/_execute");
        request.setJsonEntity("{\"parameters\":{\"question\":\"can you show me RAM info with visualization?\"}}");
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Content-Type", "application/json");
        request.setOptions(builder);
        Response response = getRestClient().performRequest(request);
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
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Content-Type", "application/json");
        request.setOptions(builder);
        Response response = getRestClient().performRequest(request);
        String responseStr = readResponse(response);
        String toolOutput = extractAdditionalInfo(responseStr);
        Assert.assertEquals("Title,Id\n" + String.format(Locale.ROOT, "%s,%s\n", title, id), toolOutput);
    }

    private void prepareVisualization(String title, String id) throws IOException {
        Request request = new Request("POST", ".kibana/_doc/" + id);
        request
            .setJsonEntity(
                "{\n"
                    + "    \"visualization\": {\n"
                    + "        \"title\": \""
                    + title
                    + "\"\n"
                    + "    },\n"
                    + "    \"type\": \"visualization\"\n"
                    + "}"
            );
        Response response = executeRequest(request);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), RestStatus.OK.getStatus());
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
