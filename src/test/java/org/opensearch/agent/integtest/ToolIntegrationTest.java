/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.integtest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;

import lombok.extern.log4j.Log4j2;

@Log4j2
public abstract class ToolIntegrationTest extends OpenSearchIntegTestCase {
    protected HttpServer server;
    protected String modelId;
    protected String agentId;
    protected String modelGroupId;
    protected String connectorId;

    private final Gson gson = new Gson();

    abstract List<PromptHandler> promptHandlers();

    abstract String toolType();

    @Before
    public void setupAgent() throws IOException, InterruptedException {
        setupMockLLM();
        clusterSettings(false);
        try {
            connectorId = setUpConnector();
        } catch (Exception e) {
            // Wait for ML encryption master key has been initialized
            TimeUnit.SECONDS.sleep(10);
            connectorId = setUpConnector();
        }
        modelGroupId = setupModelGroup();
        modelId = setupModel(connectorId, modelGroupId);
        agentId = setUpAgent(modelId);
    }

    @After
    public void cleanUpClusterSetting() throws IOException {
        clusterSettings(true);
    }

    @After
    public void stopMockLLM() {
        server.stop(1);
    }

    private void setupMockLLM() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);

        server.createContext("/invoke", exchange -> {
            InputStream ins = exchange.getRequestBody();
            String req = new String(ins.readAllBytes(), StandardCharsets.UTF_8);
            Map<String, String> map = gson.fromJson(req, Map.class);
            String prompt = map.get("prompt");
            log.debug("prompt received: {}", prompt);

            String llmRes = "";
            for (PromptHandler promptHandler : promptHandlers()) {
                if (promptHandler.apply(prompt)) {
                    PromptHandler.LLMResponse llmResponse = new PromptHandler.LLMResponse();
                    if (prompt.contains("TOOL RESPONSE: ")) {
                        llmResponse
                            .setCompletion(
                                "```json{\n"
                                    + "    \"thought\": \"Thought: Now I know the final answer\",\n"
                                    + "    \"final_answer\": \"final answer\"\n"
                                    + "}```"
                            );
                    } else {
                        String actionInput = promptHandler.questionAndInput().getValue();
                        llmResponse
                            .setCompletion(
                                "```json{\n"
                                    + "    \"thought\": \"Thought: Let me use tool to figure out\",\n"
                                    + "    \"action\": \""
                                    + toolType()
                                    + "\",\n"
                                    + "    \"action_input\": \""
                                    + actionInput
                                    + "\"\n"
                                    + "}```"
                            );
                    }
                    llmRes = gson.toJson(llmResponse);
                    break;
                }
            }
            byte[] llmResBytes = llmRes.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, llmResBytes.length);
            exchange.getResponseBody().write(llmResBytes);
            exchange.close();
        });
        server.start();
    }

    private String setUpConnector() throws IOException {
        String url = String.format(Locale.ROOT, "http://127.0.0.1:%d/invoke", server.getAddress().getPort());
        Request request = new Request("POST", "/_plugins/_ml/connectors/_create");
        request
            .setJsonEntity(
                "{\n"
                    + " \"name\": \"BedRock test claude Connector\",\n"
                    + " \"description\": \"The connector to BedRock service for claude model\",\n"
                    + " \"version\": 1,\n"
                    + " \"protocol\": \"aws_sigv4\",\n"
                    + " \"parameters\": {\n"
                    + "  \"region\": \"us-east-1\",\n"
                    + "  \"service_name\": \"bedrock\",\n"
                    + "  \"anthropic_version\": \"bedrock-2023-05-31\",\n"
                    + "  \"endpoint\": \"bedrock.us-east-1.amazonaws.com\",\n"
                    + "  \"auth\": \"Sig_V4\",\n"
                    + "  \"content_type\": \"application/json\",\n"
                    + "  \"max_tokens_to_sample\": 8000,\n"
                    + "  \"temperature\": 0.0001,\n"
                    + "  \"response_filter\": \"$.completion\"\n"
                    + " },\n"
                    + " \"credential\": {\n"
                    + "  \"access_key\": \"<key>\",\n"
                    + "  \"secret_key\": \"<secret>\"\n"
                    + " },\n"
                    + " \"actions\": [\n"
                    + "  {\n"
                    + "   \"action_type\": \"predict\",\n"
                    + "   \"method\": \"POST\",\n"
                    + "   \"url\": \""
                    + url
                    + "\",\n"
                    + "   \"headers\": {\n"
                    + "    \"content-type\": \"application/json\",\n"
                    + "    \"x-amz-content-sha256\": \"required\"\n"
                    + "   },\n"
                    + "   \"request_body\": \"{\\\"prompt\\\":\\\"${parameters.prompt}\\\", \\\"max_tokens_to_sample\\\":${parameters.max_tokens_to_sample}, \\\"temperature\\\":${parameters.temperature},  \\\"anthropic_version\\\":\\\"${parameters.anthropic_version}\\\" }\"\n"
                    + "  }\n"
                    + " ]\n"
                    + "}"
            );
        Response response = executeRequest(request);

        return JsonParser.parseString(readResponse(response)).getAsJsonObject().get("connector_id").getAsString();
    }

    private void clusterSettings(boolean reset) throws IOException {
        Request request = new Request("PUT", "_cluster/settings");
        if (!reset) {
            request
                .setJsonEntity(
                    "{\n"
                        + "    \"persistent\": {\n"
                        + "        \"plugins.ml_commons.only_run_on_ml_node\": false,\n"
                        + "        \"plugins.ml_commons.memory_feature_enabled\": true,\n"
                        + "        \"plugins.ml_commons.trusted_connector_endpoints_regex\": [\n"
                        + "            \"^.*$\"\n"
                        + "        ]\n"
                        + "    }\n"
                        + "}"
                );
        } else {
            request
                .setJsonEntity(
                    "{\n"
                        + "    \"persistent\": {\n"
                        + "        \"plugins.ml_commons.only_run_on_ml_node\": null,\n"
                        + "        \"plugins.ml_commons.memory_feature_enabled\": null,\n"
                        + "        \"plugins.ml_commons.trusted_connector_endpoints_regex\": null"
                        + "    }\n"
                        + "}"
                );
        }
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Content-Type", "application/json");
        request.setOptions(builder);
        getRestClient().performRequest(request);
    }

    private String setupModelGroup() throws IOException {
        Request request = new Request("POST", "/_plugins/_ml/model_groups/_register");
        request
            .setJsonEntity(
                "{\n"
                    + "    \"name\": \"test_model_group_bedrock-"
                    + UUID.randomUUID().toString()
                    + "\",\n"
                    + "    \"description\": \"This is a public model group\"\n"
                    + "}"
            );
        Response response = executeRequest(request);

        String resp = readResponse(response);

        return JsonParser.parseString(resp).getAsJsonObject().get("model_group_id").getAsString();
    }

    private String setupModel(String connectorId, String modelGroupId) throws IOException {
        Request request = new Request("POST", "/_plugins/_ml/models/_register?deploy=true");
        request
            .setJsonEntity(
                "{\n"
                    + "    \"name\": \"Bedrock Claude V2 model\",\n"
                    + "    \"function_name\": \"remote\",\n"
                    + "    \"model_group_id\": \""
                    + modelGroupId
                    + "\",\n"
                    + "    \"description\": \"test model\",\n"
                    + "    \"connector_id\": \""
                    + connectorId
                    + "\"\n"
                    + "}"
            );
        Response response = executeRequest(request);

        String resp = readResponse(response);

        return JsonParser.parseString(resp).getAsJsonObject().get("model_id").getAsString();
    }

    private String setUpAgent(String modelId) throws IOException {
        Request request = new Request("POST", "/_plugins/_ml/agents/_register");
        request
            .setJsonEntity(
                "{\n"
                    + "  \"name\": \"integTest-agent\",\n"
                    + "  \"type\": \"conversational\",\n"
                    + "  \"description\": \"this is a test agent\",\n"
                    + "  \"llm\": {\n"
                    + "    \"model_id\": \""
                    + modelId
                    + "\",\n"
                    + "    \"parameters\": {\n"
                    + "      \"max_iteration\": \"5\",\n"
                    + "      \"stop_when_no_tool_found\": \"true\",\n"
                    + "      \"response_filter\": \"$.completion\"\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"tools\": [\n"
                    + "    {\n"
                    + "      \"type\": \""
                    + toolType()
                    + "\",\n"
                    + "      \"name\": \""
                    + toolType()
                    + "\",\n"
                    + "      \"include_output_in_agent_response\": true,\n"
                    + "      \"description\": \"tool description\"\n"
                    + "    }\n"
                    + "  ],\n"
                    + "  \"memory\": {\n"
                    + "    \"type\": \"conversation_index\"\n"
                    + "  }\n"
                    + "}"
            );
        Response response = executeRequest(request);

        String resp = readResponse(response);

        return JsonParser.parseString(resp).getAsJsonObject().get("agent_id").getAsString();
    }

    public static Response executeRequest(Request request) throws IOException {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Content-Type", "application/json");
        request.setOptions(builder);
        return getRestClient().performRequest(request);
    }

    public static String readResponse(Response response) throws IOException {
        StringBuilder sb = new StringBuilder();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }
}
