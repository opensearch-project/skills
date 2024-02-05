/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;

import lombok.extern.log4j.Log4j2;

@Log4j2
public abstract class ToolIntegrationTest extends BaseAgentToolsIT {
    protected HttpServer server;
    protected String modelId;
    protected String agentId;
    protected String modelGroupId;
    protected String connectorId;

    private final Gson gson = new Gson();

    abstract List<PromptHandler> promptHandlers();

    abstract String toolType();

    @Before
    public void setupTestAgent() throws IOException, InterruptedException {
        server = MockHttpServer.setupMockLLM(promptHandlers());
        server.start();
        clusterSettings(false);
        try {
            connectorId = setUpConnector();
        } catch (Exception e) {
            // Wait for ML encryption master key has been initialized
            TimeUnit.SECONDS.sleep(10);
            connectorId = setUpConnector();
        }
        modelGroupId = setupModelGroup();
        modelId = setupLLMModel(connectorId, modelGroupId);
        // wait for model to get deployed
        TimeUnit.SECONDS.sleep(1);
        agentId = setupConversationalAgent(modelId);
        log.info("model_id: {}, agent_id: {}", modelId, agentId);
    }

    @After
    public void cleanUpClusterSetting() throws IOException {
        clusterSettings(true);
    }

    @After
    public void stopMockLLM() {
        server.stop(1);
    }

    @After
    public void deleteModel() {
        deleteModel(modelId);
    }

    private String setUpConnectorWithRetry(int maxRetryTimes) throws InterruptedException {
        int retryTimes = 0;
        String connectorId = null;
        while (retryTimes < maxRetryTimes) {
            try {
                connectorId = setUpConnector();
                break;
            } catch (Exception e) {
                // Wait for ML encryption master key has been initialized
                log.info("Failed to setup connector, retry times: {}", retryTimes);
                retryTimes++;
                TimeUnit.SECONDS.sleep(20);
            }
        }
        return connectorId;
    }

    private String setUpConnector() {
        String url = String.format(Locale.ROOT, "http://127.0.0.1:%d/invoke", server.getAddress().getPort());
        return createConnector(
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
    }

    private void clusterSettings(boolean clean) throws IOException {
        if (!clean) {
            updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
            updateClusterSettings("plugins.ml_commons.memory_feature_enabled", true);
            updateClusterSettings("plugins.ml_commons.trusted_connector_endpoints_regex", List.of("^.*$"));
        } else {
            updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", null);
            updateClusterSettings("plugins.ml_commons.memory_feature_enabled", null);
            updateClusterSettings("plugins.ml_commons.trusted_connector_endpoints_regex", null);
        }
    }

    private String setupModelGroup() throws IOException {
        Request request = new Request("POST", "/_plugins/_ml/model_groups/_register");
        request
            .setJsonEntity(
                "{\n"
                    + "    \"name\": \"test_model_group_bedrock-"
                    + UUID.randomUUID()
                    + "\",\n"
                    + "    \"description\": \"This is a public model group\"\n"
                    + "}"
            );
        Response response = executeRequest(request);

        String resp = readResponse(response);

        return JsonParser.parseString(resp).getAsJsonObject().get("model_group_id").getAsString();
    }

    private String setupLLMModel(String connectorId, String modelGroupId) throws IOException {
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

    private String setupConversationalAgent(String modelId) throws IOException {
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
        return client().performRequest(request);
    }

    public static String readResponse(Response response) throws IOException {
        try (InputStream ins = response.getEntity().getContent()) {
            return String.join("", org.opensearch.common.io.Streams.readAllLines(ins));
        }
    }
}
