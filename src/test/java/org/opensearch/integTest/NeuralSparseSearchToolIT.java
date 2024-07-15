/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.ResponseException;

import lombok.SneakyThrows;

public class NeuralSparseSearchToolIT extends BaseAgentToolsIT {
    public static String TEST_INDEX_NAME = "test_index";
    public static String TEST_NESTED_INDEX_NAME = "test_index_nested";

    private String modelId;
    private String registerAgentRequestBody;

    @SneakyThrows
    private void prepareModel() {
        String requestBody = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource("org/opensearch/agent/tools/register_sparse_encoding_model_request_body.json")
                            .toURI()
                    )
            );
        modelId = registerModelThenDeploy(requestBody);
    }

    @SneakyThrows
    private void prepareIndex() {
        createIndexWithConfiguration(
            TEST_INDEX_NAME,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"text\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"embedding\": {\n"
                + "        \"type\": \"rank_features\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(TEST_INDEX_NAME, "0", List.of("text", "embedding"), List.of("text doc 1", Map.of("hello", 1, "world", 2)));
        addDocToIndex(TEST_INDEX_NAME, "1", List.of("text", "embedding"), List.of("text doc 2", Map.of("a", 3, "b", 4)));
        addDocToIndex(TEST_INDEX_NAME, "2", List.of("text", "embedding"), List.of("text doc 3", Map.of("test", 5, "a", 6)));
    }

    @SneakyThrows
    private void prepareNestedIndex() {
        createIndexWithConfiguration(
            TEST_NESTED_INDEX_NAME,
            "{\n"
                + "  \"mappings\": {\n"
                + "    \"properties\": {\n"
                + "      \"text\": {\n"
                + "        \"type\": \"text\"\n"
                + "      },\n"
                + "      \"embedding\": {\n"
                + "        \"type\": \"nested\",\n"
                + "        \"properties\":{\n"
                + "            \"sparse\":{\n"
                + "                \"type\":\"rank_features\"\n"
                + "            }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        addDocToIndex(
            TEST_NESTED_INDEX_NAME,
            "0",
            List.of("text", "embedding"),
            List.of("text doc 1", Map.of("sparse", List.of(Map.of("hello", 1, "world", 2))))
        );
        addDocToIndex(
            TEST_NESTED_INDEX_NAME,
            "1",
            List.of("text", "embedding"),
            List.of("text doc 2", Map.of("sparse", List.of(Map.of("a", 3, "b", 4))))
        );
        addDocToIndex(
            TEST_NESTED_INDEX_NAME,
            "2",
            List.of("text", "embedding"),
            List.of("text doc 3", Map.of("sparse", List.of(Map.of("test", 5, "a", 6))))
        );
    }

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        prepareModel();
        prepareIndex();
        prepareNestedIndex();
        registerAgentRequestBody = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_neural_sparse_search_tool_request_body.json")
                            .toURI()
                    )
            );
        registerAgentRequestBody = registerAgentRequestBody.replace("<MODEL_ID>", modelId);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
        deleteModel(modelId);
    }

    public void testNeuralSparseSearchToolInFlowAgent() {
        String agentId = createAgent(registerAgentRequestBody);
        // successful case
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index\",\"_source\":{\"text\":\"text doc 3\"},\"_id\":\"2\",\"_score\":2.4136734}\n"
                + "{\"_index\":\"test_index\",\"_source\":{\"text\":\"text doc 2\"},\"_id\":\"1\",\"_score\":1.2068367}\n",
            result
        );

        // use non-exist token to test the case the tool can not find match docs.
        String result2 = executeAgent(agentId, "{\"parameters\": {\"question\": \"c\"}}");
        assertEquals("The agent execute response not equal with expected.", "Can not get any match from search result.", result2);

        // if blank input, call onFailure and get exception
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("[input] is null or empty, can not process it."), containsString("IllegalArgumentException"))
            );

        // use json string input
        String jsonInput = gson.toJson(Map.of("parameters", Map.of("question", gson.toJson(Map.of("hi", "a")))));
        String result3 = executeAgent(agentId, jsonInput);
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index\",\"_source\":{\"text\":\"text doc 3\"},\"_id\":\"2\",\"_score\":2.4136734}\n"
                + "{\"_index\":\"test_index\",\"_source\":{\"text\":\"text doc 2\"},\"_id\":\"1\",\"_score\":1.2068367}\n",
            result3
        );
    }

    public void testNeuralSparseSearchToolInFlowAgent_withNestedIndex() {
        String registerAgentRequestBodyNested = registerAgentRequestBody;
        registerAgentRequestBodyNested = registerAgentRequestBodyNested.replace("\"nested_path\": \"\"", "\"nested_path\": \"embedding\"");
        registerAgentRequestBodyNested = registerAgentRequestBodyNested
            .replace("\"embedding_field\": \"embedding\"", "\"embedding_field\": \"embedding.sparse\"");
        registerAgentRequestBodyNested = registerAgentRequestBodyNested
            .replace("\"index\": \"test_index\"", "\"index\": \"test_index_nested\"");
        String agentId = createAgent(registerAgentRequestBodyNested);
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index_nested\",\"_source\":{\"text\":\"text doc 3\"},\"_id\":\"2\",\"_score\":2.4136734}\n"
                + "{\"_index\":\"test_index_nested\",\"_source\":{\"text\":\"text doc 2\"},\"_id\":\"1\",\"_score\":1.2068367}\n",
            result
        );
    }

    public void testNeuralSparseSearchToolInFlowAgent_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index\",\"_source\":{},\"_id\":\"2\",\"_score\":2.4136734}\n"
                + "{\"_index\":\"test_index\",\"_source\":{},\"_id\":\"1\",\"_score\":1.2068367}\n",
            result
        );
    }

    public void testNeuralSparseSearchToolInFlowAgent_withIllegalEmbeddingField_thenThrowException() {
        String agentId = createAgent(registerAgentRequestBody.replace("\"embedding\"", "\"embedding2\""));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString("[neural_sparse] query only works on [rank_features] fields"),
                    containsString("IllegalArgumentException")
                )
            );
    }

    public void testNeuralSparseSearchToolInFlowAgent_withIllegalIndexField_thenThrowException() {
        String agentId = createAgent(registerAgentRequestBody.replace("test_index", "test_index2"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("no such index [test_index2]"), containsString("IndexNotFoundException"))
            );
    }

    public void testNeuralSparseSearchToolInFlowAgent_withIllegalModelIdField_thenThrowException() {
        String agentId = createAgent(registerAgentRequestBody.replace(modelId, "test_model_id"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(exception.getMessage(), allOf(containsString("Failed to find model"), containsString("OpenSearchStatusException")));
    }
}
