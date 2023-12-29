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

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        prepareModel();
        prepareIndex();
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
    }

    public void testNeuralSparseSearchToolInFlowAgent() {
        String agentId = createAgent(registerAgentRequestBody);
        // successful case
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index\",\"_source\":{\"text\":\"text doc 3\"},\"_id\":\"2\",\"_score\":6.0}\n"
                + "{\"_index\":\"test_index\",\"_source\":{\"text\":\"text doc 2\"},\"_id\":\"1\",\"_score\":3.0}\n",
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
                allOf(containsString("[input] is null or empty, can not process it."), containsString("illegal_argument_exception"))
            );
    }

    public void testNeuralSparseSearchToolInFlowAgent_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index\",\"_source\":{},\"_id\":\"2\",\"_score\":6.0}\n"
                + "{\"_index\":\"test_index\",\"_source\":{},\"_id\":\"1\",\"_score\":3.0}\n",
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
                    containsString("failed to create query: [neural_sparse] query only works on [rank_features] fields"),
                    containsString("search_phase_execution_exception")
                )
            );
    }

    public void testNeuralSparseSearchToolInFlowAgent_withIllegalIndexField_thenThrowException() {
        String agentId = createAgent(registerAgentRequestBody.replace("test_index", "test_index2"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("no such index [test_index2]"), containsString("index_not_found_exception"))
            );
    }

    public void testNeuralSparseSearchToolInFlowAgent_withIllegalModelIdField_thenThrowException() {
        String agentId = createAgent(registerAgentRequestBody.replace(modelId, "test_model_id"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(exception.getMessage(), allOf(containsString("Failed to find model"), containsString("status_exception")));
    }
}
