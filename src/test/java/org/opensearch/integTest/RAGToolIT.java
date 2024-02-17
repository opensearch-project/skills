/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.opensearch.agent.tools.RAGTool;
import org.opensearch.client.ResponseException;

import lombok.SneakyThrows;

public class RAGToolIT extends ToolIntegrationTest {

    public static String TEST_NEURAL_INDEX_NAME = "test_neural_index";
    public static String TEST_NEURAL_SPARSE_INDEX_NAME = "test_neural_sparse_index";
    private String textEmbeddingModelId;
    private String sparseEncodingModelId;
    private String largeLanguageModelId;
    private String registerAgentWithNeuralQueryRequestBody;
    private String registerAgentWithNeuralSparseQueryRequestBody;
    private String registerAgentWithNeuralQueryAndLLMRequestBody;
    private String mockLLMResponseWithSource = "{\n"
        + "  \"inference_results\": [\n"
        + "    {\n"
        + "      \"output\": [\n"
        + "        {\n"
        + "          \"name\": \"response\",\n"
        + "          \"result\": \"\"\" Based on the context given:\n"
        + "                     a, b, c are alphabets.\"\"\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    private String mockLLMResponseWithoutSource = "{\n"
        + "  \"inference_results\": [\n"
        + "    {\n"
        + "      \"output\": [\n"
        + "        {\n"
        + "          \"name\": \"response\",\n"
        + "          \"result\": \"\"\" Based on the context given:\n"
        + "                      I do not see any information about a, b, c\". So I would have to say I don't know the answer to your question based on this context..\"\"\"\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    private String registerAgentWithNeuralSparseQueryAndLLMRequestBody;

    public RAGToolIT() throws IOException, URISyntaxException {}

    @SneakyThrows
    private void prepareModel() {
        String requestBody = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource("org/opensearch/agent/tools/register_text_embedding_model_request_body.json")
                            .toURI()
                    )
            );
        textEmbeddingModelId = registerModelThenDeploy(requestBody);

        String requestBody1 = Files
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
        sparseEncodingModelId = registerModelThenDeploy(requestBody1);
        largeLanguageModelId = this.modelId;
    }

    @SneakyThrows
    private void prepareIndex() {
        // prepare index for neural sparse query type
        createIndexWithConfiguration(
            TEST_NEURAL_SPARSE_INDEX_NAME,
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
        addDocToIndex(
            TEST_NEURAL_SPARSE_INDEX_NAME,
            "0",
            List.of("text", "embedding"),
            List.of("hello world", Map.of("hello", 1, "world", 2))
        );
        addDocToIndex(TEST_NEURAL_SPARSE_INDEX_NAME, "1", List.of("text", "embedding"), List.of("a b", Map.of("a", 3, "b", 4)));

        // prepare index for neural query type
        String pipelineConfig = "{\n"
            + "  \"description\": \"text embedding pipeline\",\n"
            + "  \"processors\": [\n"
            + "    {\n"
            + "      \"text_embedding\": {\n"
            + "        \"model_id\": \""
            + textEmbeddingModelId
            + "\",\n"
            + "        \"field_map\": {\n"
            + "          \"text\": \"embedding\"\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        createIngestPipelineWithConfiguration("test-embedding-model", pipelineConfig);

        String indexMapping = "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"text\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"embedding\": {\n"
            + "        \"type\": \"knn_vector\",\n"
            + "        \"dimension\": 768,\n"
            + "        \"method\": {\n"
            + "          \"name\": \"hnsw\",\n"
            + "          \"space_type\": \"l2\",\n"
            + "          \"engine\": \"lucene\",\n"
            + "          \"parameters\": {\n"
            + "            \"ef_construction\": 128,\n"
            + "            \"m\": 24\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"settings\": {\n"
            + "    \"index\": {\n"
            + "      \"knn.space_type\": \"cosinesimil\",\n"
            + "      \"default_pipeline\": \"test-embedding-model\",\n"
            + "      \"knn\": \"true\"\n"
            + "    }\n"
            + "  }\n"
            + "}";

        createIndexWithConfiguration(TEST_NEURAL_INDEX_NAME, indexMapping);

        addDocToIndex(TEST_NEURAL_INDEX_NAME, "0", List.of("text"), List.of("hello world"));

        addDocToIndex(TEST_NEURAL_INDEX_NAME, "1", List.of("text"), List.of("a b"));
    }

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        prepareModel();
        prepareIndex();
        String registerAgentWithNeuralQueryRequestBodyFile = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource(
                                "org/opensearch/agent/tools/register_flow_agent_of_ragtool_with_neural_query_type_request_body.json"
                            )
                            .toURI()
                    )
            );
        registerAgentWithNeuralQueryRequestBody = registerAgentWithNeuralQueryRequestBodyFile
            .replace("<MODEL_ID>", textEmbeddingModelId)
            .replace("<INDEX_NAME>", TEST_NEURAL_INDEX_NAME);

        registerAgentWithNeuralSparseQueryRequestBody = registerAgentWithNeuralQueryRequestBodyFile
            .replace("<MODEL_ID>", sparseEncodingModelId)
            .replace("<INDEX_NAME>", TEST_NEURAL_SPARSE_INDEX_NAME)
            .replace("\"query_type\": \"neural\"", "\"query_type\": \"neural_sparse\"");

        registerAgentWithNeuralQueryAndLLMRequestBody = registerAgentWithNeuralQueryRequestBodyFile
            .replace("<MODEL_ID>", textEmbeddingModelId + "\" ,\n \"inference_model_id\": \"" + largeLanguageModelId)
            .replace("<INDEX_NAME>", TEST_NEURAL_INDEX_NAME)
            .replace("false", "true");
        registerAgentWithNeuralSparseQueryAndLLMRequestBody = registerAgentWithNeuralQueryRequestBodyFile
            .replace("<MODEL_ID>", sparseEncodingModelId + "\" ,\n \"inference_model_id\": \"" + largeLanguageModelId)
            .replace("<INDEX_NAME>", TEST_NEURAL_SPARSE_INDEX_NAME)
            .replace("\"query_type\": \"neural\"", "\"query_type\": \"neural_sparse\"")
            .replace("false", "true");
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
        deleteModel(textEmbeddingModelId);
        deleteModel(sparseEncodingModelId);
    }

    public void testRAGToolWithNeuralQuery() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody);

        // neural query to test match similar text, doc1 match with higher score
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"c\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_index\",\"_source\":{\"text\":\"hello world\"},\"_id\":\"0\",\"_score\":0.7046764}\n"
                + "{\"_index\":\"test_neural_index\",\"_source\":{\"text\":\"a b\"},\"_id\":\"1\",\"_score\":0.2649903}\n",
            result
        );

        // neural query to test match exact same text case, doc0 match with higher score
        String result1 = executeAgent(agentId, "{\"parameters\": {\"question\": \"hello\"}}");

        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_index\",\"_source\":{\"text\":\"hello world\"},\"_id\":\"0\",\"_score\":0.56714886}\n"
                + "{\"_index\":\"test_neural_index\",\"_source\":{\"text\":\"a b\"},\"_id\":\"1\",\"_score\":0.24236833}\n",
            result1
        );

        // if blank input, call onFailure and get exception
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("[input] is null or empty, can not process it."), containsString("IllegalArgumentException"))
            );

    }

    public void testRAGToolWithNeuralQueryAndLLM() {
        String agentId = createAgent(registerAgentWithNeuralQueryAndLLMRequestBody);

        // neural query to test match similar text, doc1 match with higher score
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"use RAGTool to answer c\"}}");
        assertEquals(mockLLMResponseWithSource, result);

        // if blank input, call onFailure and get exception
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("[input] is null or empty, can not process it."), containsString("IllegalArgumentException"))
            );

    }

    public void testRAGToolWithNeuralSparseQuery() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody);

        // neural sparse query to test match extract same text, doc1 match with high score
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_sparse_index\",\"_source\":{\"text\":\"a b\"},\"_id\":\"1\",\"_score\":1.2068367}\n",
            result
        );

        // neural sparse query to test match extract non-existed text, no match
        String result2 = executeAgent(agentId, "{\"parameters\": {\"question\": \"c\"}}");
        assertEquals("The agent execute response not equal with expected.", "Can not get any match from search result.", result2);

        // if blank input, call onFailure and get exception
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("[input] is null or empty, can not process it."), containsString("IllegalArgumentException"))
            );
    }

    public void testRAGToolWithNeuralSparseQueryAndLLM() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryAndLLMRequestBody);

        // neural sparse query to test match extract same text, doc1 match with high score
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"use RAGTool to answer a\"}}");
        assertEquals(mockLLMResponseWithSource, result);

        // if blank input, call onFailure and get exception
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("[input] is null or empty, can not process it."), containsString("IllegalArgumentException"))
            );
    }

    public void testRAGToolWithNeuralSparseQuery_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_sparse_index\",\"_source\":{},\"_id\":\"1\",\"_score\":1.2068367}\n",
            result
        );
    }

    public void testRAGToolWithNeuralSparseQueryAndLLM_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryAndLLMRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(mockLLMResponseWithoutSource, result);
    }

    public void testRAGToolWithNeuralQuery_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_index\",\"_source\":{},\"_id\":\"0\",\"_score\":0.70493275}\n"
                + "{\"_index\":\"test_neural_index\",\"_source\":{},\"_id\":\"1\",\"_score\":0.2650575}\n",
            result
        );
    }

    public void testRAGToolWithNeuralQueryAndLLM_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentWithNeuralQueryAndLLMRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(mockLLMResponseWithoutSource, result);
    }

    public void testRAGToolWithNeuralSparseQuery_withIllegalEmbeddingField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody.replace("\"embedding\"", "\"embedding2\""));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("all shards failed"), containsString("SearchPhaseExecutionException"))
            );
    }

    public void testRAGToolWithNeuralSparseQueryAndLLM_withIllegalEmbeddingField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryAndLLMRequestBody.replace("\"embedding\"", "\"embedding2\""));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("all shards failed"), containsString("SearchPhaseExecutionException"))
            );
    }

    public void testRAGToolWithNeuralQuery_withIllegalEmbeddingField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody.replace("\"embedding\"", "\"embedding2\""));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("all shards failed"), containsString("SearchPhaseExecutionException"))
            );
    }

    public void testRAGToolWithNeuralQueryAndLLM_withIllegalEmbeddingField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryAndLLMRequestBody.replace("\"embedding\"", "\"embedding2\""));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("all shards failed"), containsString("SearchPhaseExecutionException"))
            );
    }

    public void testRAGToolWithNeuralSparseQuery_withIllegalIndexField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody.replace(TEST_NEURAL_SPARSE_INDEX_NAME, "test_index2"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("no such index [test_index2]"), containsString("IndexNotFoundException"))
            );
    }

    public void testRAGToolWithNeuralSparseQueryAndLLM_withIllegalIndexField_thenThrowException() {
        String agentId = createAgent(
            registerAgentWithNeuralSparseQueryAndLLMRequestBody.replace(TEST_NEURAL_SPARSE_INDEX_NAME, "test_index2")
        );
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("no such index [test_index2]"), containsString("IndexNotFoundException"))
            );
    }

    public void testRAGToolWithNeuralQuery_withIllegalIndexField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody.replace(TEST_NEURAL_INDEX_NAME, "test_index2"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("no such index [test_index2]"), containsString("IndexNotFoundException"))
            );
    }

    public void testRAGToolWithNeuralQueryAndLLM_withIllegalIndexField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryAndLLMRequestBody.replace(TEST_NEURAL_INDEX_NAME, "test_index2"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("no such index [test_index2]"), containsString("IndexNotFoundException"))
            );
    }

    public void testRAGToolWithNeuralSparseQuery_withIllegalModelIdField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody.replace(sparseEncodingModelId, "test_model_id"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(exception.getMessage(), allOf(containsString("Failed to find model"), containsString("OpenSearchStatusException")));
    }

    public void testRAGToolWithNeuralSparseQueryAndLLM_withIllegalModelIdField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryAndLLMRequestBody.replace(sparseEncodingModelId, "test_model_id"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(exception.getMessage(), allOf(containsString("Failed to find model"), containsString("OpenSearchStatusException")));
    }

    public void testRAGToolWithNeuralQuery_withIllegalModelIdField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody.replace(textEmbeddingModelId, "test_model_id"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(exception.getMessage(), allOf(containsString("Failed to find model"), containsString("OpenSearchStatusException")));
    }

    public void testRAGToolWithNeuralQueryAndLLM_withIllegalModelIdField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryAndLLMRequestBody.replace(textEmbeddingModelId, "test_model_id"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(exception.getMessage(), allOf(containsString("Failed to find model"), containsString("OpenSearchStatusException")));
    }

    @Override
    List<PromptHandler> promptHandlers() {
        PromptHandler RAGToolHandler = new PromptHandler() {
            @Override
            String response(String prompt) {
                if (prompt.contains("RAGTool")) {
                    return mockLLMResponseWithSource;
                } else {
                    return mockLLMResponseWithoutSource;
                }
            }

            @Override
            boolean apply(String prompt) {
                return true;
            }
        };
        return List.of(RAGToolHandler);
    }

    @Override
    String toolType() {
        return RAGTool.TYPE;
    }
}
