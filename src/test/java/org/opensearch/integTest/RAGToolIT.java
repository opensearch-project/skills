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
import org.opensearch.client.ResponseException;

import lombok.SneakyThrows;

public class RAGToolIT extends BaseAgentToolsIT {

    public static String TEST_NEURAL_INDEX_NAME = "test_neural_index";
    public static String TEST_NEURAL_SPARSE_INDEX_NAME = "test_neural_sparse_index";
    private String textEmbeddingModelId;
    private String sparseEncodingModelId;
    private String registerAgentWithNeuralQueryRequestBody;
    private String registerAgentWithNeuralSparseQueryRequestBody;

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
            + "        \"dimension\": 384,\n"
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
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
    }

    public void testRAGToolWithNeuralQueryInFlowAgent() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody);

        // neural query to test match similar text, doc1 match with higher score
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"c\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_index\",\"_source\":{\"text\":\"a b\"},\"_id\":\"1\",\"_score\":0.60735726}\n"
                + "{\"_index\":\"test_neural_index\",\"_source\":{\"text\":\"hello world\"},\"_id\":\"0\",\"_score\":0.3785958}\n",
            result
        );

        // neural query to test match exact same text case, doc0 match with higher score
        String result1 = executeAgent(agentId, "{\"parameters\": {\"question\": \"hello\"}}");

        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_index\",\"_source\":{\"text\":\"hello world\"},\"_id\":\"0\",\"_score\":0.70875686}\n"
                + "{\"_index\":\"test_neural_index\",\"_source\":{\"text\":\"a b\"},\"_id\":\"1\",\"_score\":0.39044854}\n",
            result1
        );

        // if blank input, call onFailure and get exception
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("[input] is null or empty, can not process it."), containsString("illegal_argument_exception"))
            );

    }

    public void testRAGToolWithNeuralSparseQueryInFlowAgent() {
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
                allOf(containsString("[input] is null or empty, can not process it."), containsString("illegal_argument_exception"))
            );
    }

    public void testRAGToolWithNeuralSparseQueryInFlowAgent_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_sparse_index\",\"_source\":{},\"_id\":\"1\",\"_score\":1.2068367}\n",
            result
        );
    }

    public void testRAGToolWithNeuralQueryInFlowAgent_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_neural_index\",\"_source\":{},\"_id\":\"1\",\"_score\":0.7572355}\n"
                + "{\"_index\":\"test_neural_index\",\"_source\":{},\"_id\":\"0\",\"_score\":0.38389856}\n",
            result
        );
    }

    public void testRAGToolWithNeuralSparseQuery_withIllegalEmbeddingField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody.replace("\"embedding\"", "\"embedding2\""));
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

    public void testRAGToolWithNeuralQuery_withIllegalEmbeddingField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody.replace("\"embedding\"", "\"embedding2\""));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(
                    containsString("failed to create query: Field 'embedding2' is not knn_vector type."),
                    containsString("query_shard_exception")
                )
            );
    }

    public void testRAGToolWithNeuralSparseQueryInFlowAgent_withIllegalIndexField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody.replace(TEST_NEURAL_SPARSE_INDEX_NAME, "test_index2"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("no such index [test_index2]"), containsString("index_not_found_exception"))
            );
    }

    public void testRAGToolWithNeuralQueryInFlowAgent_withIllegalIndexField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody.replace(TEST_NEURAL_INDEX_NAME, "test_index2"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(
                exception.getMessage(),
                allOf(containsString("no such index [test_index2]"), containsString("index_not_found_exception"))
            );
    }

    public void testRAGToolWithNeuralSparseQueryInFlowAgent_withIllegalModelIdField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralSparseQueryRequestBody.replace(sparseEncodingModelId, "test_model_id"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(exception.getMessage(), allOf(containsString("Failed to find model"), containsString("status_exception")));
    }

    public void testRAGToolWithNeuralQueryInFlowAgent_withIllegalModelIdField_thenThrowException() {
        String agentId = createAgent(registerAgentWithNeuralQueryRequestBody.replace(textEmbeddingModelId, "test_model_id"));
        Exception exception = assertThrows(ResponseException.class, () -> executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}"));

        org.hamcrest.MatcherAssert
            .assertThat(exception.getMessage(), allOf(containsString("Failed to find model"), containsString("status_exception")));
    }
}
