/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.ResponseException;

import lombok.SneakyThrows;

public class VectorDBToolIT extends BaseAgentToolsIT {

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
                            .getResource("org/opensearch/agent/tools/register_text_embedding_model_request_body.json")
                            .toURI()
                    )
            );
        modelId = registerModelThenDeploy(requestBody);
    }

    @SneakyThrows
    private void prepareIndex() {

        String pipelineConfig = "{\n"
            + "  \"description\": \"text embedding pipeline\",\n"
            + "  \"processors\": [\n"
            + "    {\n"
            + "      \"text_embedding\": {\n"
            + "        \"model_id\": \""
            + modelId
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

        createIndexWithConfiguration(TEST_INDEX_NAME, indexMapping);

        addDocToIndex(TEST_INDEX_NAME, "0", List.of("text"), List.of("hello world"));

        addDocToIndex(TEST_INDEX_NAME, "1", List.of("text"), List.of("a b"));
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
                            .getResource("org/opensearch/agent/tools/register_flow_agent_of_vectordb_tool_request_body.json")
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

    public void testVectorDBToolInFlowAgent() {
        String agentId = createAgent(registerAgentRequestBody);

        // match similar text, doc1 match with higher score
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"c\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index\",\"_source\":{\"text\":\"a b\"},\"_id\":\"1\",\"_score\":0.60735726}\n"
                + "{\"_index\":\"test_index\",\"_source\":{\"text\":\"hello world\"},\"_id\":\"0\",\"_score\":0.3785958}\n",
            result
        );

        // match exact same text case, doc0 match with higher score
        String result1 = executeAgent(agentId, "{\"parameters\": {\"question\": \"hello\"}}");

        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index\",\"_source\":{\"text\":\"hello world\"},\"_id\":\"0\",\"_score\":0.70875686}\n"
                + "{\"_index\":\"test_index\",\"_source\":{\"text\":\"a b\"},\"_id\":\"1\",\"_score\":0.39044854}\n",
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

    public void testVectorDBToolInFlowAgent_withIllegalSourceField_thenGetEmptySource() {
        String agentId = createAgent(registerAgentRequestBody.replace("text", "text2"));
        String result = executeAgent(agentId, "{\"parameters\": {\"question\": \"a\"}}");
        assertEquals(
            "The agent execute response not equal with expected.",
            "{\"_index\":\"test_index\",\"_source\":{},\"_id\":\"1\",\"_score\":0.7572355}\n"
                + "{\"_index\":\"test_index\",\"_source\":{},\"_id\":\"0\",\"_score\":0.38389856}\n",
            result
        );
    }

    public void testVectorDBToolInFlowAgent_withIllegalEmbeddingField_thenThrowException() {
        String agentId = createAgent(registerAgentRequestBody.replace("\"embedding\"", "\"embedding2\""));
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
