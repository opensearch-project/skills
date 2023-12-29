/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Response;

import lombok.SneakyThrows;
import org.opensearch.ml.common.agent.LLMSpec;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLToolSpec;

public class NeuralSparseSearchToolIT extends BaseAgentToolsIT {
    public static String TEST_INDEX_NAME = "test_index";
//    public static String
    private String modelId;

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
        createIndexWithConfiguration(TEST_INDEX_NAME, "{\n" +
                "  \"mappings\": {\n" +
                "    \"properties\": {\n" +
                "      \"text\": {\n" +
                "        \"type\": \"text\"\n" +
                "      },\n" +
                "      \"embedding\": {\n" +
                "        \"type\": \"rank_features\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");
        addDocToIndex(TEST_INDEX_NAME, "0", List.of("text", "embedding"),List.of("text doc 1", Map.of("hello",1, "world",2)));
        addDocToIndex(TEST_INDEX_NAME, "1", List.of("text", "embedding"),List.of("text doc 2", Map.of("a",3, "b",4)));
        addDocToIndex(TEST_INDEX_NAME, "2", List.of("text", "embedding"),List.of("text doc 3", Map.of("test",5, "mock",6)));
    }

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        prepareModel();
        prepareIndex();
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
    }

    public void testClient() {
        Response response = makeRequest(client(), "GET", TEST_INDEX_NAME+"/_search", null, (String) null, null);
        logger.info("here");
        MLToolSpec.builder();
    }
}
