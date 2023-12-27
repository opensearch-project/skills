/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.opensearch.client.Response;

import lombok.SneakyThrows;

public class NeuralSparseSearchToolIT extends BaseAgentToolsIT {
    private String modelId;

    @Before
    @SneakyThrows
    public void prepareModel() {
        String requestBody = Files
            .readString(
                Path
                    .of(
                        this
                            .getClass()
                            .getClassLoader()
                            .getResource("org/opensearch/agent/tools/RegisterSparseEncodingModelRequestBody.json")
                            .toURI()
                    )
            );
        modelId = registerModelThenDeploy(requestBody);
    }

    public void testClient() {
        Response response = makeRequest(client(), "GET", "/_cat/indices", null, (String) null, null);
        logger.info("here");
    }
}
