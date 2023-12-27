package org.opensearch.integTest;

import org.opensearch.client.Response;

public class NeuralSparseSearchToolIT extends BaseAgentToolsIT {
    public void testClient() {
        Response response = makeRequest(
                client(),
                "GET",
                "/_cat/indices",
                null,
                (String) null,
                null
        );
        logger.info("here");
    }
}
