/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.opensearch.agent.tools.utils.ToolHelper;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class ToolHelperTests {
    @Test
    public void TestExtractFieldNamesTypes() {
        Map<String, Object> indexMappings = Map
            .of(
                "response",
                Map.of("type", "integer"),
                "responseLatency",
                Map.of("type", "float"),
                "date",
                Map.of("type", "date"),
                "objectA",
                Map.of("type", "object", "properties", Map.of("subA", Map.of("type", "keyword"))),
                "objectB",
                Map.of("properties", Map.of("subB", Map.of("type", "keyword"))),
                "textC",
                Map.of("type", "text", "fields", Map.of("subC", Map.of("type", "keyword"))),
                "aliasD",
                Map.of("type", "alias", "path", "date")
            );
        Map<String, String> result = new HashMap<>();
        ToolHelper.extractFieldNamesTypes(indexMappings, result, "", true);
        assertMapEquals(
            result,
            Map
                .of(
                    "response",
                    "integer",
                    "responseLatency",
                    "float",
                    "date",
                    "date",
                    "objectA.subA",
                    "keyword",
                    "objectB.subB",
                    "keyword",
                    "textC",
                    "text",
                    "textC.subC",
                    "keyword"
                )
        );

        Map<String, String> result1 = new HashMap<>();
        ToolHelper.extractFieldNamesTypes(indexMappings, result1, "", false);
        assertMapEquals(
            result1,
            Map
                .of(
                    "response",
                    "integer",
                    "responseLatency",
                    "float",
                    "date",
                    "date",
                    "objectA.subA",
                    "keyword",
                    "objectB.subB",
                    "keyword",
                    "textC",
                    "text"
                )
        );
    }

    private void assertMapEquals(Map<String, String> expected, Map<String, String> actual) {
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(entry.getValue(), actual.get(entry.getKey()));
        }
    }
}
