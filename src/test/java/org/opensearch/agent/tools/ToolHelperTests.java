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
import org.opensearch.agent.tools.utils.mergeMetaData.MergeRuleHelper;

import com.google.gson.Gson;

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

    private Gson gson = new Gson();

    private Map<String, Object> prepareMap1() {
        String mapBlock = "{\n"
            + "    \"event\": {\n"
            + "        \"properties\": {\n"
            + "            \"field1\": {\n"
            + "                \"type\": \"string\"\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}\n"
            + "\n";
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    private Map<String, Object> prepareMap2() {
        String mapBlock = "{\n"
            + "    \"event\": {\n"
            + "        \"properties\": {\n"
            + "            \"field2\": {\n"
            + "                \"type\": \"string\"\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}\n"
            + "\n";
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    private Map<String, Object> prepareNormalMap1() {
        String mapBlock = "{\n"
            + "    \"event1\": {\n"
            + "        \"properties\": {\n"
            + "            \"field1\": {\n"
            + "                \"type\": \"string\"\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"replace\" : {\n"
            + "        \"type\":\"string\"\n"
            + "    }\n"
            + "\n"
            + "}\n"
            + "\n";
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    private Map<String, Object> prepareNormalMap2() {
        String mapBlock = "{\n"
            + "    \"event2\": {\n"
            + "        \"properties\": {\n"
            + "            \"field2\": {\n"
            + "                \"type\": \"string\"\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"replace\" : {\n"
            + "        \"type\":\"keyword\"\n"
            + "    }\n"
            + "}\n"
            + "\n";
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    @Test
    public void testMergeTwoObjectMaps() {
        String mapBlock = "{\n"
            + "    \"event\": {\n"
            + "        \"properties\": {\n"
            + "            \"field1\": {\n"
            + "                \"type\": \"string\"\n"
            + "            },\n"
            + "            \"field2\": {\n"
            + "                \"type\": \"string\"\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}\n"
            + "\n";
        Map<String, Object> allFields = new HashMap<>();
        Map<String, Object> map1 = prepareMap1();
        Map<String, Object> map2 = prepareMap2();
        MergeRuleHelper.merge(map1, allFields);
        MergeRuleHelper.merge(map2, allFields);
        assertEquals(allFields, gson.fromJson(mapBlock, Map.class));
    }

    @Test
    public void testMergeTwoNormalMaps() {
        String mapBlock = "{\n"
            + "    \"event1\": {\n"
            + "        \"properties\": {\n"
            + "            \"field1\": {\n"
            + "                \"type\": \"string\"\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"event2\": {\n"
            + "        \"properties\": {\n"
            + "            \"field2\": {\n"
            + "                \"type\": \"string\"\n"
            + "            }\n"
            + "        }\n"
            + "    },\n"
            + "    \"replace\" : {\n"
            + "        \"type\":\"keyword\"\n"
            + "    }\n"
            + "}\n"
            + "\n";
        Map<String, Object> allFields = new HashMap<>();
        Map<String, Object> map1 = prepareNormalMap1();
        Map<String, Object> map2 = prepareNormalMap2();
        MergeRuleHelper.merge(map1, allFields);
        MergeRuleHelper.merge(map2, allFields);
        assertEquals(allFields, gson.fromJson(mapBlock, Map.class));
    }

    @Test
    public void testMergeTwoDeepMaps() {
        String mapBlock = "{\n"
            + "    \"event\": {\n"
            + "        \"properties\": {\n"
            + "            \"field1\": {\n"
            + "                \"type\": \"string\"\n"
            + "            },\n"
            + "            \"field2\": {\n"
            + "                \"type\": \"string\"\n"
            + "            },\n"
            + "            \"deep\": {\n"
            + "                \"properties\": {\n"
            + "                    \"field1\": {\n"
            + "                        \"type\": \"string\"\n"
            + "                    },\n"
            + "                    \"field2\": {\n"
            + "                        \"type\": \"string\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "\n"
            + "}\n"
            + "\n";
        Map<String, Object> allFields = new HashMap<>();
        Map<String, Object> map1 = prepareDeepMap1();
        Map<String, Object> map2 = prepareDeepMap2();
        MergeRuleHelper.merge(map1, allFields);
        MergeRuleHelper.merge(map2, allFields);
        assertEquals(allFields, gson.fromJson(mapBlock, Map.class));
    }

    private Map<String, Object> prepareDeepMap1() {
        String mapBlock = "{\n"
            + "    \"event\": {\n"
            + "        \"properties\": {\n"
            + "            \"field1\": {\n"
            + "                \"type\": \"string\"\n"
            + "            },\n"
            + "            \"deep\": {\n"
            + "                \"properties\": {\n"
            + "                    \"field1\": {\n"
            + "                        \"type\": \"string\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "\n"
            + "}\n"
            + "\n";
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    private Map<String, Object> prepareDeepMap2() {
        String mapBlock = "{\n"
            + "    \"event\": {\n"
            + "        \"properties\": {\n"
            + "            \"field2\": {\n"
            + "                \"type\": \"string\"\n"
            + "            },\n"
            + "            \"deep\": {\n"
            + "                \"properties\": {\n"
            + "                    \"field2\": {\n"
            + "                        \"type\": \"string\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}\n"
            + "\n";
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

}
