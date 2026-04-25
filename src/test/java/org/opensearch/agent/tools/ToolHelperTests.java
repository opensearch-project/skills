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
        String mapBlock = """
            {
                "event": {
                    "properties": {
                        "field1": {
                            "type": "string"
                        }
                    }
                }
            }

            """;
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    private Map<String, Object> prepareMap2() {
        String mapBlock = """
            {
                "event": {
                    "properties": {
                        "field2": {
                            "type": "string"
                        }
                    }
                }
            }

            """;
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    private Map<String, Object> prepareNormalMap1() {
        String mapBlock = """
            {
                "event1": {
                    "properties": {
                        "field1": {
                            "type": "string"
                        }
                    }
                },
                "replace" : {
                    "type":"string"
                }

            }

            """;
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    private Map<String, Object> prepareNormalMap2() {
        String mapBlock = """
            {
                "event2": {
                    "properties": {
                        "field2": {
                            "type": "string"
                        }
                    }
                },
                "replace" : {
                    "type":"keyword"
                }
            }

            """;
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    @Test
    public void testMergeTwoObjectMaps() {
        String mapBlock = """
            {
                "event": {
                    "properties": {
                        "field1": {
                            "type": "string"
                        },
                        "field2": {
                            "type": "string"
                        }
                    }
                }
            }

            """;
        Map<String, Object> allFields = new HashMap<>();
        Map<String, Object> map1 = prepareMap1();
        Map<String, Object> map2 = prepareMap2();
        MergeRuleHelper.merge(map1, allFields);
        MergeRuleHelper.merge(map2, allFields);
        assertEquals(allFields, gson.fromJson(mapBlock, Map.class));
    }

    @Test
    public void testMergeTwoNormalMaps() {
        String mapBlock = """
            {
                "event1": {
                    "properties": {
                        "field1": {
                            "type": "string"
                        }
                    }
                },
                "event2": {
                    "properties": {
                        "field2": {
                            "type": "string"
                        }
                    }
                },
                "replace" : {
                    "type":"keyword"
                }
            }

            """;
        Map<String, Object> allFields = new HashMap<>();
        Map<String, Object> map1 = prepareNormalMap1();
        Map<String, Object> map2 = prepareNormalMap2();
        MergeRuleHelper.merge(map1, allFields);
        MergeRuleHelper.merge(map2, allFields);
        assertEquals(allFields, gson.fromJson(mapBlock, Map.class));
    }

    @Test
    public void testMergeTwoDeepMaps() {
        String mapBlock = """
            {
                "event": {
                    "properties": {
                        "field1": {
                            "type": "string"
                        },
                        "field2": {
                            "type": "string"
                        },
                        "deep": {
                            "properties": {
                                "field1": {
                                    "type": "string"
                                },
                                "field2": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }

            }

            """;
        Map<String, Object> allFields = new HashMap<>();
        Map<String, Object> map1 = prepareDeepMap1();
        Map<String, Object> map2 = prepareDeepMap2();
        MergeRuleHelper.merge(map1, allFields);
        MergeRuleHelper.merge(map2, allFields);
        assertEquals(allFields, gson.fromJson(mapBlock, Map.class));
    }

    private Map<String, Object> prepareDeepMap1() {
        String mapBlock = """
            {
                "event": {
                    "properties": {
                        "field1": {
                            "type": "string"
                        },
                        "deep": {
                            "properties": {
                                "field1": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }

            }

            """;
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    private Map<String, Object> prepareDeepMap2() {
        String mapBlock = """
            {
                "event": {
                    "properties": {
                        "field2": {
                            "type": "string"
                        },
                        "deep": {
                            "properties": {
                                "field2": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            }

            """;
        Map<String, Object> tmpMap = gson.fromJson(mapBlock, Map.class);
        return tmpMap;
    }

    @Test
    public void testExtractFieldNamesTypes_OtelLogMapping() {
        String mapping = """
            {
                "@timestamp": { "type": "date" },
                "observedTimestamp": { "type": "date" },
                "time": { "type": "date" },
                "body": {
                    "type": "text",
                    "fields": { "keyword": { "type": "keyword" } }
                },
                "severityText": { "type": "keyword" },
                "severityNumber": { "type": "integer" },
                "traceId": { "type": "keyword" },
                "spanId": { "type": "keyword" },
                "flags": { "type": "integer" },
                "attributes": {
                    "type": "object",
                    "properties": {
                        "otelServiceName": { "type": "keyword" },
                        "otelTraceID": { "type": "keyword" },
                        "otelSpanID": { "type": "keyword" },
                        "otelTraceSampled": { "type": "boolean" },
                        "thread.name": { "type": "keyword" },
                        "thread.id": { "type": "long" },
                        "exception": {
                            "type": "object",
                            "properties": {
                                "type": { "type": "keyword" },
                                "message": { "type": "text" },
                                "stacktrace": { "type": "text" }
                            }
                        },
                        "http.url": { "type": "keyword" },
                        "http.method": { "type": "keyword" },
                        "http.route": { "type": "keyword" },
                        "http.target": { "type": "keyword" },
                        "http.user_agent": { "type": "text" },
                        "db.system": { "type": "keyword" },
                        "db.operation": { "type": "keyword" },
                        "code.namespace": { "type": "keyword" },
                        "code.function": { "type": "keyword" },
                        "client.address": { "type": "ip" },
                        "owner.id": { "type": "integer" },
                        "pet.id": { "type": "integer" },
                        "hibernate.entity": { "type": "keyword" }
                    }
                },
                "resource": {
                    "type": "object",
                    "properties": {
                        "attributes": {
                            "type": "object",
                            "properties": {
                                "service.name": { "type": "keyword" },
                                "service.instance.id": { "type": "keyword" },
                                "service.version": { "type": "keyword" },
                                "telemetry.sdk.name": { "type": "keyword" },
                                "telemetry.sdk.language": { "type": "keyword" },
                                "telemetry.sdk.version": { "type": "keyword" },
                                "k8s.namespace.name": { "type": "keyword" },
                                "k8s.pod.name": { "type": "keyword" },
                                "k8s.container.name": { "type": "keyword" },
                                "host.name": { "type": "keyword" },
                                "cloud.provider": { "type": "keyword" },
                                "cloud.region": { "type": "keyword" }
                            }
                        }
                    }
                },
                "instrumentationScope": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "keyword" },
                        "version": { "type": "keyword" }
                    }
                }
            }
            """;
        Map<String, Object> indexMappings = gson.fromJson(mapping, Map.class);
        Map<String, String> result = new HashMap<>();
        ToolHelper.extractFieldNamesTypes(indexMappings, result, "", true);

        // Date fields at top level
        assertEquals("date", result.get("@timestamp"));
        assertEquals("date", result.get("observedTimestamp"));
        assertEquals("date", result.get("time"));

        // Text field with keyword sub-field
        assertEquals("text", result.get("body"));
        assertEquals("keyword", result.get("body.keyword"));

        // Top-level leaf fields
        assertEquals("keyword", result.get("severityText"));
        assertEquals("integer", result.get("severityNumber"));

        // Nested under attributes (object type skipped, children flattened)
        assertEquals("keyword", result.get("attributes.otelServiceName"));
        assertEquals("long", result.get("attributes.thread.id"));
        assertEquals("ip", result.get("attributes.client.address"));
        assertEquals("integer", result.get("attributes.owner.id"));

        // Deeply nested: attributes.exception.*
        assertEquals("keyword", result.get("attributes.exception.type"));
        assertEquals("text", result.get("attributes.exception.message"));

        // Deeply nested: resource.attributes.* — the key question
        assertEquals("keyword", result.get("resource.attributes.service.name"));
        assertEquals("keyword", result.get("resource.attributes.cloud.region"));
        assertEquals("keyword", result.get("resource.attributes.k8s.namespace.name"));
        assertEquals("keyword", result.get("resource.attributes.host.name"));

        // instrumentationScope.*
        assertEquals("keyword", result.get("instrumentationScope.name"));
        assertEquals("keyword", result.get("instrumentationScope.version"));

        // Verify date fields are found
        java.util.Set<String> dateFields = ToolHelper.findDateTypeFields(result);
        assertEquals(3, dateFields.size());
        assert dateFields.contains("@timestamp");
        assert dateFields.contains("observedTimestamp");
        assert dateFields.contains("time");

        // Verify object types themselves are NOT in the result
        assert !result.containsKey("attributes");
        assert !result.containsKey("resource");
        assert !result.containsKey("resource.attributes");
        assert !result.containsKey("instrumentationScope");
    }

}
