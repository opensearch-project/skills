/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import static org.hamcrest.Matchers.containsString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import lombok.SneakyThrows;

public class SearchAroundDocumentToolIT extends BaseAgentToolsIT {

    private static final String TEST_INDEX_NAME = "test_search_around_document_index";
    private static final String REGISTER_AGENT_RESOURCE =
        "org/opensearch/agent/tools/register_flow_agent_of_search_around_document_tool_request_body.json";

    private String registerAgentRequestBody;
    private String agentId;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        registerAgentRequestBody = Files.readString(Path.of(this.getClass().getClassLoader().getResource(REGISTER_AGENT_RESOURCE).toURI()));
        prepareDataIndex();
        agentId = createAgent(registerAgentRequestBody);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        deleteExternalIndices();
    }

    @SneakyThrows
    private void prepareDataIndex() {
        createIndexWithConfiguration(TEST_INDEX_NAME, """
            {
              "mappings": {
                "properties": {
                  "@timestamp": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"
                  },
                  "message": {
                    "type": "text"
                  },
                  "level": {
                    "type": "keyword"
                  }
                }
              }
            }""");

        // Index 7 documents with known timestamps and IDs
        addDocToIndex(
            TEST_INDEX_NAME,
            "doc1",
            List.of("@timestamp", "message", "level"),
            List.of("2025-01-01 09:00:00", "First log entry", "INFO")
        );
        addDocToIndex(
            TEST_INDEX_NAME,
            "doc2",
            List.of("@timestamp", "message", "level"),
            List.of("2025-01-01 09:10:00", "Second log entry", "INFO")
        );
        addDocToIndex(
            TEST_INDEX_NAME,
            "doc3",
            List.of("@timestamp", "message", "level"),
            List.of("2025-01-01 09:20:00", "Third log entry", "WARN")
        );
        addDocToIndex(
            TEST_INDEX_NAME,
            "doc4",
            List.of("@timestamp", "message", "level"),
            List.of("2025-01-01 09:30:00", "Fourth log entry - target", "ERROR")
        );
        addDocToIndex(
            TEST_INDEX_NAME,
            "doc5",
            List.of("@timestamp", "message", "level"),
            List.of("2025-01-01 09:40:00", "Fifth log entry", "WARN")
        );
        addDocToIndex(
            TEST_INDEX_NAME,
            "doc6",
            List.of("@timestamp", "message", "level"),
            List.of("2025-01-01 09:50:00", "Sixth log entry", "INFO")
        );
        addDocToIndex(
            TEST_INDEX_NAME,
            "doc7",
            List.of("@timestamp", "message", "level"),
            List.of("2025-01-01 10:00:00", "Seventh log entry", "ERROR")
        );
    }

    @SneakyThrows
    public void testSearchAroundDocument_basicSearch() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"doc_id\": \"doc4\", \"timestamp_field\": \"@timestamp\", \"count\": \"2\"}}",
                    TEST_INDEX_NAME
                )
        );

        JsonArray docs = JsonParser.parseString(result).getAsJsonArray();

        // Should have 5 documents: 2 before + target + 2 after
        assertEquals(5, docs.size());

        // Verify chronological order: doc2, doc3, doc4 (target), doc5, doc6
        assertEquals("doc2", docs.get(0).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc3", docs.get(1).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc4", docs.get(2).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc5", docs.get(3).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc6", docs.get(4).getAsJsonObject().get("_id").getAsString());
    }

    @SneakyThrows
    public void testSearchAroundDocument_countExceedsAvailable() {
        // doc1 is the first document, requesting 5 before but only 0 exist before it
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"doc_id\": \"doc1\", \"timestamp_field\": \"@timestamp\", \"count\": \"5\"}}",
                    TEST_INDEX_NAME
                )
        );

        JsonArray docs = JsonParser.parseString(result).getAsJsonArray();

        // Should have target + up to 5 after (doc2-doc7 = 6 after, but count=5)
        // No before docs since doc1 is the earliest
        assertEquals(6, docs.size());

        // First should be the target (doc1), followed by 5 after docs
        assertEquals("doc1", docs.get(0).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc2", docs.get(1).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc3", docs.get(2).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc4", docs.get(3).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc5", docs.get(4).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc6", docs.get(5).getAsJsonObject().get("_id").getAsString());
    }

    @SneakyThrows
    public void testSearchAroundDocument_lastDocument() {
        // doc7 is the last document, requesting 3 before
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"doc_id\": \"doc7\", \"timestamp_field\": \"@timestamp\", \"count\": \"3\"}}",
                    TEST_INDEX_NAME
                )
        );

        JsonArray docs = JsonParser.parseString(result).getAsJsonArray();

        // Should have 3 before + target, no after docs
        assertEquals(4, docs.size());

        assertEquals("doc4", docs.get(0).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc5", docs.get(1).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc6", docs.get(2).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc7", docs.get(3).getAsJsonObject().get("_id").getAsString());
    }

    @SneakyThrows
    public void testSearchAroundDocument_countOfOne() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"doc_id\": \"doc4\", \"timestamp_field\": \"@timestamp\", \"count\": \"1\"}}",
                    TEST_INDEX_NAME
                )
        );

        JsonArray docs = JsonParser.parseString(result).getAsJsonArray();

        // 1 before + target + 1 after = 3
        assertEquals(3, docs.size());

        assertEquals("doc3", docs.get(0).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc4", docs.get(1).getAsJsonObject().get("_id").getAsString());
        assertEquals("doc5", docs.get(2).getAsJsonObject().get("_id").getAsString());
    }

    @SneakyThrows
    public void testSearchAroundDocument_jsonInput() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"input\": \"{\\\"index\\\": \\\"%s\\\", \\\"doc_id\\\": \\\"doc4\\\", \\\"timestamp_field\\\": \\\"@timestamp\\\", \\\"count\\\": 2}\"}}",
                    TEST_INDEX_NAME
                )
        );

        JsonArray docs = JsonParser.parseString(result).getAsJsonArray();
        assertEquals(5, docs.size());
        assertEquals("doc4", docs.get(2).getAsJsonObject().get("_id").getAsString());
    }

    @SneakyThrows
    public void testSearchAroundDocument_responseContainsSource() {
        String result = executeAgent(
            agentId,
            String
                .format(
                    Locale.ROOT,
                    "{\"parameters\": {\"index\": \"%s\", \"doc_id\": \"doc4\", \"timestamp_field\": \"@timestamp\", \"count\": \"1\"}}",
                    TEST_INDEX_NAME
                )
        );

        JsonArray docs = JsonParser.parseString(result).getAsJsonArray();
        // Verify the target document has _source with correct fields
        JsonElement targetDoc = docs.get(1);
        assertTrue(targetDoc.getAsJsonObject().has("_source"));
        assertTrue(targetDoc.getAsJsonObject().has("_id"));
        assertTrue(targetDoc.getAsJsonObject().has("_index"));

        String source = targetDoc.getAsJsonObject().get("_source").toString();
        assertTrue(source.contains("Fourth log entry - target"));
        assertTrue(source.contains("ERROR"));
    }

    @SneakyThrows
    public void testSearchAroundDocument_nonExistentDoc() {
        Exception exception = assertThrows(
            Exception.class,
            () -> executeAgent(
                agentId,
                String
                    .format(
                        Locale.ROOT,
                        "{\"parameters\": {\"index\": \"%s\", \"doc_id\": \"nonexistent\", \"timestamp_field\": \"@timestamp\", \"count\": \"2\"}}",
                        TEST_INDEX_NAME
                    )
            )
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("Document not found"));
    }

    @SneakyThrows
    public void testSearchAroundDocument_missingRequiredParameters() {
        Exception exception = assertThrows(
            Exception.class,
            () -> executeAgent(agentId, String.format(Locale.ROOT, "{\"parameters\": {\"index\": \"%s\"}}", TEST_INDEX_NAME))
        );
        MatcherAssert.assertThat(exception.getMessage(), containsString("requires"));
    }
}
