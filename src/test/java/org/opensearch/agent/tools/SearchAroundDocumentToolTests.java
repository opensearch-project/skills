/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.transport.client.Client;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class SearchAroundDocumentToolTests {

    private Client client;
    private SearchAroundDocumentTool tool;
    private static final Gson GSON = new Gson();

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        client = mock(Client.class);
        SearchAroundDocumentTool.Factory.getInstance().init(client, NamedXContentRegistry.EMPTY);
        tool = SearchAroundDocumentTool.Factory.getInstance().create(Collections.emptyMap());
    }

    private SearchHit createMockHit(String id, String index, Map<String, Object> source, Object[] sortValues) {
        SearchHit hit = mock(SearchHit.class);
        when(hit.getId()).thenReturn(id);
        when(hit.getIndex()).thenReturn(index);
        when(hit.getScore()).thenReturn(1.0f);
        when(hit.getSourceAsMap()).thenReturn(source);
        when(hit.getSortValues()).thenReturn(sortValues);
        return hit;
    }

    private SearchResponse createMockSearchResponse(SearchHit[] hits) {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        when(searchHits.getHits()).thenReturn(hits);
        when(response.getHits()).thenReturn(searchHits);
        return response;
    }

    private void mockThreeSearchCalls(SearchResponse targetResponse, SearchResponse beforeResponse, SearchResponse afterResponse) {
        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            int call = callCount.getAndIncrement();
            switch (call) {
                case 0:
                    listener.onResponse(targetResponse);
                    break;
                case 1:
                    listener.onResponse(beforeResponse);
                    break;
                case 2:
                    listener.onResponse(afterResponse);
                    break;
                default:
                    listener.onFailure(new RuntimeException("Unexpected search call"));
            }
            return null;
        }).when(client).search(any(SearchRequest.class), any());
    }

    // ========== Validate Tests ==========

    @Test
    public void testValidateWithNullParameters() {
        assertFalse(tool.validate(null));
    }

    @Test
    public void testValidateWithEmptyParameters() {
        assertFalse(tool.validate(Collections.emptyMap()));
    }

    @Test
    public void testValidateWithJsonInput() {
        Map<String, String> params = new HashMap<>();
        params.put("input", "{\"index\":\"test\",\"doc_id\":\"1\",\"timestamp_field\":\"@timestamp\",\"count\":5}");
        assertTrue(tool.validate(params));
    }

    @Test
    public void testValidateWithDirectParameters() {
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "5");
        assertTrue(tool.validate(params));
    }

    @Test
    public void testValidateWithMissingParameters() {
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc1");
        assertFalse(tool.validate(params));
    }

    @Test
    public void testValidateWithEmptyValues() {
        Map<String, String> params = new HashMap<>();
        params.put("index", "");
        params.put("doc_id", "doc1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "5");
        assertFalse(tool.validate(params));
    }

    // ========== Factory Tests ==========

    @Test
    public void testFactoryCreate() {
        SearchAroundDocumentTool.Factory factory = SearchAroundDocumentTool.Factory.getInstance();
        SearchAroundDocumentTool createdTool = factory.create(Collections.emptyMap());
        assertNotNull(createdTool);
        assertEquals(SearchAroundDocumentTool.TYPE, createdTool.getType());
    }

    @Test
    public void testFactoryGetInstance() {
        SearchAroundDocumentTool.Factory factory1 = SearchAroundDocumentTool.Factory.getInstance();
        SearchAroundDocumentTool.Factory factory2 = SearchAroundDocumentTool.Factory.getInstance();
        assertTrue(factory1 == factory2);
    }

    @Test
    public void testFactoryDefaults() {
        SearchAroundDocumentTool.Factory factory = SearchAroundDocumentTool.Factory.getInstance();
        assertEquals(SearchAroundDocumentTool.TYPE, factory.getDefaultType());
        assertNotNull(factory.getDefaultDescription());
        assertNull(factory.getDefaultVersion());
        assertNotNull(factory.getDefaultAttributes());
    }

    // ========== Tool Metadata Tests ==========

    @Test
    public void testGetType() {
        assertEquals("SearchAroundDocumentTool", tool.getType());
    }

    @Test
    public void testGetVersion() {
        assertNull(tool.getVersion());
    }

    // ========== Run - Success Tests ==========

    @Test
    public void testRunWithDirectParameters() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };

        SearchHit targetHit = createMockHit(
            "target-doc",
            "test-index",
            Map.of("@timestamp", "2024-01-01T00:00:05", "message", "target"),
            sortValues
        );

        SearchHit beforeHit1 = createMockHit(
            "before-1",
            "test-index",
            Map.of("@timestamp", "2024-01-01T00:00:03", "message", "before1"),
            new Object[] { 998L, 3L }
        );
        SearchHit beforeHit2 = createMockHit(
            "before-2",
            "test-index",
            Map.of("@timestamp", "2024-01-01T00:00:04", "message", "before2"),
            new Object[] { 999L, 4L }
        );

        SearchHit afterHit1 = createMockHit(
            "after-1",
            "test-index",
            Map.of("@timestamp", "2024-01-01T00:00:06", "message", "after1"),
            new Object[] { 1001L, 6L }
        );
        SearchHit afterHit2 = createMockHit(
            "after-2",
            "test-index",
            Map.of("@timestamp", "2024-01-01T00:00:07", "message", "after2"),
            new Object[] { 1002L, 7L }
        );

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });
        // Before: returned in DESC order (before2, before1) - tool reverses to chronological
        SearchResponse beforeResponse = createMockSearchResponse(new SearchHit[] { beforeHit2, beforeHit1 });
        SearchResponse afterResponse = createMockSearchResponse(new SearchHit[] { afterHit1, afterHit2 });

        mockThreeSearchCalls(targetResponse, beforeResponse, afterResponse);

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "target-doc");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        String result = future.get();
        assertNotNull(result);

        List<Map<String, Object>> docs = GSON.fromJson(result, new TypeToken<List<Map<String, Object>>>() {
        }.getType());
        assertEquals(5, docs.size());

        // Verify chronological order: before1, before2, target, after1, after2
        assertEquals("before-1", docs.get(0).get("_id"));
        assertEquals("before-2", docs.get(1).get("_id"));
        assertEquals("target-doc", docs.get(2).get("_id"));
        assertEquals("after-1", docs.get(3).get("_id"));
        assertEquals("after-2", docs.get(4).get("_id"));
    }

    @Test
    public void testRunWithJsonInput() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };

        SearchHit targetHit = createMockHit(
            "doc-1",
            "my-index",
            Map.of("@timestamp", "2024-01-01T00:00:05", "message", "target"),
            sortValues
        );

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });
        SearchResponse beforeResponse = createMockSearchResponse(new SearchHit[] {});
        SearchResponse afterResponse = createMockSearchResponse(new SearchHit[] {});

        mockThreeSearchCalls(targetResponse, beforeResponse, afterResponse);

        Map<String, String> params = new HashMap<>();
        params.put("input", "{\"index\":\"my-index\",\"doc_id\":\"doc-1\",\"timestamp_field\":\"@timestamp\",\"count\":2}");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        String result = future.get();
        assertNotNull(result);

        List<Map<String, Object>> docs = GSON.fromJson(result, new TypeToken<List<Map<String, Object>>>() {
        }.getType());
        assertEquals(1, docs.size());
        assertEquals("doc-1", docs.get(0).get("_id"));
    }

    @Test
    public void testRunWithNoBeforeOrAfterDocuments() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };

        SearchHit targetHit = createMockHit("doc-1", "test-index", Map.of("message", "only doc"), sortValues);

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });
        SearchResponse beforeResponse = createMockSearchResponse(new SearchHit[] {});
        SearchResponse afterResponse = createMockSearchResponse(new SearchHit[] {});

        mockThreeSearchCalls(targetResponse, beforeResponse, afterResponse);

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "5");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        String result = future.get();
        List<Map<String, Object>> docs = GSON.fromJson(result, new TypeToken<List<Map<String, Object>>>() {
        }.getType());
        assertEquals(1, docs.size());
        assertEquals("doc-1", docs.get(0).get("_id"));
    }

    @Test
    public void testRunWithCountAsDouble() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };

        SearchHit targetHit = createMockHit("doc-1", "test-index", Map.of("message", "target"), sortValues);

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });
        SearchResponse beforeResponse = createMockSearchResponse(new SearchHit[] {});
        SearchResponse afterResponse = createMockSearchResponse(new SearchHit[] {});

        mockThreeSearchCalls(targetResponse, beforeResponse, afterResponse);

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2.0");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        String result = future.get();
        assertNotNull(result);
    }

    @Test
    public void testRunResponseContainsSortValues() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };

        SearchHit targetHit = createMockHit("doc-1", "test-index", Map.of("message", "target"), sortValues);

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });
        SearchResponse beforeResponse = createMockSearchResponse(new SearchHit[] {});
        SearchResponse afterResponse = createMockSearchResponse(new SearchHit[] {});

        mockThreeSearchCalls(targetResponse, beforeResponse, afterResponse);

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "1");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        String result = future.get();
        List<Map<String, Object>> docs = GSON.fromJson(result, new TypeToken<List<Map<String, Object>>>() {
        }.getType());
        assertEquals(1, docs.size());

        Map<String, Object> doc = docs.get(0);
        assertEquals("doc-1", doc.get("_id"));
        assertEquals("test-index", doc.get("_index"));
        assertNotNull(doc.get("_source"));
        assertNotNull(doc.get("sort"));
    }

    // ========== Run - Error Tests ==========

    @Test
    public void testRunWithDocumentNotFound() throws Exception {
        SearchResponse emptyResponse = createMockSearchResponse(new SearchHit[] {});

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(emptyResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "nonexistent");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            future.get();
            assertTrue("Should have thrown", false);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("Document not found"));
        }
    }

    @Test
    public void testRunWithMissingSortValues() throws Exception {
        SearchHit targetHit = createMockHit("doc-1", "test-index", Map.of("message", "target"), new Object[] {});

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(targetResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            future.get();
            assertTrue("Should have thrown", false);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("sort values"));
        }
    }

    @Test
    public void testRunWithNullSortValues() throws Exception {
        SearchHit targetHit = createMockHit("doc-1", "test-index", Map.of("message", "target"), null);

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(targetResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            future.get();
            assertTrue("Should have thrown", false);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("sort values"));
        }
    }

    @Test
    public void testRunWithOnlySingleSortValue() throws Exception {
        SearchHit targetHit = createMockHit("doc-1", "test-index", Map.of("message", "target"), new Object[] { 1000L });

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(targetResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            future.get();
            assertTrue("Should have thrown", false);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("sort values"));
        }
    }

    @Test
    public void testRunWithMissingRequiredParameters() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            future.get();
            assertTrue("Should have thrown", false);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("requires"));
        }
    }

    @Test
    public void testRunWithTargetSearchFailure() throws Exception {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Target search failed"));
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            future.get();
            assertTrue("Should have thrown", false);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Target search failed", e.getCause().getMessage());
        }
    }

    @Test
    public void testRunWithBeforeSearchFailure() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };
        SearchHit targetHit = createMockHit("doc-1", "test-index", Map.of("message", "target"), sortValues);
        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });

        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            int call = callCount.getAndIncrement();
            if (call == 0) {
                listener.onResponse(targetResponse);
            } else {
                listener.onFailure(new RuntimeException("Before search failed"));
            }
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            future.get();
            assertTrue("Should have thrown", false);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Before search failed", e.getCause().getMessage());
        }
    }

    @Test
    public void testRunWithAfterSearchFailure() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };
        SearchHit targetHit = createMockHit("doc-1", "test-index", Map.of("message", "target"), sortValues);
        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });
        SearchResponse beforeResponse = createMockSearchResponse(new SearchHit[] {});

        AtomicInteger callCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            int call = callCount.getAndIncrement();
            if (call == 0) {
                listener.onResponse(targetResponse);
            } else if (call == 1) {
                listener.onResponse(beforeResponse);
            } else {
                listener.onFailure(new RuntimeException("After search failed"));
            }
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "2");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        try {
            future.get();
            assertTrue("Should have thrown", false);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("After search failed", e.getCause().getMessage());
        }
    }

    // ========== Run - Ordering Tests ==========

    @Test
    public void testRunBeforeDocsAreReversedToChronological() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };

        SearchHit targetHit = createMockHit("target", "idx", Map.of("ts", 1000), sortValues);

        // Before search returns in DESC order: doc3 (newest before), doc2, doc1 (oldest before)
        SearchHit beforeHit3 = createMockHit("before-3", "idx", Map.of("ts", 999), new Object[] { 999L, 4L });
        SearchHit beforeHit2 = createMockHit("before-2", "idx", Map.of("ts", 998), new Object[] { 998L, 3L });
        SearchHit beforeHit1 = createMockHit("before-1", "idx", Map.of("ts", 997), new Object[] { 997L, 2L });

        SearchHit afterHit1 = createMockHit("after-1", "idx", Map.of("ts", 1001), new Object[] { 1001L, 6L });

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });
        SearchResponse beforeResponse = createMockSearchResponse(new SearchHit[] { beforeHit3, beforeHit2, beforeHit1 });
        SearchResponse afterResponse = createMockSearchResponse(new SearchHit[] { afterHit1 });

        mockThreeSearchCalls(targetResponse, beforeResponse, afterResponse);

        Map<String, String> params = new HashMap<>();
        params.put("index", "idx");
        params.put("doc_id", "target");
        params.put("timestamp_field", "ts");
        params.put("count", "3");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        String result = future.get();
        List<Map<String, Object>> docs = GSON.fromJson(result, new TypeToken<List<Map<String, Object>>>() {
        }.getType());
        assertEquals(5, docs.size());

        // Before docs should be reversed to chronological (oldest first)
        assertEquals("before-1", docs.get(0).get("_id"));
        assertEquals("before-2", docs.get(1).get("_id"));
        assertEquals("before-3", docs.get(2).get("_id"));
        assertEquals("target", docs.get(3).get("_id"));
        assertEquals("after-1", docs.get(4).get("_id"));
    }

    @Test
    public void testRunResponseDocStructure() throws Exception {
        Object[] sortValues = new Object[] { 1000L, 5L };
        Map<String, Object> source = Map.of("@timestamp", "2024-01-01", "level", "INFO", "message", "test log");

        SearchHit targetHit = createMockHit("doc-1", "logs-index", source, sortValues);

        SearchResponse targetResponse = createMockSearchResponse(new SearchHit[] { targetHit });
        SearchResponse beforeResponse = createMockSearchResponse(new SearchHit[] {});
        SearchResponse afterResponse = createMockSearchResponse(new SearchHit[] {});

        mockThreeSearchCalls(targetResponse, beforeResponse, afterResponse);

        Map<String, String> params = new HashMap<>();
        params.put("index", "logs-index");
        params.put("doc_id", "doc-1");
        params.put("timestamp_field", "@timestamp");
        params.put("count", "1");

        CompletableFuture<String> future = new CompletableFuture<>();
        tool.run(params, ActionListener.wrap(future::complete, future::completeExceptionally));

        String result = future.get();
        List<Map<String, Object>> docs = GSON.fromJson(result, new TypeToken<List<Map<String, Object>>>() {
        }.getType());
        assertEquals(1, docs.size());

        Map<String, Object> doc = docs.get(0);
        assertEquals("doc-1", doc.get("_id"));
        assertEquals("logs-index", doc.get("_index"));
        assertNotNull(doc.get("_score"));
        assertNotNull(doc.get("sort"));

        @SuppressWarnings("unchecked")
        Map<String, Object> returnedSource = (Map<String, Object>) doc.get("_source");
        assertEquals("2024-01-01", returnedSource.get("@timestamp"));
        assertEquals("INFO", returnedSource.get("level"));
        assertEquals("test log", returnedSource.get("message"));
    }
}
