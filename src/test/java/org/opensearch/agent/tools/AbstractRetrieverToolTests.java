/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.SearchModule;

import lombok.SneakyThrows;

public class AbstractRetrieverToolTests {
    static public final String TEST_QUERY = "{\"query\":{\"match_all\":{}}}";
    static public final String TEST_INDEX = "test index";
    static public final String[] TEST_SOURCE_FIELDS = new String[] { "test 1", "test 2" };
    static public final Integer TEST_DOC_SIZE = 3;
    static public final NamedXContentRegistry TEST_XCONTENT_REGISTRY_FOR_QUERY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );

    private String mockedSearchResponseString;
    private String mockedEmptySearchResponseString;
    private AbstractRetrieverTool mockedImpl;

    @Before
    @SneakyThrows
    public void setup() {
        try (InputStream searchResponseIns = AbstractRetrieverTool.class.getResourceAsStream("retrieval_tool_search_response.json")) {
            if (searchResponseIns != null) {
                mockedSearchResponseString = new String(searchResponseIns.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        try (InputStream searchResponseIns = AbstractRetrieverTool.class.getResourceAsStream("retrieval_tool_empty_search_response.json")) {
            if (searchResponseIns != null) {
                mockedEmptySearchResponseString = new String(searchResponseIns.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        mockedImpl = Mockito
            .mock(
                AbstractRetrieverTool.class,
                Mockito
                    .withSettings()
                    .useConstructor(null, TEST_XCONTENT_REGISTRY_FOR_QUERY, TEST_INDEX, TEST_SOURCE_FIELDS, TEST_DOC_SIZE)
                    .defaultAnswer(Mockito.CALLS_REAL_METHODS)
            );
        when(mockedImpl.getQueryBody(any(String.class))).thenReturn(TEST_QUERY);
    }

    @Test
    @SneakyThrows
    public void testRunAsyncWithSearchResults() {
        Client client = mock(Client.class);
        SearchResponse mockedSearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, mockedSearchResponseString)
            );
        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedSearchResponse);
            return null;
        }).when(client).search(any(), any());
        mockedImpl.setClient(client);

        final CompletableFuture<String> future = new CompletableFuture<>();
        ActionListener<String> listener = ActionListener.wrap(r -> { future.complete(r); }, e -> { future.completeExceptionally(e); });

        mockedImpl.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hello world"), listener);

        future.join();
        assertEquals(
            "{\"_index\":\"hybrid-index\",\"_source\":{\"passage_text\":\"Company test_mock have a history of 100 years.\"},\"_id\":\"1\",\"_score\":89.2917}\n"
                + "{\"_index\":\"hybrid-index\",\"_source\":{\"passage_text\":\"the price of the api is 2$ per invokation\"},\"_id\":\"2\",\"_score\":0.10702579}\n",
            future.get()
        );
    }

    @Test
    @SneakyThrows
    public void testRunAsyncWithEmptySearchResponse() {
        Client client = mock(Client.class);
        SearchResponse mockedEmptySearchResponse = SearchResponse
            .fromXContent(
                JsonXContent.jsonXContent
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, mockedEmptySearchResponseString)
            );
        doAnswer(invocation -> {
            SearchRequest searchRequest = invocation.getArgument(0);
            assertEquals((long) TEST_DOC_SIZE, (long) searchRequest.source().size());
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockedEmptySearchResponse);
            return null;
        }).when(client).search(any(), any());
        mockedImpl.setClient(client);

        final CompletableFuture<String> future = new CompletableFuture<>();
        ActionListener<String> listener = ActionListener.wrap(r -> { future.complete(r); }, e -> { future.completeExceptionally(e); });

        mockedImpl.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hello world"), listener);

        future.join();
        assertEquals("Can not get any match from search result.", future.get());
    }

    @Test
    @SneakyThrows
    public void testRunAsyncWithIllegalQueryThenListenerOnFailure() {
        Client client = mock(Client.class);
        mockedImpl.setClient(client);

        final CompletableFuture<String> future1 = new CompletableFuture<>();
        ActionListener<String> listener1 = ActionListener.wrap(future1::complete, future1::completeExceptionally);
        mockedImpl.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, ""), listener1);

        Exception exception1 = assertThrows(Exception.class, future1::join);
        assertTrue(exception1.getCause() instanceof IllegalArgumentException);
        assertEquals(exception1.getCause().getMessage(), "[input] is null or empty, can not process it.");

        final CompletableFuture<String> future2 = new CompletableFuture<>();
        ActionListener<String> listener2 = ActionListener.wrap(future2::complete, future2::completeExceptionally);
        mockedImpl.run(Map.of(AbstractRetrieverTool.INPUT_FIELD, "  "), listener2);

        Exception exception2 = assertThrows(Exception.class, future2::join);
        assertTrue(exception2.getCause() instanceof IllegalArgumentException);
        assertEquals(exception2.getCause().getMessage(), "[input] is null or empty, can not process it.");

        final CompletableFuture<String> future3 = new CompletableFuture<>();
        ActionListener<String> listener3 = ActionListener.wrap(future3::complete, future3::completeExceptionally);
        mockedImpl.run(Map.of("test", "hello world"), listener3);

        Exception exception3 = assertThrows(Exception.class, future3::join);
        assertTrue(exception3.getCause() instanceof IllegalArgumentException);
        assertEquals(exception3.getCause().getMessage(), "[input] is null or empty, can not process it.");

        final CompletableFuture<String> future4 = new CompletableFuture<>();
        ActionListener<String> listener4 = ActionListener.wrap(future4::complete, future4::completeExceptionally);
        mockedImpl.run(null, listener4);

        Exception exception4 = assertThrows(Exception.class, future4::join);
        assertTrue(exception4.getCause() instanceof NullPointerException);
    }

    @Test
    @SneakyThrows
    public void testValidate() {
        assertTrue(mockedImpl.validate(Map.of(AbstractRetrieverTool.INPUT_FIELD, "hi")));
        assertFalse(mockedImpl.validate(Map.of(AbstractRetrieverTool.INPUT_FIELD, "")));
        assertFalse(mockedImpl.validate(Map.of(AbstractRetrieverTool.INPUT_FIELD, " ")));
        assertFalse(mockedImpl.validate(Map.of("test", " ")));
        assertFalse(mockedImpl.validate(new HashMap<>()));
        assertFalse(mockedImpl.validate(null));
    }
}
