/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.agent.tools.AbstractRetrieverTool.DOC_SIZE_FIELD;
import static org.opensearch.agent.tools.LogPatternTool.PATTERN;
import static org.opensearch.agent.tools.LogPatternTool.SAMPLE_LOG_SIZE;
import static org.opensearch.agent.tools.LogPatternTool.TOP_N_PATTERN;
import static org.opensearch.integTest.BaseAgentToolsIT.gson;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

public class LogPatternToolTests {

    public static String responseBodyResourceFile = "org/opensearch/agent/tools/expected_flow_agent_of_log_pattern_tool_response_body.json";
    public static final String TEST_QUERY_TEXT = "123fsd23134sdfouh";
    private Map<String, Object> params = new HashMap<>();
    private final Client client = mock(Client.class);
    @Mock
    private SearchResponse searchResponse;
    @Mock
    private SearchHits searchHits;

    @SneakyThrows
    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        LogPatternTool.Factory.getInstance().init(client, null);
        List<String> fields = List.of("field1", "field2", "field3");
        SearchHit[] hits = new SearchHit[]{
            createHit(0, null, fields, List.of("123", "123.abc-AB * De /", 12345)),
            createHit(1, null, fields, List.of("123", "45.abc-AB * De /", 12345)),
            createHit(2, null, fields, List.of("123", "12.abc_AB * De /", 12345)),
            createHit(3, null, fields, List.of("123", "45.ab_AB * De /", 12345)),
            createHit(4, null, fields, List.of("123", ".abAB * De /", 12345)),
        };
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(hits);
    }

    private SearchHit createHit(int docId, String id, List<String> fieldNames, List<Object> fieldContents) throws IOException {
        return new SearchHit(docId, id, null, null)
            .sourceRef(createSource(fieldNames, fieldContents));
    }

    private BytesReference createSource(List<String> fieldNames, List<Object> fieldContents) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (int i = 0; i < fieldNames.size(); i++) {
            builder.field(fieldNames.get(i), fieldContents.get(i));
        }
        builder.endObject();
        return (BytesReference.bytes(builder));
    }

    @Test
    @SneakyThrows
    public void testCreateTool() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        assertEquals(LogPatternTool.LOG_PATTERN_DEFAULT_DOC_SIZE, (int) tool.docSize);
        assertEquals(LogPatternTool.DEFAULT_TOP_N_PATTERN, tool.getTopNPattern());
        assertEquals(LogPatternTool.DEFAULT_SAMPLE_LOG_SIZE, tool.getSampleLogSize());
        assertNull(tool.getPattern());
        assertEquals("LogPatternTool", tool.getType());
        assertEquals("LogPatternTool", tool.getName());
        assertEquals(LogPatternTool.DEFAULT_DESCRIPTION, LogPatternTool.Factory.getInstance().getDefaultDescription());
        assertEquals(LogPatternTool.TYPE, LogPatternTool.Factory.getInstance().getDefaultType());
        assertNull(LogPatternTool.Factory.getInstance().getDefaultVersion());
    }

    @Test
    @SneakyThrows
    public void testCreateToolWithNonIntegerSize() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> LogPatternTool.Factory.getInstance().create(Map.of(DOC_SIZE_FIELD, "1.5")));
        MatcherAssert.assertThat(exception.getMessage(), containsString("Invalid value 1.5 for parameter doc_size, it should be a number"));

        Exception exception2 = assertThrows(IllegalArgumentException.class, () -> LogPatternTool.Factory.getInstance().create(Map.of(TOP_N_PATTERN, "1.5")));
        MatcherAssert.assertThat(exception2.getMessage(), containsString("Invalid value 1.5 for parameter top_n_pattern, it should be a number"));

        Exception exception3 = assertThrows(IllegalArgumentException.class, () -> LogPatternTool.Factory.getInstance().create(Map.of(SAMPLE_LOG_SIZE, "1.5")));
        MatcherAssert.assertThat(exception3.getMessage(), containsString("Invalid value 1.5 for parameter sample_log_size, it should be a number"));
    }

    @Test
    @SneakyThrows
    public void testCreateToolWithNonPositiveSize() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> LogPatternTool.Factory.getInstance().create(Map.of(DOC_SIZE_FIELD, "-1")));
        MatcherAssert.assertThat(exception.getMessage(), containsString("Invalid value -1 for parameter doc_size, it should be positive"));

        Exception exception2 = assertThrows(IllegalArgumentException.class, () -> LogPatternTool.Factory.getInstance().create(Map.of(TOP_N_PATTERN, "-1")));
        MatcherAssert.assertThat(exception2.getMessage(), containsString("Invalid value -1 for parameter top_n_pattern, it should be positive"));

        Exception exception3 = assertThrows(IllegalArgumentException.class, () -> LogPatternTool.Factory.getInstance().create(Map.of(SAMPLE_LOG_SIZE, "-1")));
        MatcherAssert.assertThat(exception3.getMessage(), containsString("Invalid value -1 for parameter sample_log_size, it should be positive"));
    }

    @Test
    public void testGetQueryBody() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        assertEquals(TEST_QUERY_TEXT, tool.getQueryBody(TEST_QUERY_TEXT));
    }

    @Test
    public void testValidate() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        assertTrue(tool.validate(Map.of("index", "test1", "input", "input_value")));

        // validate failure if no index
        assertFalse(tool.validate(Map.of("input", "input_value")));

        // validate failure if no
        assertFalse(tool.validate(Map.of("index", "test1")));
    }

    @Test
    public void testFindLongestField() {
        assertEquals("field2", LogPatternTool.findLongestField(Map.of("field1", "123", "field2", "1234", "filed3", 1234)));
    }

    @Test
    public void testExtractPattern() {
        assertEquals("././", LogPatternTool.extractPattern("123.abc/.AB/", null));
        assertEquals("123.c/.AB/", LogPatternTool.extractPattern("123.abc/.AB/", Pattern.compile("ab")));
        assertEquals(".abc/.AB/", LogPatternTool.extractPattern("123.abc/.AB/", Pattern.compile("[0-9]")));
    }

    @SneakyThrows
    @Test
    public void testExecutionDefault() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        JsonElement expected = gson.fromJson(Files.readString(
            Path.of(this.getClass().getClassLoader().getResource(responseBodyResourceFile).toURI())), JsonElement.class);
        tool.run(ImmutableMap.of("index", "index_name", "input", "{}"), ActionListener
            .<String>wrap(response -> assertEquals(expected, gson.fromJson(response, JsonElement.class)), e -> fail("Tool runs failed: " + e.getMessage())));
    }

    @SneakyThrows
    @Test
    public void testExecutionWithSpecifiedPatternField() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        JsonElement expected = gson.fromJson("[{\"total count\":5,\"sample logs\":[{\"field1\":\"123\",\"field3\":12345,\"field2\":\"123.abc-AB * De /\"},{\"field1\":\"123\",\"field3\":12345,\"field2\":\"45.abc-AB * De /\"}],\"pattern\":\"\"}]", JsonElement.class);
        tool.run(ImmutableMap.of("index", "index_name", "input", "{}", "pattern_field", "field1", "sample_log_size", "2"), ActionListener
            .<String>wrap(response -> assertEquals(expected, gson.fromJson(response, JsonElement.class)), e -> fail("Tool runs failed: " + e.getMessage())));
    }

    @SneakyThrows
    @Test
    public void testExecutionWithSpecifiedPattern() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(Map.of(PATTERN, "[a-zA-Z]"));
        JsonElement expected = gson.fromJson("[{\"pattern\":\"45.- *  /\",\"sample logs\":[{\"field1\":\"123\",\"field3\":12345,\"field2\":\"45.abc-AB * De /\"}],\"total count\":1},{\"pattern\":\". *  /\",\"sample logs\":[{\"field1\":\"123\",\"field3\":12345,\"field2\":\".abAB * De /\"}],\"total count\":1},{\"pattern\":\"123.- *  /\",\"sample logs\":[{\"field1\":\"123\",\"field3\":12345,\"field2\":\"123.abc-AB * De /\"}],\"total count\":1}]", JsonElement.class);
        tool.run(ImmutableMap.of("index", "index_name", "input", "{}"), ActionListener
            .<String>wrap(response -> assertEquals(expected, gson.fromJson(response, JsonElement.class)), e -> fail("Tool runs failed: " + e.getMessage())));
    }

    @SneakyThrows
    @Test
    public void testExecutionWithBlankInput() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        tool.run(ImmutableMap.of("index", "index_name", "input", ""), ActionListener
            .<String>wrap(response -> fail(), e -> MatcherAssert.assertThat(e.getMessage(), containsString("[input] is null or empty, can not process it."))));
    }

    @SneakyThrows
    @Test
    public void testExecutionFailedWithNonStringPatternFieldSpecified() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        tool.run(ImmutableMap.of("index", "index_name", "input", "{}", "pattern_field", "field3", "sample_log_size", "2"), ActionListener
            .<String>wrap(response -> fail(), e -> MatcherAssert.assertThat(e.getMessage(), containsString("Invalid parameter pattern_field: pattern field field3 in index index_name is not type of String"))));
    }


    @SneakyThrows
    @Test
    public void testExecutionFailedWithNoStringPatternField() {
        List<String> fields = List.of("field1", "field2", "field3");
        SearchHit[] hits = new SearchHit[]{
            createHit(0, null, fields, List.of(1, 123, 12345)),
        };
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        doAnswer(invocation -> {
                ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
                listener.onResponse(searchResponse);
                return null;
        }).when(client).search(any(), any());
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(hits);

        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        tool.run(ImmutableMap.of("index", "index_name", "input", "{}"), ActionListener
            .<String>wrap(response -> fail(), e -> MatcherAssert.assertThat(e.getMessage(), containsString("Pattern field is not set and this index doesn't contain any string field"))));
    }

    @SneakyThrows
    @Test
    public void testExecutionFailedWithNonExistPatternFieldSpecified() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        tool.run(ImmutableMap.of("index", "index_name", "input", "{}", "pattern_field", "field4", "sample_log_size", "2"), ActionListener
            .<String>wrap(response -> fail(), e -> MatcherAssert.assertThat(e.getMessage(), containsString("Invalid parameter pattern_field: index index_name does not have a field named field4"))));
    }

    @SneakyThrows
    @Test
    public void testExecutionWithNoHits() {
        SearchHit[] hits = new SearchHit[]{};
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getHits()).thenReturn(hits);

        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        tool.run(ImmutableMap.of("index", "index_name", "input", "{}"), ActionListener
            .<String>wrap(response -> assertEquals("Can not get any match from search result.", response), e -> fail("Tool runs failed: " + e.getMessage())));
    }

    @SneakyThrows
    @Test
    public void testExecutionFailedInSearch() {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[1];
            listener.onFailure(new Exception("Failed in Search"));
            return null;
        }).when(client).search(any(), any());

        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        tool.run(ImmutableMap.of("index", "index_name", "input", "{}"), ActionListener
            .<String>wrap(response -> fail(), e -> assertEquals("Failed in Search", e.getMessage())));
    }
}
