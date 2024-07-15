/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonSyntaxException;

import lombok.SneakyThrows;

public class NeuralSparseSearchToolTests {
    public static final String TEST_QUERY_TEXT = "123fsd23134sdfouh";
    public static final String TEST_EMBEDDING_FIELD = "test embedding";
    public static final String TEST_MODEL_ID = "123fsd23134";
    public static final String TEST_NESTED_PATH = "nested_path";
    private Map<String, Object> params = new HashMap<>();

    @Before
    public void setup() {
        params.put(NeuralSparseSearchTool.INDEX_FIELD, AbstractRetrieverToolTests.TEST_INDEX);
        params.put(NeuralSparseSearchTool.EMBEDDING_FIELD, TEST_EMBEDDING_FIELD);
        params.put(NeuralSparseSearchTool.SOURCE_FIELD, gson.toJson(AbstractRetrieverToolTests.TEST_SOURCE_FIELDS));
        params.put(NeuralSparseSearchTool.MODEL_ID_FIELD, TEST_MODEL_ID);
        params.put(NeuralSparseSearchTool.DOC_SIZE_FIELD, AbstractRetrieverToolTests.TEST_DOC_SIZE.toString());
    }

    @Test
    @SneakyThrows
    public void testCreateTool() {
        NeuralSparseSearchTool tool = NeuralSparseSearchTool.Factory.getInstance().create(params);
        assertEquals(AbstractRetrieverToolTests.TEST_INDEX, tool.getIndex());
        assertEquals(TEST_EMBEDDING_FIELD, tool.getEmbeddingField());
        assertEquals(AbstractRetrieverToolTests.TEST_SOURCE_FIELDS, tool.getSourceFields());
        assertEquals(TEST_MODEL_ID, tool.getModelId());
        assertEquals(AbstractRetrieverToolTests.TEST_DOC_SIZE, tool.getDocSize());
        assertEquals("NeuralSparseSearchTool", tool.getType());
        assertEquals("NeuralSparseSearchTool", tool.getName());
        assertEquals(
            "Use this tool to search data in OpenSearch index.",
            NeuralSparseSearchTool.Factory.getInstance().getDefaultDescription()
        );
    }

    @Test
    @SneakyThrows
    public void testGetQueryBody() {
        NeuralSparseSearchTool tool = NeuralSparseSearchTool.Factory.getInstance().create(params);
        Map<String, Map<String, Map<String, Map<String, String>>>> queryBody = gson.fromJson(tool.getQueryBody(TEST_QUERY_TEXT), Map.class);
        assertEquals("123fsd23134sdfouh", queryBody.get("query").get("neural_sparse").get("test embedding").get("query_text"));
        assertEquals("123fsd23134", queryBody.get("query").get("neural_sparse").get("test embedding").get("model_id"));
    }

    @Test
    @SneakyThrows
    public void testGetQueryBodyWithNestedPath() {
        params.put(NeuralSparseSearchTool.NESTED_PATH_FIELD, TEST_NESTED_PATH);
        NeuralSparseSearchTool tool = NeuralSparseSearchTool.Factory.getInstance().create(params);
        Map<String, Map<String, Map<String, Object>>> nestedQueryBody = gson.fromJson(tool.getQueryBody(TEST_QUERY_TEXT), Map.class);
        assertEquals("nested_path", nestedQueryBody.get("query").get("nested").get("path"));
        assertEquals("max", nestedQueryBody.get("query").get("nested").get("score_mode"));
        Map<String, Map<String, Map<String, String>>> queryBody = (Map<String, Map<String, Map<String, String>>>) nestedQueryBody
            .get("query")
            .get("nested")
            .get("query");
        assertEquals("123fsd23134sdfouh", queryBody.get("neural_sparse").get("test embedding").get("query_text"));
        assertEquals("123fsd23134", queryBody.get("neural_sparse").get("test embedding").get("model_id"));
    }

    @Test
    @SneakyThrows
    public void testGetQueryBodyWithJsonObjectString() {
        NeuralSparseSearchTool tool = NeuralSparseSearchTool.Factory.getInstance().create(params);
        String jsonInput = gson.toJson(Map.of("hi", "a"));
        Map<String, Map<String, Map<String, Map<String, String>>>> queryBody = gson.fromJson(tool.getQueryBody(jsonInput), Map.class);
        assertEquals("{\"hi\":\"a\"}", queryBody.get("query").get("neural_sparse").get("test embedding").get("query_text"));
        assertEquals("123fsd23134", queryBody.get("query").get("neural_sparse").get("test embedding").get("model_id"));
    }

    @Test
    @SneakyThrows
    public void testGetQueryBodyWithIllegalParams() {
        Map<String, Object> illegalParams1 = new HashMap<>(params);
        illegalParams1.remove(NeuralSparseSearchTool.MODEL_ID_FIELD);
        NeuralSparseSearchTool tool1 = NeuralSparseSearchTool.Factory.getInstance().create(illegalParams1);
        Exception exception1 = assertThrows(
            IllegalArgumentException.class,
            () -> tool1.getQueryBody(AbstractRetrieverToolTests.TEST_QUERY)
        );
        assertEquals("Parameter [embedding_field] and [model_id] can not be null or empty.", exception1.getMessage());

        Map<String, Object> illegalParams2 = new HashMap<>(params);
        illegalParams2.remove(NeuralSparseSearchTool.EMBEDDING_FIELD);
        NeuralSparseSearchTool tool2 = NeuralSparseSearchTool.Factory.getInstance().create(illegalParams2);
        Exception exception2 = assertThrows(
            IllegalArgumentException.class,
            () -> tool2.getQueryBody(AbstractRetrieverToolTests.TEST_QUERY)
        );
        assertEquals("Parameter [embedding_field] and [model_id] can not be null or empty.", exception2.getMessage());
    }

    @Test
    @SneakyThrows
    public void testCreateToolsParseParams() {
        assertThrows(
            ClassCastException.class,
            () -> NeuralSparseSearchTool.Factory.getInstance().create(Map.of(NeuralSparseSearchTool.INDEX_FIELD, 123))
        );

        assertThrows(
            ClassCastException.class,
            () -> NeuralSparseSearchTool.Factory.getInstance().create(Map.of(NeuralSparseSearchTool.EMBEDDING_FIELD, 123))
        );

        assertThrows(
            ClassCastException.class,
            () -> NeuralSparseSearchTool.Factory.getInstance().create(Map.of(NeuralSparseSearchTool.MODEL_ID_FIELD, 123))
        );

        assertThrows(
            ClassCastException.class,
            () -> NeuralSparseSearchTool.Factory.getInstance().create(Map.of(NeuralSparseSearchTool.NESTED_PATH_FIELD, 123))
        );

        assertThrows(
            JsonSyntaxException.class,
            () -> NeuralSparseSearchTool.Factory.getInstance().create(Map.of(NeuralSparseSearchTool.SOURCE_FIELD, "123"))
        );

        // although it will be parsed as integer, but the parameters value should always be String
        assertThrows(
            ClassCastException.class,
            () -> NeuralSparseSearchTool.Factory.getInstance().create(Map.of(NeuralSparseSearchTool.DOC_SIZE_FIELD, 123))
        );
    }
}
