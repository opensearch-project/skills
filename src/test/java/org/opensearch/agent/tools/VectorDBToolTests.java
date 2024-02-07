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

public class VectorDBToolTests {
    public static final String TEST_QUERY_TEXT = "123fsd23134sdfouh";
    public static final String TEST_EMBEDDING_FIELD = "test embedding";
    public static final String TEST_MODEL_ID = "123fsd23134";
    public static final Integer TEST_K = 123;
    private Map<String, Object> params = new HashMap<>();

    @Before
    public void setup() {
        params.put(VectorDBTool.INDEX_FIELD, AbstractRetrieverToolTests.TEST_INDEX);
        params.put(VectorDBTool.EMBEDDING_FIELD, TEST_EMBEDDING_FIELD);
        params.put(VectorDBTool.SOURCE_FIELD, gson.toJson(AbstractRetrieverToolTests.TEST_SOURCE_FIELDS));
        params.put(VectorDBTool.MODEL_ID_FIELD, TEST_MODEL_ID);
        params.put(VectorDBTool.DOC_SIZE_FIELD, AbstractRetrieverToolTests.TEST_DOC_SIZE.toString());
        params.put(VectorDBTool.K_FIELD, TEST_K.toString());
    }

    @Test
    @SneakyThrows
    public void testCreateTool() {
        VectorDBTool tool = VectorDBTool.Factory.getInstance().create(params);
        assertEquals(AbstractRetrieverToolTests.TEST_INDEX, tool.getIndex());
        assertEquals(TEST_EMBEDDING_FIELD, tool.getEmbeddingField());
        assertEquals(AbstractRetrieverToolTests.TEST_SOURCE_FIELDS, tool.getSourceFields());
        assertEquals(TEST_MODEL_ID, tool.getModelId());
        assertEquals(AbstractRetrieverToolTests.TEST_DOC_SIZE, tool.getDocSize());
        assertEquals(TEST_K, tool.getK());
        assertEquals("VectorDBTool", tool.getType());
        assertEquals("VectorDBTool", tool.getName());
        assertEquals(VectorDBTool.DEFAULT_DESCRIPTION, VectorDBTool.Factory.getInstance().getDefaultDescription());
    }

    @Test
    @SneakyThrows
    public void testGetQueryBody() {
        VectorDBTool tool = VectorDBTool.Factory.getInstance().create(params);
        Map<String, Map<String, Map<String, Map<String, String>>>> queryBody = gson.fromJson(tool.getQueryBody(TEST_QUERY_TEXT), Map.class);
        assertEquals("123fsd23134sdfouh", queryBody.get("query").get("neural").get("test embedding").get("query_text"));
        assertEquals("123fsd23134", queryBody.get("query").get("neural").get("test embedding").get("model_id"));
        assertEquals(123.0, queryBody.get("query").get("neural").get("test embedding").get("k"));
    }

    @Test
    @SneakyThrows
    public void testGetQueryBodyWithJsonObjectString() {
        VectorDBTool tool = VectorDBTool.Factory.getInstance().create(params);
        String jsonInput = gson.toJson(Map.of("hi", "a"));
        Map<String, Map<String, Map<String, Map<String, String>>>> queryBody = gson.fromJson(tool.getQueryBody(jsonInput), Map.class);
        assertEquals("{\"hi\":\"a\"}", queryBody.get("query").get("neural").get("test embedding").get("query_text"));
        assertEquals("123fsd23134", queryBody.get("query").get("neural").get("test embedding").get("model_id"));
        assertEquals(123.0, queryBody.get("query").get("neural").get("test embedding").get("k"));
    }

    @Test
    @SneakyThrows
    public void testGetQueryBodyWithIllegalParams() {
        Map<String, Object> illegalParams1 = new HashMap<>(params);
        illegalParams1.remove(VectorDBTool.MODEL_ID_FIELD);
        VectorDBTool tool1 = VectorDBTool.Factory.getInstance().create(illegalParams1);
        Exception exception1 = assertThrows(
            IllegalArgumentException.class,
            () -> tool1.getQueryBody(AbstractRetrieverToolTests.TEST_QUERY)
        );
        assertEquals("Parameter [embedding_field] and [model_id] can not be null or empty.", exception1.getMessage());

        Map<String, Object> illegalParams2 = new HashMap<>(params);
        illegalParams2.remove(VectorDBTool.EMBEDDING_FIELD);
        VectorDBTool tool2 = VectorDBTool.Factory.getInstance().create(illegalParams2);
        Exception exception2 = assertThrows(
            IllegalArgumentException.class,
            () -> tool2.getQueryBody(AbstractRetrieverToolTests.TEST_QUERY)
        );
        assertEquals("Parameter [embedding_field] and [model_id] can not be null or empty.", exception2.getMessage());
    }

    @Test
    @SneakyThrows
    public void testCreateToolsParseParams() {
        assertThrows(ClassCastException.class, () -> VectorDBTool.Factory.getInstance().create(Map.of(VectorDBTool.INDEX_FIELD, 123)));

        assertThrows(ClassCastException.class, () -> VectorDBTool.Factory.getInstance().create(Map.of(VectorDBTool.EMBEDDING_FIELD, 123)));

        assertThrows(ClassCastException.class, () -> VectorDBTool.Factory.getInstance().create(Map.of(VectorDBTool.MODEL_ID_FIELD, 123)));

        assertThrows(JsonSyntaxException.class, () -> VectorDBTool.Factory.getInstance().create(Map.of(VectorDBTool.SOURCE_FIELD, "123")));

        // although it will be parsed as integer, but the parameters value should always be String
        assertThrows(ClassCastException.class, () -> VectorDBTool.Factory.getInstance().create(Map.of(VectorDBTool.DOC_SIZE_FIELD, 123)));

        assertThrows(ClassCastException.class, () -> VectorDBTool.Factory.getInstance().create(Map.of(VectorDBTool.K_FIELD, 123)));
    }
}
