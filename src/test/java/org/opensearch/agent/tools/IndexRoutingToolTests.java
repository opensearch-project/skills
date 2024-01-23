/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.opensearch.agent.indices.SkillsIndexEnum.SKILLS_INDEX_SUMMARY;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.agent.tools.utils.LLMProvider;

import lombok.SneakyThrows;

public class IndexRoutingToolTests {
    public static final String TEST_QUERY_TEXT = "test query text";
    public static final String TEST_MODEL_ID = "foo";
    public static final String TEST_INFERENCE_MODEL_ID = "bar";
    public static final Integer TEST_K = 123;
    private final Map<String, Object> params = new HashMap<>();

    @Before
    public void setup() {
        params.clear();
        params.put(IndexRoutingTool.EMBEDDING_MODEL_ID, TEST_MODEL_ID);
        params.put(IndexRoutingTool.INFERENCE_MODEL_ID, TEST_INFERENCE_MODEL_ID);
        params.put(IndexRoutingTool.LLM_PROVIDER, LLMProvider.ANTHROPIC.name());
    }

    @Test
    @SneakyThrows
    public void testCreateTool() {
        params.put(IndexRoutingTool.PROMPT_TEMPLATE, "test");
        IndexRoutingTool tool = IndexRoutingTool.Factory.getInstance().create(params);
        assertEquals(SKILLS_INDEX_SUMMARY.getIndexName(), tool.getIndex());
        assertEquals(TEST_MODEL_ID, tool.getModelId());
        assertEquals(TEST_INFERENCE_MODEL_ID, tool.getInferenceModelId());
        assertEquals("\n\nHuman: test \n\nAssistant:", tool.getPrompt());
        assertEquals(IndexRoutingTool.DEFAULT_K, tool.getDocSize().intValue());
        assertEquals(IndexRoutingTool.DEFAULT_K, tool.getK().intValue());
        assertEquals("IndexRoutingTool", tool.getType());
        assertEquals("IndexRoutingTool", tool.getName());
    }

    @Test
    @SneakyThrows
    public void testGetQueryBody() {
        IndexRoutingTool tool = IndexRoutingTool.Factory.getInstance().create(params);
        assertEquals(
            "{\"query\":{\"neural\":{\"index_summary_embedding_foo\":{\""
                + "query_text\":\"test query text\",\"model_id\":\"foo\",\"k\":5}}}}",
            tool.getQueryBody(TEST_QUERY_TEXT)
        );
    }

    @Test
    @SneakyThrows
    public void testInputParser() {
        IndexRoutingTool tool = IndexRoutingTool.Factory.getInstance().create(params);
        String input1 = (String) tool.inputParser.parse("abcd");
        Assert.assertEquals("abcd", input1);

        String input2 = (String) tool.inputParser.parse("{\"question\": \"foo\"}");
        Assert.assertEquals("foo", input2);

        String input3 = (String) tool.inputParser.parse("{\"foo\": \"bar\"}");
        Assert.assertEquals("{\"foo\": \"bar\"}", input3);

    }
}
