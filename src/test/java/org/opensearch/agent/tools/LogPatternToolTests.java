/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import lombok.SneakyThrows;

public class LogPatternToolTests {

    public static final String TEST_QUERY_TEXT = "123fsd23134sdfouh";
    private Map<String, Object> params = new HashMap<>();

    @Before
    public void setup() {}

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
    }

    @Test
    public void testGetQueryBody() {
        LogPatternTool tool = LogPatternTool.Factory.getInstance().create(params);
        assertEquals(TEST_QUERY_TEXT, tool.getQueryBody(TEST_QUERY_TEXT));
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
}
