/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.text.StringEscapeUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.core.action.ActionListener;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import com.google.gson.Gson;

/**
 * this is a test file to test PainlessTool with junit
 */
public class PainlessScriptToolTests {
    @Mock
    private ScriptService scriptService;
    @Mock
    private TemplateScript templateScript;
    @Mock
    private ActionListener<String> actionListener;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        TemplateScript.Factory factory = new TemplateScript.Factory() {
            @Override
            public TemplateScript newInstance(Map<String, Object> params) {
                return templateScript;
            }
        };

        when(scriptService.compile(any(), any())).thenReturn(factory);

        PainlessScriptTool.Factory.getInstance().init(scriptService);
    }

    @Test
    public void testRun() {
        String script = "return 'Hello World';";
        PainlessScriptTool tool = PainlessScriptTool.Factory.getInstance().create(Map.of("script", script));
        when(templateScript.execute()).thenReturn("hello");
        tool.run(Map.of(), actionListener);

        verify(templateScript).execute();
        verify(scriptService).compile(any(), any());
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);
        verify(actionListener, times(1)).onResponse(responseCaptor.capture());
        assertEquals("hello", responseCaptor.getValue());
    }

    // test run wit exception
    @Test
    public void testRun_with_exception() {
        String script = "return 'Hello World';";
        PainlessScriptTool tool = PainlessScriptTool.Factory.getInstance().create(Map.of("script", script));
        when(templateScript.execute()).thenThrow(new RuntimeException("error"));
        tool.run(Map.of(), actionListener);

        verify(templateScript).execute();
        verify(scriptService).compile(any(), any());
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(actionListener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("error", exceptionCaptor.getValue().getMessage());
    }

    // test factory create
    @Test
    public void testFactory_create() {
        String script = "return 'Hello World';";
        PainlessScriptTool tool = PainlessScriptTool.Factory.getInstance().create(Map.of("script", script));
        assertEquals(PainlessScriptTool.TYPE, tool.getType());
        assertEquals("PainlessTool", tool.getName());
        assertEquals("Use this tool to execute painless script", tool.getDescription());
    }

    // test factory create with exception
    @Test(expected = IllegalArgumentException.class)
    public void testFactory_create_with_exception() {
        PainlessScriptTool.Factory.getInstance().create(Map.of());
    }

    // test flattenMap
    @Test
    public void testFlattenMap_without_prefix() {
        String script = "return 'Hello World';";
        PainlessScriptTool tool = PainlessScriptTool.Factory.getInstance().create(Map.of("script", script));
        Map<String, Object> map = Map.of("a", Map.of("b", "c"), "k", "v");
        Map<String, Object> resultMap = new HashMap<>();
        tool.flattenMap(map, resultMap, "");
        assertEquals(Map.of("a.b", "c", "k", "v"), resultMap);
    }

    // with prefix
    @Test
    public void testFlattenMap_with_prefix() {
        String script = "return 'Hello World';";
        PainlessScriptTool tool = PainlessScriptTool.Factory.getInstance().create(Map.of("script", script));
        Map<String, Object> map = Map.of("a", Map.of("b", "c"), "k", "v");
        Map<String, Object> resultMap = new HashMap<>();
        tool.flattenMap(map, resultMap, "prefix");
        assertEquals(Map.of("prefix.a.b", "c", "prefix.k", "v"), resultMap);
    }

    // nest map with depth 3
    @Test
    public void testFlattenMap_with_depth_3() {
        String script = "return 'Hello World';";
        PainlessScriptTool tool = PainlessScriptTool.Factory.getInstance().create(Map.of("script", script));
        Map<String, Object> map = Map.of("a", Map.of("b", Map.of("c", "d"), "k", "v"));
        Gson gson = new Gson();
        System.out.println(StringEscapeUtils.escapeJson(gson.toJson(map)));
        Map<String, Object> resultMap = new HashMap<>();
        tool.flattenMap(map, resultMap, "");
        assertEquals(Map.of("a.b.c", "d", "a.k", "v"), resultMap);
    }

    // test getFlattenedParameters
    @Test
    public void testGetFlattenedParameters() {
        String script = "return 'Hello World';";
        PainlessScriptTool tool = PainlessScriptTool.Factory.getInstance().create(Map.of("script", script));
        Map<String, String> map = Map.of("k", "{\\\"a\\\":{\\\"k\\\":\\\"v\\\",\\\"b\\\":{\\\"c\\\":\\\"d\\\"}}}");
        Map<String, Object> resultMap = tool.getFlattenedParameters(map);
        assertEquals(
            Map.of("k.a.b.c", "d", "k.a.k", "v", "k", "{\\\"a\\\":{\\\"k\\\":\\\"v\\\",\\\"b\\\":{\\\"c\\\":\\\"d\\\"}}}"),
            resultMap
        );
    }
}
