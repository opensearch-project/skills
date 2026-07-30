/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils.mergeMetaData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class MergeRuleHelperTests {

    @Test
    public void testMerge_nullSource_doesNotThrow() {
        Map<String, Object> target = new HashMap<>();
        target.put("field1", Map.of("type", "text"));

        MergeRuleHelper.merge(null, target);

        // Target remains unchanged
        assertEquals(1, target.size());
        assertEquals(Map.of("type", "text"), target.get("field1"));
    }

    @Test
    public void testMerge_emptySource_doesNotThrow() {
        Map<String, Object> target = new HashMap<>();
        target.put("field1", Map.of("type", "text"));

        MergeRuleHelper.merge(new HashMap<>(), target);

        // Target remains unchanged
        assertEquals(1, target.size());
        assertEquals(Map.of("type", "text"), target.get("field1"));
    }

    @Test
    public void testMerge_emptySource_emptyTarget() {
        Map<String, Object> target = new HashMap<>();

        MergeRuleHelper.merge(new HashMap<>(), target);

        assertTrue(target.isEmpty());
    }

    @Test
    public void testMerge_nullSource_emptyTarget() {
        Map<String, Object> target = new HashMap<>();

        MergeRuleHelper.merge(null, target);

        assertTrue(target.isEmpty());
    }

    @Test
    public void testMerge_validSource_mergesIntoTarget() {
        Map<String, Object> source = new HashMap<>();
        source.put("field1", Map.of("type", "text"));

        Map<String, Object> target = new HashMap<>();

        MergeRuleHelper.merge(source, target);

        assertEquals(1, target.size());
        assertTrue(target.containsKey("field1"));
    }
}
