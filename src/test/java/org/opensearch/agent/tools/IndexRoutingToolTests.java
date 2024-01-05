/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import org.apache.commons.text.similarity.JaccardSimilarity;
import org.junit.Test;

import lombok.SneakyThrows;

public class IndexRoutingToolTests {
    @SneakyThrows
    @Test
    public void testTool() {
        JaccardSimilarity similarity = new JaccardSimilarity();
        Double abcd = similarity.apply("abcd", "abc");
        System.out.println(abcd);
    }
}
