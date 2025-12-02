/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;

/**
 * Helper utilities for anomaly detector tools
 */
public class AnomalyDetectorToolHelper {

    /**
     * Create an aggregation builder based on the aggregation method
     * @param method aggregation method (avg, sum, min, max, count)
     * @param field field name to aggregate on
     * @return aggregation builder
     */
    public static AggregationBuilder createAggregationBuilder(String method, String field) {
        return switch (method.toLowerCase(Locale.ROOT)) {
            case "avg" -> AggregationBuilders.avg(field).field(field);
            case "sum" -> AggregationBuilders.sum(field).field(field);
            case "min" -> AggregationBuilders.min(field).field(field);
            case "max" -> AggregationBuilders.max(field).field(field);
            case "count" -> AggregationBuilders.count(field).field(field);
            default -> throw new IllegalArgumentException("Unsupported aggregation method: " + method);
        };
    }

    /**
     * Extract list of indices from tool parameters
     * @param parameters tool parameters containing "input" with JSON array of indices
     * @return list of index names
     */
    public static List<String> extractIndicesList(Map<String, String> parameters) {
        String inputStr = parameters.get("input");
        if (inputStr == null || inputStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Input parameter is required");
        }

        try {
            Map<String, Object> input = StringUtils.gson.fromJson(inputStr, Map.class);
            List<String> indices = (List<String>) input.get("indices");

            if (indices == null || indices.isEmpty()) {
                throw new IllegalArgumentException("No indices provided");
            }

            for (String index : indices) {
                if (index.startsWith(".")) {
                    throw new IllegalArgumentException("System indices not supported: " + index);
                }
            }

            return indices;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse indices: " + e.getMessage());
        }
    }
}
