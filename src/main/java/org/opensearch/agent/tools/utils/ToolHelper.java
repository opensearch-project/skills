/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import java.util.Map;

public class ToolHelper {
    /**
     * Flatten all the fields in the mappings, insert the field->field type mapping to a map
     * @param mappingSource the mappings of an index
     * @param fieldsToType the result containing the field->field type mapping
     * @param prefix the parent field path
     */
    public static void extractFieldNamesTypes(Map<String, Object> mappingSource, Map<String, String> fieldsToType, String prefix) {
        if (prefix.length() > 0) {
            prefix += ".";
        }

        for (Map.Entry<String, Object> entry : mappingSource.entrySet()) {
            String n = entry.getKey();
            Object v = entry.getValue();

            if (v instanceof Map) {
                Map<String, Object> vMap = (Map<String, Object>) v;
                if (vMap.containsKey("type")) {
                    if (!((vMap.getOrDefault("type", "")).equals("alias"))) {
                        fieldsToType.put(prefix + n, (String) vMap.get("type"));
                    }
                }
                if (vMap.containsKey("properties")) {
                    extractFieldNamesTypes((Map<String, Object>) vMap.get("properties"), fieldsToType, prefix + n);
                }
                if (vMap.containsKey("fields")) {
                    extractFieldNamesTypes((Map<String, Object>) vMap.get("fields"), fieldsToType, prefix + n);
                }
            }
        }
    }
}
