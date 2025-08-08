/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;

import com.google.gson.reflect.TypeToken;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class ToolHelper {
    /**
     * Load prompt from the resource file of the invoking class
     * @param source class which calls this function
     * @param fileName the resource file name of prompt
     * @return the LLM request prompt template.
     */
    public static Map<String, String> loadDefaultPromptDictFromFile(Class<?> source, String fileName) {
        try (InputStream searchResponseIns = source.getResourceAsStream(fileName)) {
            if (searchResponseIns != null) {
                String defaultPromptContent = new String(searchResponseIns.readAllBytes(), StandardCharsets.UTF_8);
                return StringUtils.gson.fromJson(defaultPromptContent, Map.class);
            }
        } catch (IOException e) {
            log.error("Failed to load default prompt dict from file: {}", fileName, e);
        }
        return new HashMap<>();
    }

    /**
     * Flatten all the fields in the mappings, insert the field to fieldType mapping to a map
     * @param mappingSource the mappings of an index
     * @param fieldsToType the result containing the field to fieldType mapping
     * @param prefix the parent field path
     * @param includeFields whether include the `fields` in a text type field, for some use case like PPLTool, `fields` in a text type field
     *                      cannot be included, but for CreateAnomalyDetectorTool, `fields` must be included.
     */
    public static void extractFieldNamesTypes(
        Map<String, Object> mappingSource,
        Map<String, String> fieldsToType,
        String prefix,
        boolean includeFields
    ) {
        if (prefix.length() > 0) {
            prefix += ".";
        }

        for (Map.Entry<String, Object> entry : mappingSource.entrySet()) {
            String n = entry.getKey();
            Object v = entry.getValue();

            if (v instanceof Map) {
                Map<String, Object> vMap = (Map<String, Object>) v;
                if (vMap.containsKey("type")) {
                    String fieldType = (String) vMap.getOrDefault("type", "");
                    // no need to extract alias into the result, and for object field, extract the subfields only
                    if (!fieldType.equals("alias") && !fieldType.equals("object")) {
                        fieldsToType.put(prefix + n, (String) vMap.get("type"));
                    }
                }
                if (vMap.containsKey("properties")) {
                    extractFieldNamesTypes((Map<String, Object>) vMap.get("properties"), fieldsToType, prefix + n, includeFields);
                }
                if (includeFields && vMap.containsKey("fields")) {
                    extractFieldNamesTypes((Map<String, Object>) vMap.get("fields"), fieldsToType, prefix + n, true);
                }
            }
        }
    }

    /**
     * Wrapper to get PPL transport action listener
     * @param listener input action listener
     * @return wrapped action listener
     */
    public static <T extends ActionResponse> ActionListener<T> getPPLTransportActionListener(
        ActionListener<TransportPPLQueryResponse> listener
    ) {
        return ActionListener.wrap(r -> { listener.onResponse(TransportPPLQueryResponse.fromActionResponse(r)); }, listener::onFailure);
    }

    /**
     * Extracts required parameters from the input parameter map based on tool attributes.
     * If the attributes contain a list of required parameters, only those parameters are extracted.
     * Otherwise, all parameters are returned.
     *
     * @param parameters The input parameters map to extract from
     * @param attributes The tool attributes containing required parameter information
     * @return A map containing only the required parameters or all parameters if no required parameters are specified
     */
    public static Map<String, String> extractRequiredParameters(Map<String, String> parameters, Map<String, ?> attributes) {
        Map<String, String> extractedParameters = new HashMap<>();
        if (parameters == null) {
            return extractedParameters;
        }
        if (attributes != null && attributes.containsKey(ToolConstants.TOOL_REQUIRED_PARAMS)) {
            List<String> requiredParameters = StringUtils
                .parseStringArrayToList((String) attributes.get(ToolConstants.TOOL_REQUIRED_PARAMS));
            if (requiredParameters != null) {
                for (String requiredParameter : requiredParameters) {
                    extractedParameters.put(requiredParameter, parameters.get(requiredParameter));
                }
            }
        } else {
            extractedParameters.putAll(parameters);
        }
        return extractedParameters;
    }

    /**
     * Extracts all relevant parameters including required parameters and nested parameters from the 'input' field.
     * First extracts required parameters, then processes the 'input' field if present.
     * The 'input' field is expected to contain a JSON string that can be parsed into a map of additional parameters.
     * Variable substitution is performed on the 'input' field using the format ${parameters.key}.
     *
     * @param parameters The input parameters map to extract from
     * @param attributes The tool attributes containing parameter requirements
     * @return A map containing all extracted parameters including those from the 'input' field
     */
    public static Map<String, String> extractInputParameters(Map<String, String> parameters, Map<String, ?> attributes) {
        Map<String, String> extractedParameters = extractRequiredParameters(parameters, attributes);
        if (extractedParameters.containsKey("input")) {
            try {
                StringSubstitutor stringSubstitutor = new StringSubstitutor(parameters, "${parameters.", "}");
                String input = stringSubstitutor.replace(parameters.get("input"));
                extractedParameters.put("input", input);
                Map<String, String> inputParameters = gson
                    .fromJson(input, TypeToken.getParameterized(Map.class, String.class, String.class).getType());
                extractedParameters.putAll(inputParameters);
            } catch (Exception exception) {
                log.info("fail extract parameters from key 'input' due to{}", exception.getMessage());
            }
        }
        return extractedParameters;
    }
}
