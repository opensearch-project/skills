/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import static org.opensearch.agent.tools.utils.ToolHelper.getPPLTransportActionListener;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.json.JSONObject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;

import lombok.extern.log4j.Log4j2;

/**
 * Utility class for executing PPL queries and parsing results
 */
@Log4j2
public class PPLExecuteHelper {

    /**
     * Executes PPL query and parses the result using provided result parser
     *
     * @param <T> The parsed result type
     * @param client OpenSearch client
     * @param ppl PPL query string to execute
     * @param resultParser Function to parse PPL result into desired format
     * @param listener Action listener for handling parsed results or failures
     */
    public static <T> void executePPLAndParseResult(
        Client client,
        String ppl,
        Function<Map<String, Object>, T> resultParser,
        ActionListener<T> listener
    ) {
        try {
            JSONObject jsonContent = new JSONObject(ImmutableMap.of("query", ppl));
            PPLQueryRequest pplQueryRequest = new PPLQueryRequest(ppl, jsonContent, null, "jdbc");
            TransportPPLQueryRequest transportPPLQueryRequest = new TransportPPLQueryRequest(pplQueryRequest);

            client
                .execute(
                    PPLQueryAction.INSTANCE,
                    transportPPLQueryRequest,
                    getPPLTransportActionListener(ActionListener.wrap(transportPPLQueryResponse -> {
                        String result = transportPPLQueryResponse.getResult();
                        if (Strings.isEmpty(result)) {
                            listener.onFailure(new RuntimeException("Empty PPL response"));
                        } else {
                            Map<String, Object> pplResult = gson.fromJson(result, new TypeToken<Map<String, Object>>() {
                            }.getType());
                            if (pplResult.containsKey("error")) {
                                Object errorObj = pplResult.get("error");
                                String errorDetail;
                                if (errorObj instanceof Map) {
                                    Map<?, ?> errorMap = (Map<?, ?>) errorObj;
                                    Object reason = errorMap.get("reason");
                                    errorDetail = reason != null ? reason.toString() : errorMap.toString();
                                } else {
                                    errorDetail = errorObj != null ? errorObj.toString() : "Unknown error";
                                }
                                throw new RuntimeException("PPL query error: " + errorDetail);
                            }

                            Object datarowsObj = pplResult.get("datarows");
                            if (!(datarowsObj instanceof List)) {
                                throw new IllegalStateException("Invalid PPL response format: missing or invalid datarows");
                            }

                            listener.onResponse(resultParser.apply(pplResult));
                        }
                    }, error -> {
                        log.error("PPL execution failed: {}", error.getMessage());
                        listener.onFailure(new RuntimeException("PPL execution failed: " + error.getMessage(), error));
                    }))
                );
        } catch (Exception e) {
            String errorMessage = String.format(Locale.ROOT, "Failed to execute PPL query: %s", e.getMessage());
            log.error(errorMessage, e);
            listener.onFailure(new RuntimeException(errorMessage, e));
        }
    }

    /**
     * Helper method to create a result parser that extracts datarows
     */
    public static <T> Function<Map<String, Object>, T> dataRowsParser(Function<List<List<Object>>, T> rowParser) {
        return pplResult -> {
            Object datarowsObj = pplResult.get("datarows");
            @SuppressWarnings("unchecked")
            List<List<Object>> dataRows = (List<List<Object>>) datarowsObj;
            if (dataRows.isEmpty()) {
                log.warn("PPL query returned no data rows for the specified criteria");
            }
            return rowParser.apply(dataRows);
        };
    }
}
