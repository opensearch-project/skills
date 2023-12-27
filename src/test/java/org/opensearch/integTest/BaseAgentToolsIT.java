/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.integTest;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Before;
import org.opensearch.client.*;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;

import java.util.List;
import java.util.Map;

public abstract class BaseAgentToolsIT extends OpenSearchSecureRestTestCase {
    private static final int MAX_TASK_RESULT_QUERY_TIME_IN_SECOND = 60 * 5;
    private static final int DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND = 1000;
    /**
     * Update cluster settings to run ml models
     */
    @Before
    public void updateClusterSettings() {
        updateClusterSettings("plugins.ml_commons.only_run_on_ml_node", false);
        // default threshold for native circuit breaker is 90, it may be not enough on test runner machine
        updateClusterSettings("plugins.ml_commons.native_memory_threshold", 100);
        updateClusterSettings("plugins.ml_commons.allow_registering_model_via_url", true);
    }

    @SneakyThrows
    protected void updateClusterSettings(String settingKey, Object value) {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("persistent")
                .field(settingKey, value)
                .endObject()
                .endObject();
        Response response = makeRequest(
                client(),
                "PUT",
                "_cluster/settings",
                null,
                builder.toString(),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    @SneakyThrows
    private Map parseResponseToMap(Response response) {
        Map<String, Object> responseInMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(response.getEntity()),
                false
        );
        response.getEntity().toString();
        return responseInMap;
    }

    @SneakyThrows
    private Object parseFieldFromResponse(Response response, String field) {
        assertNotNull(field);
        Map map = parseResponseToMap(response);
        Object result = map.get(field);
        assertNotNull(result);
        return result;
    }

    protected String registerModel(String requestBody) {
        Response response = makeRequest(
                client(),
                "POST",
                "/_plugins/_ml/models/_register",
                null,
                requestBody,
                null
        );
        return parseFieldFromResponse(response, MLTask.TASK_ID_FIELD).toString();
    }

    protected String deployModel(String modelId) {
        Response response = makeRequest(
                client(),
                "POST",
                "/_plugins/_ml/models/" + modelId + "/_deploy",
                null,
                (String) null,
                null
        );
        return parseFieldFromResponse(response, MLTask.TASK_ID_FIELD).toString();
    }

    @SneakyThrows
    protected Response waitTaskComplete(String taskId) {
        for (int i = 0; i < MAX_TASK_RESULT_QUERY_TIME_IN_SECOND; i++) {
            Response response = makeRequest(
                    client(),
                    "GET",
                    "/_plugins/_ml/tasks/" + taskId,
                    null,
                    (String) null,
                    null
            );
            String state = parseFieldFromResponse(response, MLTask.STATE_FIELD).toString();
            if (state.equals(MLTaskState.COMPLETED)) {
                return response;
            }
            Thread.sleep(DEFAULT_TASK_RESULT_QUERY_INTERVAL_IN_MILLISECOND);
        }
        fail("The task failed to complete after " + MAX_TASK_RESULT_QUERY_TIME_IN_SECOND + " seconds.");
        return null;
    }

    // Register the model then deploy it. Returns the model_id until the model is deployed
    protected String registerModelThenDeploy(String requestBody) {
        String registerModelTaskId = registerModel(requestBody);
        Response registerTaskResponse = waitTaskComplete(registerModelTaskId);
        String modelId = parseFieldFromResponse(registerTaskResponse, MLTask.MODEL_ID_FIELD).toString();
        String deployModelTaskId = deployModel(modelId);
        waitTaskComplete(deployModelTaskId);
        return modelId;
    }

    public static Response makeRequest(
            RestClient client,
            String method,
            String endpoint,
            Map<String, String> params,
            String jsonEntity,
            List<Header> headers
    ) {
        HttpEntity httpEntity = StringUtils.isBlank(jsonEntity) ? null : new StringEntity(jsonEntity, ContentType.APPLICATION_JSON);
        return makeRequest(client, method, endpoint, params, httpEntity, headers);
    }

    public static Response makeRequest(
            RestClient client,
            String method,
            String endpoint,
            Map<String, String> params,
            HttpEntity entity,
            List<Header> headers
    ) {
        return makeRequest(client, method, endpoint, params, entity, headers, false);
    }

    @SneakyThrows
    public static Response makeRequest(
            RestClient client,
            String method,
            String endpoint,
            Map<String, String> params,
            HttpEntity entity,
            List<Header> headers,
            boolean strictDeprecationMode
    ) {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());

        if (params != null) {
            params.forEach(request::addParameter);
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }
}