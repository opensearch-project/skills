/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.agent.tools.utils.CommonConstants.COMMON_MODEL_ID_FIELD;
import static org.opensearch.ml.common.CommonValue.TENANT_ID_FIELD;
import static org.opensearch.ml.common.utils.StringUtils.gson;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.ad.client.AnomalyDetectionNodeClient;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.agent.tools.utils.AnomalyDetectorToolHelper;
import org.opensearch.agent.tools.utils.ToolHelper;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.indexInsight.IndexInsight;
import org.opensearch.ml.common.indexInsight.MLIndexInsightType;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.spi.tools.WithModelTool;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetAction;
import org.opensearch.ml.common.transport.indexInsight.MLIndexInsightGetRequest;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.ml.common.utils.ToolUtils;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TimeConfiguration;
import org.opensearch.timeseries.transport.JobRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;
import org.opensearch.timeseries.transport.ValidateConfigRequest;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableMap;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Enhanced tool for creating anomaly detectors with LLM-assisted configuration and validation.
 * Analyzes index mappings, generates detector configurations using LLM, validates through multiple phases,
 * and automatically creates and starts detectors for multiple indices.
 * 
 * Usage:
 * 1. Register agent:
 * POST /_plugins/_ml/agents/_register
 * {
 *   "name": "AnomalyDetectorEnhanced",
 *   "type": "flow",
 *   "tools": [
 *     {
 *       "name": "create_anomaly_detector_enhanced",
 *       "type": "CreateAnomalyDetectorToolEnhanced",
 *       "parameters": {
 *         "model_id": "model-id",
 *         "model_type": "CLAUDE"
 *       }
 *     }
 *   ]
 * }
 * 2. Execute agent:
 * POST /_plugins/_ml/agents/{agent_id}/_execute
 * {
 *   "parameters": {
 *     "input": ["ecommerce-data", "server-logs"]
 *   }
 * }
 * 3. Result: detector creation status for each index
 * {
 *   "ecommerce-data": {
 *     "status": "success",
 *     "detectorId": "abc123",
 *     "detectorName": "ecommerce-data-detector-xyz",
 *     "createResponse": "Detector created successfully",
 *     "startResponse": "Detector started successfully"
 *   },
 *   "server-logs": {
 *     "status": "failed_validation",
 *     "error": "Insufficient data for model training"
 *   }
 * }
 */
@Log4j2
@Setter
@Getter
@ToolAnnotation(CreateAnomalyDetectorToolEnhanced.TYPE)
public class CreateAnomalyDetectorToolEnhanced implements WithModelTool {
    public static final String TYPE = "CreateAnomalyDetectorToolEnhanced";

    private static final String DEFAULT_DESCRIPTION =
        "Enhanced tool for creating anomaly detector configurations. Takes an index name, extracts the index mappings, and uses LLM to generate complete detector JSON configurations ready for the create detector API.";

    private static final String EXTRACT_INFORMATION_REGEX =
        "(?s).*\\{category_field=([^|]*)\\|aggregation_field=([^|]*)\\|aggregation_method=([^|]*)\\|interval=([^}]*)}.*";
    private static final Pattern EXTRACT_INFO_PATTERN = Pattern.compile(EXTRACT_INFORMATION_REGEX);

    // Extended format with optional filter field
    private static final String EXTRACT_INFO_WITH_FILTER_REGEX =
        "(?s).*\\{category_field=([^|]*)\\|aggregation_field=([^|]*)\\|aggregation_method=([^|]*)\\|filter=([^|]*)\\|interval=([^}]*)}.*";
    private static final Pattern EXTRACT_INFO_WITH_FILTER_PATTERN = Pattern.compile(EXTRACT_INFO_WITH_FILTER_REGEX);

    private static final String NONE_SIGNAL = "{NONE}";
    private static final int MAX_DETECTORS_PER_INDEX = 3;

    private static final Set<String> VALID_FIELD_TYPES = Set
        .of(
            "keyword",
            "constant_keyword",
            "wildcard",
            "long",
            "integer",
            "short",
            "byte",
            "double",
            "float",
            "half_float",
            "scaled_float",
            "unsigned_long",
            "ip"
        );

    private static final String OUTPUT_KEY_INDEX = "index";
    private static final String OUTPUT_KEY_CATEGORY_FIELD = "categoryField";
    private static final String OUTPUT_KEY_AGGREGATION_FIELD = "aggregationField";
    private static final String OUTPUT_KEY_AGGREGATION_METHOD = "aggregationMethod";
    private static final String OUTPUT_KEY_DATE_FIELDS = "dateFields";
    private static final Map<String, String> DEFAULT_PROMPT_DICT = loadDefaultPromptFromFile();

    private static final int MAX_DETECTOR_VALIDATION_RETRIES = 3;
    private static final int MAX_MODEL_VALIDATION_RETRIES = 3;
    private static final int MAX_FORMAT_FIX_RETRIES = 1;

    // Detector configuration defaults
    private static final int DEFAULT_INTERVAL_MINUTES = 10;
    private static final int DEFAULT_OTEL_INTERVAL_MINUTES = 2;
    private static final int DEFAULT_WINDOW_DELAY_MINUTES = 1;
    private static final int DEFAULT_SHINGLE_SIZE = 8;
    private static final int DEFAULT_SCHEMA_VERSION = 0;
    private static final String DEFAULT_DETECTOR_DESCRIPTION = "Detector generated by OpenSearch Assistant";

    private static final int SUGGEST_API_TIMEOUT_SECONDS = 30;
    private static final int DATE_FIELD_QUERY_TIMEOUT_SECONDS = 10;
    private static final int DATE_FIELD_SELECTION_TIMEOUT_SECONDS = 15;
    private static final String DATE_FIELD_LOOKBACK_PERIOD = "now-30d";

    private String name = TYPE;
    private String description = DEFAULT_DESCRIPTION;
    private String version;
    private Client client;
    private AnomalyDetectionNodeClient adClient;
    private String modelId;
    private ModelType modelType;
    private String contextPrompt;
    private Map<String, Object> attributes;

    enum ModelType {
        CLAUDE,
        OPENAI;

        public static ModelType from(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

    }

    /**
     *
     * @param client the OpenSearch transport client
     * @param modelId the model ID of LLM
     * @param modelType the model type (CLAUDE or OPENAI)
     * @param contextPrompt custom prompt (if empty, loads from default file)
     * @param namedWriteableRegistry the named writeable registry
     */
    public CreateAnomalyDetectorToolEnhanced(
        Client client,
        String modelId,
        String modelType,
        String contextPrompt,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.client = client;
        this.adClient = new AnomalyDetectionNodeClient(client, namedWriteableRegistry);
        this.modelId = modelId;
        if (!ModelType.OPENAI.toString().equalsIgnoreCase(modelType) && !ModelType.CLAUDE.toString().equalsIgnoreCase(modelType)) {
            throw new IllegalArgumentException("Unsupported model_type: " + modelType);
        }
        this.modelType = ModelType.from(modelType);

        if (contextPrompt.isEmpty()) {
            this.contextPrompt = DEFAULT_PROMPT_DICT.getOrDefault(this.modelType.toString(), "");
        } else {
            this.contextPrompt = contextPrompt;
        }

        if (this.contextPrompt == null || this.contextPrompt.trim().isEmpty()) {
            throw new IllegalArgumentException("Configuration error: detector creation prompt not found");
        }
    }

    /**
     * Creates anomaly detectors for specified indices with automatic validation and error handling.
     * 
     * @param parameters Map containing "input" with JSON array of index names
     * @param listener ActionListener to receive results as JSON string
     */
    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        if (parameters.containsKey("input")) {
            String inputStr = parameters.get("input");
            if (inputStr != null && inputStr.trim().startsWith("[")) {
                parameters.put("input", "{\"indices\": " + inputStr + "}");
            }
        }
        parameters = ToolUtils.extractInputParameters(parameters, attributes);
        final String tenantId = parameters.get(TENANT_ID_FIELD);
        try {
            List<String> indices = AnomalyDetectorToolHelper.extractIndicesList(parameters);
            int maxRetries = Integer.parseInt(parameters.getOrDefault("maxRetries", "1"));

            processMultipleIndices(indices, tenantId, maxRetries, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private <T> void processMultipleIndices(List<String> indices, String tenantId, int maxRetries, ActionListener<T> listener) {
        Map<String, Object> results = new HashMap<>();
        processNextIndex(indices, 0, tenantId, maxRetries, results, listener);
    }

    private <T> void processNextIndex(
        List<String> indices,
        int currentIndex,
        String tenantId,
        int maxRetries,
        Map<String, Object> results,
        ActionListener<T> listener
    ) {
        if (currentIndex >= indices.size()) {
            listener.onResponse((T) gson.toJson(results));
            return;
        }

        String indexName = indices.get(currentIndex);
        processSingleIndex(indexName, tenantId, maxRetries, new ActionListener<String>() {
            @Override
            public void onResponse(String result) {
                // All paths now return a JSON array of detector results
                List<Map<String, Object>> resultList = gson.fromJson(result, List.class);
                results.put(indexName, resultList);
                processNextIndex(indices, currentIndex + 1, tenantId, maxRetries, results, listener);
            }

            @Override
            public void onFailure(Exception e) {
                results.put(indexName, List.of(DetectorResult.failedValidation(indexName, e.getMessage()).toMap()));
                processNextIndex(indices, currentIndex + 1, tenantId, maxRetries, results, listener);
            }
        });
    }

    // Flow: get index insight -> get mappings -> LLM generates config -> validate config -> optimize params -> validate model -> create detector
    private void processSingleIndex(String indexName, String tenantId, int maxRetries, ActionListener<String> listener) {
        // First, try to get Index Insight analysis (graceful fallback if unavailable)
        getIndexInsight(indexName, tenantId, ActionListener.wrap(
            indexInsight -> {
                getMappingsAndFilterFields(indexName, ActionListener.wrap(
                    mappingContext -> {
                        MappingContext enhancedContext = mappingContext.withIndexInsight(indexInsight);
                        proceedWithLLM(enhancedContext, tenantId, maxRetries, listener);
                    },
                    listener::onFailure
                ));
            },
            e -> {
                log.warn("Index Insight failed for '{}', proceeding without: {}", indexName, e.getMessage());
                getMappingsAndFilterFields(indexName, ActionListener.wrap(
                    mappingContext -> proceedWithLLM(mappingContext, tenantId, maxRetries, listener),
                    listener::onFailure
                ));
            }
        ));
    }

    private <T> void proceedWithLLM(MappingContext mappingContext, String tenantId, int maxRetries, ActionListener<T> listener) {
        // Check for OTel fast-path before LLM
        OtelSignalType otelType = detectOtelSignal(mappingContext.filteredMapping);
        if (otelType != null) {
            log.info("OTel {} mapping detected for '{}', using predefined detectors", otelType, mappingContext.indexName);
            createOtelDetectors(mappingContext.indexName, otelType, listener);
            return;
        }

        // Gather additional context, then run sequential multi-detector loop
        String dateField = mappingContext.dateFields.iterator().next();
        getSampleDocuments(mappingContext.indexName, 10, ActionListener.wrap(sampleDocs -> {
            getDataDensity(mappingContext.indexName, dateField, ActionListener.wrap(density -> {
                MappingContext enrichedCtx = mappingContext.withSampleDocs(sampleDocs).withDataDensity(density);
                createMultipleDetectors(enrichedCtx, tenantId, maxRetries, new ArrayList<>(), new ArrayList<>(), 0, listener);
            }, e -> {
                MappingContext enrichedCtx = mappingContext.withSampleDocs(sampleDocs);
                createMultipleDetectors(enrichedCtx, tenantId, maxRetries, new ArrayList<>(), new ArrayList<>(), 0, listener);
            }));
        }, e -> {
            createMultipleDetectors(mappingContext, tenantId, maxRetries, new ArrayList<>(), new ArrayList<>(), 0, listener);
        }));
    }

    // ── OTel fast-path ────────────────────────────────────────────────────────

    enum OtelSignalType { TRACES, LOGS }

    /**
     * Detect OTel signal type from index mapping fields.
     * Traces: Data Prepper otel-v1-apm-span standard template fields.
     * Logs: SS4O log schema fields.
     * Metrics: intentionally deferred — key-value schema requires filter-by-name support.
     */
    private OtelSignalType detectOtelSignal(Map<String, String> fields) {
        if (fields.containsKey("traceId")
            && fields.containsKey("spanId")
            && fields.containsKey("durationInNanos")
            && fields.containsKey("serviceName")) {
            return OtelSignalType.TRACES;
        }
        if (fields.containsKey("severityNumber")
            && fields.containsKey("severityText")
            && fields.containsKey("resource.attributes.service.name")) {
            return OtelSignalType.LOGS;
        }
        return null;
    }

    /** Predefined OTel detector configuration. */
    private static class OtelDetectorSpec {
        final String nameSuffix;
        final String timeField;
        final String categoryField;
        final String featureField;
        final QueryBuilder featureFilter; // null = plain count, non-null = filter-wrapped count

        OtelDetectorSpec(String nameSuffix, String timeField, String categoryField,
                         String featureField, QueryBuilder featureFilter) {
            this.nameSuffix = nameSuffix;
            this.timeField = timeField;
            this.categoryField = categoryField;
            this.featureField = featureField;
            this.featureFilter = featureFilter;
        }
    }

    private List<OtelDetectorSpec> buildOtelSpecs(OtelSignalType type) {
        List<OtelDetectorSpec> specs = new ArrayList<>();
        if (type == OtelSignalType.TRACES) {
            // trace-errors: count spans where status.code=2 (filter in feature for HC entity support)
            specs.add(new OtelDetectorSpec(
                "trace-errors", "startTime", "serviceName", "startTime",
                QueryBuilders.termQuery("status.code", 2)
            ));
            // trace-throughput: count all spans (no filter)
            specs.add(new OtelDetectorSpec(
                "trace-throughput", "startTime", "serviceName", "startTime", null
            ));
        } else {
            // log-errors: count logs with severityNumber>=17 (filter in feature for HC entity support)
            specs.add(new OtelDetectorSpec(
                "log-errors", "time", "resource.attributes.service.name", "time",
                QueryBuilders.rangeQuery("severityNumber").gte(17)
            ));
            // log-volume: count all logs (no filter)
            specs.add(new OtelDetectorSpec(
                "log-volume", "time", "resource.attributes.service.name", "time", null
            ));
        }
        return specs;
    }

    private <T> void createOtelDetectors(String indexName, OtelSignalType type, ActionListener<T> listener) {
        List<OtelDetectorSpec> specs = buildOtelSpecs(type);
        List<Map<String, Object>> results = new ArrayList<>();
        createOtelDetectorSequentially(specs, indexName, 0, results, listener);
    }

    @SuppressWarnings("unchecked")
    private <T> void createOtelDetectorSequentially(
        List<OtelDetectorSpec> specs, String indexName, int idx,
        List<Map<String, Object>> results, ActionListener<T> listener
    ) {
        if (idx >= specs.size()) {
            log.info("OTel detectors done for '{}': {}/{} succeeded",
                indexName, results.stream().filter(r -> "success".equals(r.get("status"))).count(), results.size());
            listener.onResponse((T) gson.toJson(results));
            return;
        }
        OtelDetectorSpec spec = specs.get(idx);
        String detectorName = indexName + "-" + spec.nameSuffix + "-" + UUIDs.randomBase64UUID().substring(0, 6);

        AggregationBuilder innerAgg = AnomalyDetectorToolHelper.createAggregationBuilder("count", spec.featureField);
        // Wrap filter inside the feature aggregation (not on the detector) so HC entities
        // with 0 matching docs still get a model with value=0, enabling 0→N anomaly detection.
        AggregationBuilder featureAgg = spec.featureFilter != null
            ? AggregationBuilders.filter(spec.nameSuffix.replace("-", "_"), spec.featureFilter)
                .subAggregation(innerAgg)
            : innerAgg;
        Feature feature = new Feature(UUIDs.randomBase64UUID(), spec.nameSuffix.replace("-", "_"), true, featureAgg);

        AnomalyDetector detector = new AnomalyDetector(
            null, null, detectorName, DEFAULT_DETECTOR_DESCRIPTION,
            spec.timeField, List.of(indexName), List.of(feature), QueryBuilders.matchAllQuery(),
            new IntervalTimeConfiguration(DEFAULT_OTEL_INTERVAL_MINUTES, ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(DEFAULT_WINDOW_DELAY_MINUTES, ChronoUnit.MINUTES),
            DEFAULT_SHINGLE_SIZE, null, DEFAULT_SCHEMA_VERSION, Instant.now(),
            List.of(spec.categoryField),
            null, null, null, null, null, null, null, null, null, null, null, null,
            new IntervalTimeConfiguration(DEFAULT_OTEL_INTERVAL_MINUTES, ChronoUnit.MINUTES), true
        );

        // Use suggest API to find optimal interval, then create
        suggestAndCreateOtelDetector(detector, specs, indexName, idx, results, listener);
    }

    /** Call suggest API for interval optimization, then create and start the detector. */
    @SuppressWarnings("unchecked")
    private <T> void suggestAndCreateOtelDetector(
        AnomalyDetector detector, List<OtelDetectorSpec> specs, String indexName, int idx,
        List<Map<String, Object>> results, ActionListener<T> listener
    ) {
        SuggestConfigParamRequest suggestRequest = new SuggestConfigParamRequest(
            AnalysisType.AD, detector, "interval",
            TimeValue.timeValueSeconds(SUGGEST_API_TIMEOUT_SECONDS)
        );

        adClient.suggestAnomalyDetector(suggestRequest, ActionListener.wrap(
            suggestResp -> {
                AnomalyDetector optimized = suggestResp.getInterval() != null
                    ? applySuggestionsToDetector(detector, suggestResp)
                    : detector;
                createAndStartOtelDetector(optimized, specs, indexName, idx, results, listener);
            },
            e -> {
                log.warn("Suggest API failed for OTel detector '{}', using default interval: {}", detector.getName(), e.getMessage());
                createAndStartOtelDetector(detector, specs, indexName, idx, results, listener);
            }
        ));
    }

    @SuppressWarnings("unchecked")
    private <T> void createAndStartOtelDetector(
        AnomalyDetector detector, List<OtelDetectorSpec> specs, String indexName, int idx,
        List<Map<String, Object>> results, ActionListener<T> listener
    ) {
        String detectorName = detector.getName();
        IndexAnomalyDetectorRequest createReq = new IndexAnomalyDetectorRequest("", detector, RestRequest.Method.POST);
        adClient.createAnomalyDetector(createReq, ActionListener.wrap(
            createResp -> {
                String detectorId = createResp.getId();
                log.info("OTel detector created: {} ({})", detectorName, detectorId);
                JobRequest startReq = new JobRequest(detectorId, ".opendistro-anomaly-detectors", null, false,
                    "/_plugins/_anomaly_detection/detectors/" + detectorId + "/_start");
                adClient.startAnomalyDetector(startReq, ActionListener.wrap(
                    startResp -> {
                        results.add(DetectorResult.success(indexName, detectorId, detectorName,
                            "Detector created successfully", "Detector started successfully").toMap());
                        createOtelDetectorSequentially(specs, indexName, idx + 1, results, listener);
                    },
                    e -> {
                        log.error("Failed to start OTel detector {}: {}", detectorName, e.getMessage());
                        results.add(DetectorResult.failedStart(indexName, detectorId, e.getMessage()).toMap());
                        createOtelDetectorSequentially(specs, indexName, idx + 1, results, listener);
                    }
                ));
            },
            e -> {
                log.error("Failed to create OTel detector {}: {}", detectorName, e.getMessage());
                results.add(DetectorResult.failedCreate(indexName, e.getMessage()).toMap());
                createOtelDetectorSequentially(specs, indexName, idx + 1, results, listener);
            }
        ));
    }

    /**
     * Get Index Insight analysis for the given index using ALL type.
     * Returns null content on empty response; calls onFailure if API is unavailable.
     */
    private void getIndexInsight(String indexName, String tenantId, ActionListener<String> listener) {
        log.info("Fetching Index Insight for index '{}'", indexName);

        MLIndexInsightGetRequest request = new MLIndexInsightGetRequest(indexName, MLIndexInsightType.ALL, tenantId);

        client.execute(MLIndexInsightGetAction.INSTANCE, request, ActionListener.wrap(response -> {
            IndexInsight insight = response.getIndexInsight();
            String content = insight != null ? insight.getContent() : null;

            if (content != null && !content.isEmpty()) {
                log.info("Index Insight for '{}': {} chars", indexName, content.length());
                listener.onResponse(content);
            } else {
                log.warn("Index Insight returned empty content for '{}'", indexName);
                listener.onResponse(null);
            }
        }, e -> {
            log.warn("Index Insight API call failed for '{}': {} ({})", indexName, e.getMessage(), e.getClass().getSimpleName());
            listener.onFailure(e);
        }));
    }

    private void getSampleDocuments(String indexName, int size, ActionListener<String> listener) {
        SearchRequest request = new SearchRequest(indexName)
            .source(new SearchSourceBuilder().size(size).query(QueryBuilders.matchAllQuery())
                .sort("_doc").trackTotalHits(false));
        client.search(request, ActionListener.wrap(response -> {
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length == 0) { listener.onResponse(null); return; }
            List<Map<String, Object>> docs = new ArrayList<>();
            for (SearchHit hit : hits) { docs.add(hit.getSourceAsMap()); }
            listener.onResponse(gson.toJson(docs));
        }, e -> {
            log.warn("Failed to fetch sample docs for '{}': {}", indexName, e.getMessage());
            listener.onResponse(null);
        }));
    }

    private void getDataDensity(String indexName, String dateField, ActionListener<Long> listener) {
        SearchRequest request = new SearchRequest(indexName)
            .source(new SearchSourceBuilder().size(0)
                .query(QueryBuilders.rangeQuery(dateField).gte("now-24h")).trackTotalHits(true));
        client.search(request, ActionListener.wrap(
            response -> listener.onResponse(response.getHits().getTotalHits().value()),
            e -> {
                log.warn("Failed to get data density for '{}': {}", indexName, e.getMessage());
                listener.onResponse(-1L);
            }
        ));
    }

    private String extractResponseFromDataAsMap(Map<String, Object> dataAsMap) {
        if (dataAsMap == null) {
            return null;
        }
        if (dataAsMap.containsKey("response")) {
            return (String) dataAsMap.get("response");
        }
        // Bedrock and Claude both end with content[0].text — just navigate to the content array
        List<Map<String, Object>> content = null;
        if (dataAsMap.containsKey("output")) {
            try {
                Map<String, Object> output = (Map<String, Object>) dataAsMap.get("output");
                Map<String, Object> message = (Map<String, Object>) output.get("message");
                content = (List<Map<String, Object>>) message.get("content");
            } catch (Exception e) {
                log.error("Failed to parse Bedrock response format", e);
            }
        } else if (dataAsMap.containsKey("content")) {
            try {
                content = (List<Map<String, Object>>) dataAsMap.get("content");
            } catch (Exception e) {
                log.error("Failed to parse Claude content format", e);
            }
        }
        if (content != null && !content.isEmpty()) {
            return (String) content.getFirst().get("text");
        }
        log.error("Unknown or unparseable response format. Available keys: {}", dataAsMap.keySet());
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> loadDefaultPromptFromFile() {
        try (
            InputStream inputStream = CreateAnomalyDetectorToolEnhanced.class
                .getResourceAsStream("CreateAnomalyDetectorEnhancedPrompt.json")
        ) {
            if (inputStream != null) {
                return gson.fromJson(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8), Map.class);
            }
        } catch (IOException e) {
            log.error("Failed to load prompt from the file CreateAnomalyDetectorEnhancedPrompt.json, error: ", e);
        }
        return new HashMap<>();
    }

    /**
     *
     * @param fieldsToType the flattened field-> field type mapping
     * @param indexName the index name
     * @param dateFields the comma-separated date fields
     * @param indexInsight the index insight analysis (can be null)
     * @return the prompt about creating anomaly detector
     */
    private String constructPrompt(
        final Map<String, String> fieldsToType,
        final String indexName,
        final String dateFields,
        final String indexInsight
    ) {
        StringJoiner tableInfoJoiner = new StringJoiner("\n");
        for (Map.Entry<String, String> entry : fieldsToType.entrySet()) {
            tableInfoJoiner.add("- " + entry.getKey() + ": " + entry.getValue());
        }

        String insightSection = "";
        if (indexInsight != null && !indexInsight.isEmpty()) {
            insightSection = "\n\nINDEX ANALYSIS (from Index Insight):\n" + indexInsight
                + "\n\nUse the above analysis to inform your detector configuration choices.";
        }

        Map<String, String> indexInfo = ImmutableMap
            .of("indexName", indexName, "indexMapping", tableInfoJoiner.toString(), "dateFields", dateFields, "indexInsight", insightSection);
        StringSubstitutor substitutor = new StringSubstitutor(indexInfo, "${indexInfo.", "}");

        String basePrompt = substitutor.replace(contextPrompt);
        // If prompt template doesn't have ${indexInfo.indexInsight} placeholder, append insight before OUTPUT FORMAT
        if (!contextPrompt.contains("${indexInfo.indexInsight}") && !insightSection.isEmpty()) {
            int outputFormatIndex = basePrompt.indexOf("OUTPUT FORMAT:");
            if (outputFormatIndex > 0) {
                basePrompt = basePrompt.substring(0, outputFormatIndex) + insightSection + "\n\n" + basePrompt.substring(outputFormatIndex);
            } else {
                basePrompt = basePrompt + insightSection;
            }
        }

        return basePrompt;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return parameters != null && !parameters.isEmpty();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    private AnomalyDetector buildAnomalyDetectorFromSuggestions(Map<String, String> suggestions) {
        return buildAnomalyDetectorFromSuggestions(suggestions, null);
    }

    private AnomalyDetector buildAnomalyDetectorFromSuggestions(Map<String, String> suggestions, QueryBuilder filterQuery) {
        String indexName = suggestions.get(OUTPUT_KEY_INDEX);
        String categoryField = suggestions.get(OUTPUT_KEY_CATEGORY_FIELD);
        String aggregationFields = suggestions.get(OUTPUT_KEY_AGGREGATION_FIELD);
        String aggregationMethods = suggestions.get(OUTPUT_KEY_AGGREGATION_METHOD);
        String dateFields = suggestions.get(OUTPUT_KEY_DATE_FIELDS);
        String intervalStr = suggestions.getOrDefault("interval", String.valueOf(DEFAULT_INTERVAL_MINUTES));

        // Parse filter from suggestions if present (from LLM output)
        QueryBuilder featureFilter = filterQuery;
        if (featureFilter == null) {
            String filterExpr = suggestions.get("filter");
            featureFilter = parseFilterExpression(filterExpr);
        }

        // Parse interval (default to 10 minutes)
        int intervalMinutes = DEFAULT_INTERVAL_MINUTES;
        try {
            intervalMinutes = Integer.parseInt(intervalStr);
        } catch (NumberFormatException e) {
            log.warn("Invalid interval '{}', using default {} minutes", intervalStr, DEFAULT_INTERVAL_MINUTES);
        }

        // Parse comma-separated fields and methods
        String[] fields = aggregationFields.split(",");
        String[] methods = aggregationMethods.split(",");

        if (fields.length != methods.length) {
            throw new IllegalArgumentException("Number of aggregation fields and methods must match");
        }

        // Determine if this is an HC detector (has category field)
        boolean isHC = categoryField != null
            && !categoryField.trim().isEmpty()
            && !categoryField.trim().equalsIgnoreCase("null")
            && !categoryField.trim().equalsIgnoreCase("none");

        List<Feature> features = new ArrayList<>();
        boolean filterAppliedInFeature = false;
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i].trim();
            String method = methods[i].trim();

            if (field.isEmpty() || method.isEmpty()) {
                continue;
            }

            String cleanField = field.startsWith("feature_") ? field.substring(8) : field;
            // Handle template variable leak - LLM sometimes outputs literal template variables
            String actualDateField = dateFields.split(",")[0].trim();
            cleanField = cleanField.replace("${dateFields}", actualDateField)
                                   .replace("${indexInfo.dateFields}", actualDateField);

            AggregationBuilder innerAgg = AnomalyDetectorToolHelper.createAggregationBuilder(method, cleanField);
            // For HC detectors with count: put filter inside feature (entities with 0 matches still get a model)
            // For SE detectors or non-count: filter goes on detector level
            AggregationBuilder featureAgg;
            if (featureFilter != null && isHC && "count".equalsIgnoreCase(method)) {
                featureAgg = AggregationBuilders.filter("feature_" + cleanField + "_filter", featureFilter)
                    .subAggregation(innerAgg);
                filterAppliedInFeature = true;
            } else {
                featureAgg = innerAgg;
            }
            Feature feature = new Feature(UUIDs.randomBase64UUID(), "feature_" + cleanField, true, featureAgg);
            features.add(feature);
        }

        if (features.isEmpty()) {
            throw new IllegalArgumentException(
                "No valid features could be built from LLM suggestions. "
                    + "Fields: ["
                    + aggregationFields
                    + "], Methods: ["
                    + aggregationMethods
                    + "]"
            );
        }

        List<String> categoryFields = isHC ? List.of(categoryField.trim()) : null;

        // If filter was applied inside feature agg, detector-level is matchAll.
        // Otherwise, put filter on detector level.
        QueryBuilder detectorFilter = (featureFilter != null && !filterAppliedInFeature)
            ? featureFilter : QueryBuilders.matchAllQuery();

        String timeField = dateFields.split(",")[0].trim();

        return new AnomalyDetector(
            null,
            null,
            indexName + "-detector-" + UUIDs.randomBase64UUID().substring(0, 8),
            DEFAULT_DETECTOR_DESCRIPTION,
            timeField,
            List.of(indexName),
            features,
            detectorFilter,
            new IntervalTimeConfiguration(intervalMinutes, ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(DEFAULT_WINDOW_DELAY_MINUTES, ChronoUnit.MINUTES),
            DEFAULT_SHINGLE_SIZE,
            null,
            DEFAULT_SCHEMA_VERSION,
            Instant.now(),
            categoryFields,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new IntervalTimeConfiguration(intervalMinutes, ChronoUnit.MINUTES),
            true
        );
    }

    private void callLLM(String prompt, String tenantId, ActionListener<String> listener) {
        RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet
            .builder()
            .parameters(Collections.singletonMap("prompt", prompt))
            .build();
        ActionRequest request = new MLPredictionTaskRequest(
            modelId,
            MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build(),
            null,
            tenantId
        );

        client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(mlTaskResponse -> {
            ModelTensorOutput output = (ModelTensorOutput) mlTaskResponse.getOutput();
            List<ModelTensors> outputList = output != null ? output.getMlModelOutputs() : null;
            if (outputList == null || outputList.isEmpty()) {
                listener.onFailure(new IllegalStateException("Remote endpoint returned empty output."));
                return;
            }
            List<ModelTensor> tensorList = outputList.get(0).getMlModelTensors();
            if (tensorList == null || tensorList.isEmpty()) {
                listener.onFailure(new IllegalStateException("Remote endpoint returned empty tensors."));
                return;
            }
            Map<String, Object> dataAsMap = (Map<String, Object>) tensorList.get(0).getDataAsMap();
            String response = extractResponseFromDataAsMap(dataAsMap);

            if (Strings.isNullOrEmpty(response)) {
                listener.onFailure(new IllegalStateException("Remote endpoint fails to inference."));
            } else {
                listener.onResponse(response);
            }
        }, listener::onFailure));
    }

    private <T> void respondWithError(ActionListener<T> listener, String indexName, String status, String errorMessage) {
        String errorId = UUIDs.randomBase64UUID().substring(0, 8);
        log.error("Detector operation failed [{}] - Index: {}, Status: {}, Error: {}", errorId, indexName, status, errorMessage);

        DetectorResult result;
        switch (status) {
            case "validation":
                result = DetectorResult.failedValidation(indexName, errorMessage);
                break;
            case "create":
                result = DetectorResult.failedCreate(indexName, errorMessage);
                break;
            case "start":
                result = DetectorResult.failedStart(indexName, null, errorMessage);
                break;
            default:
                throw new IllegalArgumentException("Unknown status: " + status);
        }
        listener.onResponse((T) gson.toJson(List.of(result.toMap())));
    }

    private <T> void retryWithFormatFix(
        String parseError,
        String indexName,
        String dateFields,
        String tenantId,
        int maxRetries,
        int currentRetry,
        String validationType,
        ActionListener<T> listener,
        java.util.function.BiConsumer<Map<String, String>, ActionListener<T>> nextPhaseCallback
    ) {
        String fixPrompt = "The previous response had incorrect format. "
            + parseError
            + "\n\nPlease provide suggestions for index '"
            + indexName
            + "' in the exact format: "
            + "{category_field=field|aggregation_field=field1,field2|aggregation_method=method1,method2|interval=10}"
            + "\n\nInterval should be in minutes (default: 10). Only return the configuration in curly braces.";

        callLLM(fixPrompt, tenantId, ActionListener.wrap(fixedResponse -> {
            parseAndRetryWithLLM(
                fixedResponse,
                indexName,
                dateFields,
                tenantId,
                maxRetries,
                currentRetry,
                validationType,
                listener,
                nextPhaseCallback
            );
        }, e -> {
            log.error("LLM fix request failed: {}", e.getMessage());
            listener.onFailure(e);
        }));
    }

    private <T> void callValidationAPI(AnomalyDetector detector, String validationType, ActionListener<ValidateConfigResponse> listener) {
        try {
            ValidateConfigRequest validateRequest = new ValidateConfigRequest(AnalysisType.AD, detector, validationType);
            adClient.validateAnomalyDetector(validateRequest, listener);
        } catch (Throwable e) {
            log.error("Validation API call failed: {}", e.getMessage(), e);
            listener.onFailure(new RuntimeException("Validation API failed: " + e.getMessage(), e));
        }
    }

    private <T> void parseAndRetryWithLLM(
        String llmResponse,
        String indexName,
        String dateFields,
        String tenantId,
        int maxRetries,
        int currentRetry,
        String validationType,
        ActionListener<T> listener,
        java.util.function.BiConsumer<Map<String, String>, ActionListener<T>> nextPhaseCallback
    ) {
        log.debug("LLM_RESPONSE,index={},retry={},response={}", indexName, currentRetry, llmResponse);

        // Try extended format with filter first, fall back to original format
        Matcher matcher = EXTRACT_INFO_WITH_FILTER_PATTERN.matcher(llmResponse);
        String filterExpr = null;
        if (matcher.matches()) {
            filterExpr = matcher.group(4).replaceAll("\"", "").strip();
        } else {
            matcher = EXTRACT_INFO_PATTERN.matcher(llmResponse);
        }

        if (!matcher.matches()) {
            log.error("Regex parsing failed for response: {}", llmResponse);
            if (currentRetry < maxRetries) {
                String parseError =
                    "Cannot parse response format. Expected: {category_field=field|aggregation_field=field1,field2|aggregation_method=method1,method2|interval=minutes}";
                retryWithFormatFix(
                    parseError,
                    indexName,
                    dateFields,
                    tenantId,
                    maxRetries,
                    currentRetry + 1,
                    validationType,
                    listener,
                    nextPhaseCallback
                );
            } else {
                listener.onFailure(new IllegalStateException("Cannot parse LLM response after " + maxRetries + " retries"));
            }
            return;
        }

        String categoryField = matcher.group(1).replaceAll("\"", "").strip();
        String aggregationField = matcher.group(2).replaceAll("\"", "").strip();
        String aggregationMethod = matcher.group(3).replaceAll("\"", "").strip();
        // Filter regex: groups are 1=category, 2=field, 3=method, 4=filter, 5=interval
        // Original regex: groups are 1=category, 2=field, 3=method, 4=interval
        String interval = filterExpr != null ? matcher.group(5).replaceAll("\"", "").strip()
                                             : matcher.group(4).replaceAll("\"", "").strip();
        final String parsedFilter = filterExpr;

        // Select optimal date field from all available date fields
        String[] dateFieldArray = dateFields.split(",");
        selectOptimalDateField(indexName, dateFieldArray, new ActionListener<String>() {
            @Override
            public void onResponse(String optimalDateField) {
                Map<String, String> suggestions = new HashMap<>(Map.of(
                    OUTPUT_KEY_INDEX, indexName,
                    OUTPUT_KEY_CATEGORY_FIELD, categoryField,
                    OUTPUT_KEY_AGGREGATION_FIELD, aggregationField,
                    OUTPUT_KEY_AGGREGATION_METHOD, aggregationMethod,
                    OUTPUT_KEY_DATE_FIELDS, optimalDateField,
                    "interval", interval
                ));
                if (parsedFilter != null && !parsedFilter.isEmpty()) {
                    suggestions.put("filter", parsedFilter);
                }
                nextPhaseCallback.accept(suggestions, listener);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Failed to select optimal date field, using first available: {}", e.getMessage());
                String fallbackDateField = dateFieldArray.length > 0 ? dateFieldArray[0].trim() : dateFields.split(",")[0];

                Map<String, String> suggestions = new HashMap<>(Map.of(
                    OUTPUT_KEY_INDEX, indexName,
                    OUTPUT_KEY_CATEGORY_FIELD, categoryField,
                    OUTPUT_KEY_AGGREGATION_FIELD, aggregationField,
                    OUTPUT_KEY_AGGREGATION_METHOD, aggregationMethod,
                    OUTPUT_KEY_DATE_FIELDS, fallbackDateField,
                    "interval", interval
                ));
                if (parsedFilter != null && !parsedFilter.isEmpty()) {
                    suggestions.put("filter", parsedFilter);
                }

                nextPhaseCallback.accept(suggestions, listener);
            }
        });
    }

    // Checks if detector config is valid (fields exist, aggregations work, etc.)
    private <T> void validateDetectorPhase(
        Map<String, String> suggestions,
        MappingContext mappingContext,
        String tenantId,
        int maxRetries,
        int currentRetry,
        ActionListener<T> listener
    ) {
        if (currentRetry >= MAX_DETECTOR_VALIDATION_RETRIES) {
            listener.onFailure(new RuntimeException(
                "Detector validation failed after " + MAX_DETECTOR_VALIDATION_RETRIES + " retries"));
            return;
        }
        try {
            log.info("Validating detector configuration");
            AnomalyDetector detector = buildAnomalyDetectorFromSuggestions(suggestions);

            callValidationAPI(detector, "detector", new ActionListener<ValidateConfigResponse>() {
                @Override
                public void onResponse(ValidateConfigResponse response) {
                    if (response.getIssue() != null) {
                        String errorMessage = response.getIssue().getMessage();
                        String issueType = response.getIssue().getType().toString();

                        // GENERAL_SETTINGS = max detectors reached for cluster
                        if ("GENERAL_SETTINGS".equals(issueType)) {
                            log.error("System limit error (non-retryable): {}", errorMessage);
                            respondWithError(listener, suggestions.get(OUTPUT_KEY_INDEX), "validation", errorMessage);
                            return;
                        }
                        if (currentRetry < MAX_DETECTOR_VALIDATION_RETRIES) {
                            retryDetectorValidation(
                                suggestions,
                                mappingContext,
                                errorMessage,
                                tenantId,
                                maxRetries,
                                currentRetry + 1,
                                listener
                            );
                        } else {
                            log.error("Max detector validation retries reached: {}", errorMessage);
                            listener
                                .onFailure(
                                    new RuntimeException(
                                        "Detector validation failed after " + MAX_DETECTOR_VALIDATION_RETRIES + " retries: " + errorMessage
                                    )
                                );
                        }
                        return;
                    }
                    suggestHyperParametersPhase(detector, tenantId, maxRetries, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    log.error("Detector validation API failed: {}", e.getMessage(), e);
                    if (currentRetry < MAX_DETECTOR_VALIDATION_RETRIES) {
                        retryDetectorValidation(
                            suggestions,
                            mappingContext,
                            e.getMessage(),
                            tenantId,
                            maxRetries,
                            currentRetry + 1,
                            listener
                        );
                    } else {
                        listener
                            .onFailure(
                                new RuntimeException(
                                    "Detector validation failed after " + MAX_DETECTOR_VALIDATION_RETRIES + " retries: " + e.getMessage()
                                )
                            );
                    }
                }
            });

        } catch (Exception e) {
            log.error("Error building detector: {}", e.getMessage(), e);
            if (currentRetry < MAX_FORMAT_FIX_RETRIES) {
                retryDetectorValidation(suggestions, mappingContext, e.getMessage(), tenantId, maxRetries, currentRetry + 1, listener);
            } else {
                listener.onFailure(e);
            }
        }
    }

    private <T> void retryDetectorValidation(
        Map<String, String> originalSuggestions,
        MappingContext mappingContext,
        String validationError,
        String tenantId,
        int maxRetries,
        int currentRetry,
        ActionListener<T> listener
    ) {
        String indexName = originalSuggestions.get(OUTPUT_KEY_INDEX);

        StringBuilder contextBuilder = new StringBuilder();
        contextBuilder.append("You are creating an anomaly detector for the following index:\n\n");
        contextBuilder.append("Index: ").append(indexName).append("\n");
        contextBuilder.append("Available fields:\n");
        for (Map.Entry<String, String> field : mappingContext.filteredMapping.entrySet()) {
            contextBuilder.append("- ").append(field.getKey()).append(": ").append(field.getValue()).append("\n");
        }
        contextBuilder.append("Available date fields: ").append(String.join(", ", mappingContext.dateFields)).append("\n\n");

        String fixPrompt = createFixPrompt(originalSuggestions, validationError);
        String fullPrompt = contextBuilder.toString() + fixPrompt;

        log.info("LLM_FIX_PROMPT,index={},retry={},error={}", indexName, currentRetry,
            validationError != null ? validationError.substring(0, Math.min(200, validationError.length())) : "unknown error");

        callLLM(fullPrompt, tenantId, ActionListener.wrap(fixedResponse -> {
            parseAndRetryWithLLM(
                fixedResponse,
                originalSuggestions.get(OUTPUT_KEY_INDEX),
                originalSuggestions.get(OUTPUT_KEY_DATE_FIELDS),
                tenantId,
                maxRetries,
                currentRetry,
                "detector",
                listener,
                (suggestions, listenerCallback) -> validateDetectorPhase(
                    suggestions,
                    mappingContext,
                    tenantId,
                    maxRetries,
                    currentRetry,
                    listenerCallback
                )
            );
        }, e -> {
            log.error("LLM fix request failed: {}", e.getMessage());
            listener.onFailure(e);
        }));
    }

    private AnomalyDetector applySuggestionsToDetector(AnomalyDetector originalDetector, SuggestConfigParamResponse response) {
        TimeConfiguration newInterval = originalDetector.getInterval();
        TimeConfiguration newWindowDelay = originalDetector.getWindowDelay();
        Integer newHistoryIntervals = originalDetector.getHistoryIntervals();
        if (response.getInterval() != null) {
            newInterval = response.getInterval();
        }
        if (response.getWindowDelay() != null) {
            newWindowDelay = response.getWindowDelay();
        }
        if (response.getHistory() != null) {
            newHistoryIntervals = response.getHistory();
        }
        // Create new detector with applied suggestions
        return new AnomalyDetector(
            originalDetector.getId(),
            originalDetector.getVersion(),
            originalDetector.getName(),
            originalDetector.getDescription(),
            originalDetector.getTimeField(),
            originalDetector.getIndices(),
            originalDetector.getFeatureAttributes(),
            originalDetector.getFilterQuery(),
            newInterval,
            newWindowDelay,
            originalDetector.getShingleSize(),
            originalDetector.getUiMetadata(),
            originalDetector.getSchemaVersion(),
            originalDetector.getLastUpdateTime(),
            originalDetector.getCategoryFields(),
            originalDetector.getUser(),
            originalDetector.getCustomResultIndexOrAlias(),
            originalDetector.getImputationOption(),
            originalDetector.getRecencyEmphasis(),
            originalDetector.getSeasonIntervals(),
            newHistoryIntervals,
            originalDetector.getRules(),
            originalDetector.getCustomResultIndexMinSize(),
            originalDetector.getCustomResultIndexMinAge(),
            originalDetector.getCustomResultIndexTTL(),
            originalDetector.getFlattenResultIndexMapping(),
            null,
            originalDetector.getFrequency(),
            originalDetector.getAutoCreated()
        );
    }

    // Use AD suggest API to find better interval/window-delay/history based on actual data density.
    // Intentionally overrides LLM's interval — LLM guesses from field names, suggest API uses real data.
    private <T> void suggestHyperParametersPhase(AnomalyDetector detector, String tenantId, int maxRetries, ActionListener<T> listener) {
        log.info("Starting suggest api step");

        // Create suggest request for interval, history, window_delay
        SuggestConfigParamRequest suggestRequest = new SuggestConfigParamRequest(
            AnalysisType.AD,
            detector,
            "interval,history,window_delay",
            TimeValue.timeValueSeconds(SUGGEST_API_TIMEOUT_SECONDS)
        );

        adClient.suggestAnomalyDetector(suggestRequest, new ActionListener<SuggestConfigParamResponse>() {
            @Override
            public void onResponse(SuggestConfigParamResponse response) {
                try {
                    AnomalyDetector optimizedDetector = applySuggestionsToDetector(detector, response);
                    validateModelPhase(optimizedDetector, tenantId, maxRetries, 0, listener);

                } catch (Exception e) {
                    validateModelPhase(detector, tenantId, maxRetries, 0, listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // Continue to model validation with original detector even if suggest fails
                validateModelPhase(detector, tenantId, maxRetries, 0, listener);
            }
        });
    }

    // Checks if there's enough data to train the model (final validation before creating detector)
    private <T> void validateModelPhase(
        AnomalyDetector detector,
        String tenantId,
        int maxRetries,
        int currentRetry,
        ActionListener<T> listener
    ) {
        log.info("Starting model validation");

        callValidationAPI(detector, "model", new ActionListener<ValidateConfigResponse>() {
            @Override
            public void onResponse(ValidateConfigResponse response) {

                if (response.getIssue() != null) {
                    String issueAspect = response.getIssue().getAspect().toString();
                    boolean isBlockingError = issueAspect != null && issueAspect.toLowerCase(Locale.ROOT).startsWith("detector");
                    String errorMessage = response.getIssue().getMessage();
                    if (currentRetry < MAX_MODEL_VALIDATION_RETRIES) {
                        retryModelValidation(detector, errorMessage, tenantId, maxRetries, currentRetry + 1, listener);
                    } else {
                        // Max retries reached
                        if (isBlockingError) {
                            log.error("Max retries reached with blocking validation error: {}", errorMessage);
                            respondWithError(listener, detector.getIndices().get(0), "validation", errorMessage);
                        } else {
                            DetectorResult result = DetectorResult
                                .failedValidation(detector.getIndices().get(0), "Non-blocking warning: " + errorMessage);
                            listener.onResponse((T) gson.toJson(List.of(result.toMap())));
                        }
                    }
                    return;
                }
                createDetector(detector, listener);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Model validation API failed: {}", e.getMessage(), e);
                if (currentRetry < MAX_MODEL_VALIDATION_RETRIES) {
                    retryModelValidation(detector, e.getMessage(), tenantId, maxRetries, currentRetry + 1, listener);
                } else {
                    DetectorResult result = DetectorResult.failedValidation(detector.getIndices().get(0), "API failure: " + e.getMessage());
                    listener.onResponse((T) gson.toJson(List.of(result.toMap())));
                }
            }
        });
    }

    private <T> void retryModelValidation(
        AnomalyDetector detector,
        String validationError,
        String tenantId,
        int maxRetries,
        int currentRetry,
        ActionListener<T> listener
    ) {
        Map<String, String> currentSuggestions = Map
            .of(
                OUTPUT_KEY_INDEX,
                String.join(",", detector.getIndices()),
                OUTPUT_KEY_CATEGORY_FIELD,
                detector.getCategoryFields() == null || detector.getCategoryFields().isEmpty() ? "" : detector.getCategoryFields().get(0),
                OUTPUT_KEY_AGGREGATION_FIELD,
                detector.getFeatureAttributes().stream().map(Feature::getName).collect(java.util.stream.Collectors.joining(",")),
                OUTPUT_KEY_AGGREGATION_METHOD,
                detector.getFeatureAttributes().stream().map(this::getAggMethod).collect(java.util.stream.Collectors.joining(",")),
                OUTPUT_KEY_DATE_FIELDS,
                detector.getTimeField(),
                "interval",
                String.valueOf(detector.getIntervalInMinutes())
            );

        String fixPrompt = createFixPrompt(currentSuggestions, validationError);

        callLLM(fixPrompt, tenantId, ActionListener.wrap(fixedResponse -> {
            parseAndRetryWithLLM(
                fixedResponse,
                currentSuggestions.get(OUTPUT_KEY_INDEX),
                currentSuggestions.get(OUTPUT_KEY_DATE_FIELDS),
                tenantId,
                maxRetries,
                currentRetry,
                "model",
                listener,
                (suggestions, listenerCallback) -> {
                    try {
                        AnomalyDetector newDetector = buildAnomalyDetectorFromSuggestions(suggestions);
                        validateModelPhase(newDetector, tenantId, maxRetries, currentRetry, listenerCallback);
                    } catch (Exception e) {
                        log.error("Error building detector from LLM fix: {}", e.getMessage());
                        DetectorResult result = DetectorResult
                            .failedValidation(detector.getIndices().get(0), "Failed to build detector: " + e.getMessage());
                        listenerCallback.onResponse((T) gson.toJson(List.of(result.toMap())));
                    }
                }
            );
        }, e -> {
            log.error("LLM fix request failed: {}", e.getMessage());
            DetectorResult result = DetectorResult
                .failedValidation(detector.getIndices().get(0), "LLM fix request failed: " + e.getMessage());
            listener.onResponse((T) gson.toJson(List.of(result.toMap())));
        }));
    }

    private <T> void createDetector(AnomalyDetector detector, ActionListener<T> listener) {
        IndexAnomalyDetectorRequest request = new IndexAnomalyDetectorRequest("", detector, RestRequest.Method.POST);
        adClient.createAnomalyDetector(request, new ActionListener<IndexAnomalyDetectorResponse>() {
            @Override
            public void onResponse(IndexAnomalyDetectorResponse response) {
                String detectorId = response.getId();
                startDetector(detector.getIndices().get(0), detectorId, detector.getName(), listener);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Failed to create detector: {}", e.getMessage(), e);
                respondWithError(listener, detector.getIndices().get(0), "create", e.getMessage());
            }
        });
    }

    private <T> void startDetector(String indexName, String detectorId, String detectorName, ActionListener<T> listener) {
        JobRequest request = new JobRequest(
            detectorId,
            ".opendistro-anomaly-detectors",
            null,
            false,
            "/_plugins/_anomaly_detection/detectors/" + detectorId + "/_start"
        );

        adClient.startAnomalyDetector(request, ActionListener.wrap(response -> {
            DetectorResult result = DetectorResult
                .success(indexName, detectorId, detectorName, "Detector created successfully", "Detector started successfully");
            listener.onResponse((T) gson.toJson(List.of(result.toMap())));
        }, e -> {
            log.error("Failed to start detector: {}", e.getMessage());
            respondWithError(listener, indexName, "start", e.getMessage());
        }));
    }

    String getAggMethod(Feature feature) {
        String type = feature.getAggregation().getType();
        return "value_count".equals(type) ? "count" : type;
    }

    /**
     * Parse a filter expression (field:operator:value) into a QueryBuilder.
     * Returns null if the expression is empty, invalid, or unparseable.
     */
    private QueryBuilder parseFilterExpression(String filterExpr) {
        if (filterExpr == null || filterExpr.isEmpty()) return null;
        String[] parts = filterExpr.split(":", 3);
        if (parts.length != 3) {
            log.warn("Invalid filter expression '{}', ignoring", filterExpr);
            return null;
        }
        try {
            String field = parts[0].trim();
            String operator = parts[1].trim().toLowerCase(Locale.ROOT);
            String value = parts[2].trim();
            switch (operator) {
                case "gte": return QueryBuilders.rangeQuery(field).gte(value);
                case "gt":  return QueryBuilders.rangeQuery(field).gt(value);
                case "lte": return QueryBuilders.rangeQuery(field).lte(value);
                case "lt":  return QueryBuilders.rangeQuery(field).lt(value);
                case "eq":  return QueryBuilders.termQuery(field, value);
                default:
                    log.warn("Unknown filter operator '{}', ignoring filter", operator);
                    return null;
            }
        } catch (Exception e) {
            log.warn("Failed to parse filter expression '{}': {}", filterExpr, e.getMessage());
            return null;
        }
    }

    private void selectOptimalDateField(String indexName, String[] suggestedDateFields, ActionListener<String> listener) {
        // Query each date field to count recent documents (last 30 days)
        List<CompletableFuture<Map.Entry<String, Long>>> futures = new ArrayList<>();

        for (String dateField : suggestedDateFields) {
            CompletableFuture<Map.Entry<String, Long>> future = new CompletableFuture<>();
            futures.add(future);

            // Count documents with recent data for this date field
            SearchRequest searchRequest = new SearchRequest(indexName)
                .source(
                    new SearchSourceBuilder()
                        .query(
                            QueryBuilders
                                .boolQuery()
                                .must(QueryBuilders.existsQuery(dateField.trim()))
                                .must(QueryBuilders.rangeQuery(dateField.trim()).gte(DATE_FIELD_LOOKBACK_PERIOD).lte("now"))
                        )
                        .size(0)
                        .timeout(TimeValue.timeValueSeconds(DATE_FIELD_QUERY_TIMEOUT_SECONDS))
                );

            client.search(searchRequest, ActionListener.wrap((SearchResponse response) -> {
                long count = response.getHits().getTotalHits().value();
                future.complete(Map.entry(dateField.trim(), count));
            }, e -> {
                log.warn("Failed to query date field '{}': {}", dateField.trim(), e.getMessage());
                future.complete(Map.entry(dateField.trim(), 0L));
            }));
        }

        // Wait for all queries to complete and select the field with most recent data
        CompletableFuture
            .allOf(futures.toArray(new CompletableFuture[0]))
            .orTimeout(DATE_FIELD_SELECTION_TIMEOUT_SECONDS, java.util.concurrent.TimeUnit.SECONDS)
            .thenAccept(v -> {
                String bestDateField = futures
                    .stream()
                    .map(CompletableFuture::join)
                    .max(Comparator.comparing(Map.Entry::getValue))
                    .map(Map.Entry::getKey)
                    .orElse(suggestedDateFields[0]); // fallback to first suggestion
                listener.onResponse(bestDateField);
            })
            .exceptionally(e -> {
                futures.forEach(FutureUtils::cancel);
                log.error("Date field selection timed out for index '{}', using first suggestion: {}", indexName, e.getMessage());
                listener.onResponse(suggestedDateFields[0]);
                return null;
            });
    }

    private String createFixPrompt(Map<String, String> originalSuggestions, String validationError) {
        validationError = validationError != null ? validationError : "unknown error";
        String currentInterval = originalSuggestions.getOrDefault("interval", "10");
        String categoryField = originalSuggestions.get(OUTPUT_KEY_CATEGORY_FIELD);

        // Check if this is a sparse data issue with unreasonably high suggested interval (>= 4 hours)
        boolean isUnreasonableInterval = false;
        if (validationError.contains("interval")) {
            java.util.regex.Matcher intervalMatcher = java.util.regex.Pattern.compile("(\\d+)\\s*[Mm]inute").matcher(validationError);
            if (intervalMatcher.find()) {
                isUnreasonableInterval = Integer.parseInt(intervalMatcher.group(1)) >= 240;
            }
        }

        // Check if this is a field type incompatibility error
        boolean isFieldTypeError = validationError.contains("not supported for aggregation")
            || validationError.contains("Text fields are not optimised");

        String sparseDataGuidance = "";
        if (isFieldTypeError) {
            sparseDataGuidance = "\n**FIELD TYPE ERROR - CRITICAL FIX REQUIRED**:\n"
                + "- You selected a keyword/text field with avg/sum/max/min aggregation - THIS WILL NOT WORK\n"
                + "- RULE: keyword/text fields can ONLY use 'count' aggregation\n"
                + "- RULE: avg/sum/max/min ONLY work on numeric fields (long, integer, double, float)\n"
                + "- FIX OPTIONS:\n"
                + "  1. Change aggregation method to 'count' for the keyword field, OR\n"
                + "  2. Pick a DIFFERENT field that is numeric (long/integer/double/float)\n"
                + "- Look at the field types in the mapping and pick numeric fields for avg/sum/max/min\n";
        } else if (isUnreasonableInterval) {
            sparseDataGuidance = "\n**HIGH INTERVAL DETECTED**:\n"
                + "- Validation suggests interval >4 hours\n"
                + "- For operational metrics (latency, errors, CPU, memory): intervals >4 hours are too slow for actionable alerts\n"
                + "  → PREFERRED: Remove category field entirely (set to empty) to achieve 10-60 min intervals\n"
                + "  → ALTERNATIVE: Choose different category field with lower cardinality\n"
                + "- For business metrics (revenue, sales, users): longer intervals may be acceptable\n"
                + "  → Consider if the suggested interval fits the use case\n"
                + "- Evaluate based on the aggregation fields being monitored\n";
        } else if (validationError.contains("sparse data") || validationError.contains("interval")) {
            sparseDataGuidance = "\n**SPARSE DATA GUIDANCE**:\n"
                + "- For intervals 60-120 min: acceptable, proceed with suggestion\n"
                + "- For intervals >120 min: consider removing category field instead\n";
        }

        return "VALIDATION ERROR: "
            + validationError
            + "\n\n"
            + "Current Configuration:\n"
            + "- Category Field: "
            + (categoryField == null || categoryField.isEmpty() ? "NONE" : categoryField)
            + "\n"
            + "- Aggregation Fields: "
            + originalSuggestions.get(OUTPUT_KEY_AGGREGATION_FIELD)
            + "\n"
            + "- Aggregation Methods: "
            + originalSuggestions.get(OUTPUT_KEY_AGGREGATION_METHOD)
            + "\n"
            + "- Interval: "
            + currentInterval
            + " minutes\n"
            + sparseDataGuidance
            + "\n\nFIX STRATEGY:\n"
            + "1. Evaluate aggregation fields: operational metrics need shorter intervals, business metrics can use longer\n"
            + "2. For operational metrics with intervals >240 min: REMOVE category field (set to empty string)\n"
            + "3. For business metrics: accept suggested interval if appropriate for use case\n"
            + "4. For 'invalid query' errors: fix only the problematic field/method\n\n"
            + "CRITICAL RULES:\n"
            + "- ONLY valid aggregation methods: avg, sum, min, max, count\n"
            + "- Keyword fields can ONLY use 'count'\n"
            + "- NEVER sum/avg status_code or http_status - use bytes, duration instead\n"
            + "- Prefer numeric fields: bytes_sent, total_time, response.bytes, duration\n"
            + "- Keep the same aggregation method unless it caused the error\n\n"
            + "Return ONLY the corrected configuration in this EXACT format:\n"
            + "{category_field=FIELD_OR_EMPTY|aggregation_field=FIELD1,FIELD2|aggregation_method=METHOD1,METHOD2|interval=MINUTES}\n\n"
            + "Use empty string for category_field if removing it. DO NOT include explanations.";
    }

    /**
     * Context object to hold mapping data and index insight
     */
    private static class MappingContext {
        final String indexName;
        final Map<String, String> filteredMapping;
        final Set<String> dateFields;
        final String indexInsight;
        final List<String> detectorDecisions;
        final String sampleDocs;
        final long dataDensity24h;

        MappingContext(String indexName, Map<String, String> filteredMapping, Set<String> dateFields) {
            this(indexName, filteredMapping, dateFields, null, new ArrayList<>(), null, -1L);
        }

        MappingContext(String indexName, Map<String, String> filteredMapping, Set<String> dateFields, String indexInsight) {
            this(indexName, filteredMapping, dateFields, indexInsight, new ArrayList<>(), null, -1L);
        }

        MappingContext(String indexName, Map<String, String> filteredMapping, Set<String> dateFields,
                       String indexInsight, List<String> detectorDecisions, String sampleDocs, long dataDensity24h) {
            this.indexName = indexName;
            this.filteredMapping = filteredMapping;
            this.dateFields = dateFields;
            this.indexInsight = indexInsight;
            this.detectorDecisions = detectorDecisions;
            this.sampleDocs = sampleDocs;
            this.dataDensity24h = dataDensity24h;
        }

        MappingContext withIndexInsight(String insight) {
            return new MappingContext(indexName, filteredMapping, dateFields, insight, detectorDecisions, sampleDocs, dataDensity24h);
        }

        MappingContext withDecision(String decision) {
            List<String> updated = new ArrayList<>(detectorDecisions);
            updated.add(decision);
            return new MappingContext(indexName, filteredMapping, dateFields, indexInsight, updated, sampleDocs, dataDensity24h);
        }

        MappingContext withSampleDocs(String docs) {
            return new MappingContext(indexName, filteredMapping, dateFields, indexInsight, detectorDecisions, docs, dataDensity24h);
        }

        MappingContext withDataDensity(long density) {
            return new MappingContext(indexName, filteredMapping, dateFields, indexInsight, detectorDecisions, sampleDocs, density);
        }
    }

    /**
     * Result object to track detector creation status per index
     */
    private enum DetectorStatus {
        SUCCESS("success"),
        FAILED_VALIDATION("failed_validation"),
        FAILED_CREATE("failed_create"),
        FAILED_START("failed_start");

        private final String value;

        DetectorStatus(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private static class DetectorResult {
        String indexName;
        DetectorStatus status;
        String detectorId;
        String detectorName;
        String error;
        String createResponse;
        String startResponse;

        String toJson() {
            return gson.toJson(toMap());
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            if (indexName != null)
                map.put("indexName", indexName);
            if (status != null)
                map.put("status", status.toString());
            if (detectorId != null)
                map.put("detectorId", detectorId);
            if (detectorName != null)
                map.put("detectorName", detectorName);
            if (error != null)
                map.put("error", error);
            if (createResponse != null)
                map.put("createResponse", createResponse);
            if (startResponse != null)
                map.put("startResponse", startResponse);
            return map;
        }

        static DetectorResult failedValidation(String indexName, String error) {
            DetectorResult result = new DetectorResult();
            result.indexName = indexName;
            result.status = DetectorStatus.FAILED_VALIDATION;
            result.error = error;
            return result;
        }

        static DetectorResult failedCreate(String indexName, String error) {
            DetectorResult result = new DetectorResult();
            result.indexName = indexName;
            result.status = DetectorStatus.FAILED_CREATE;
            result.error = error;
            return result;
        }

        static DetectorResult failedStart(String indexName, String detectorId, String error) {
            DetectorResult result = new DetectorResult();
            result.indexName = indexName;
            result.status = DetectorStatus.FAILED_START;
            result.detectorId = detectorId;
            result.error = error;
            return result;
        }

        static DetectorResult success(
            String indexName,
            String detectorId,
            String detectorName,
            String createResponse,
            String startResponse
        ) {
            DetectorResult result = new DetectorResult();
            result.indexName = indexName;
            result.status = DetectorStatus.SUCCESS;
            result.detectorId = detectorId;
            result.detectorName = detectorName;
            result.createResponse = createResponse;
            result.startResponse = startResponse;
            return result;
        }
    }

    /**
     * Step 2: Get mappings and filter fields
     */
    private void getMappingsAndFilterFields(String indexName, ActionListener<MappingContext> listener) {
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(indexName);

        client.admin().indices().getMappings(getMappingsRequest, ActionListener.wrap(response -> {
            Map<String, MappingMetadata> mappings = response.getMappings();

            if (mappings.isEmpty()) {
                listener.onFailure(new IllegalArgumentException("No mapping found for the index: " + indexName));
                return;
            }

            String firstIndexName = mappings.keySet().iterator().next();
            MappingMetadata mappingMetadata = mappings.get(firstIndexName);
            Map<String, Object> mappingSource = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");

            if (mappingSource == null) {
                listener.onFailure(new IllegalArgumentException("Index '" + indexName + "' has no mapping metadata"));
                return;
            }

            Map<String, String> fieldsToType = new HashMap<>();
            ToolHelper.extractFieldNamesTypes(mappingSource, fieldsToType, "", true);

            final Set<String> dateFields = ToolHelper.findDateTypeFields(fieldsToType);
            if (dateFields.isEmpty()) {
                listener.onFailure(new IllegalArgumentException("Index '" + indexName + "' has no date fields"));
                return;
            }

            Map<String, String> filteredMapping = fieldsToType
                .entrySet()
                .stream()
                .filter(entry -> VALID_FIELD_TYPES.contains(entry.getValue()))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

            MappingContext context = new MappingContext(firstIndexName, filteredMapping, dateFields);
            listener.onResponse(context);

        }, e -> {
            log.error("Failed to get mapping: {}", e.getMessage());
            if (e instanceof IndexNotFoundException) {
                listener.onFailure(new IllegalArgumentException("Index '" + indexName + "' does not exist"));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    // ── Multi-detector sequential creation ──────────────────────────────────

    @SuppressWarnings("unchecked")
    private <T> void createMultipleDetectors(
        MappingContext ctx, String tenantId, int maxRetries,
        List<String> alreadyCreated, List<Map<String, Object>> results,
        int totalAttempts, ActionListener<T> listener
    ) {
        if (alreadyCreated.size() >= MAX_DETECTORS_PER_INDEX || totalAttempts >= MAX_DETECTORS_PER_INDEX + 2) {
            listener.onResponse((T) gson.toJson(results));
            return;
        }

        String alreadyCreatedContext = buildAlreadyCreatedContext(alreadyCreated);

        // Build prompt with context prepended
        StringJoiner dateFieldsJoiner = new StringJoiner(",");
        ctx.dateFields.forEach(dateFieldsJoiner::add);
        String basePrompt = constructPrompt(ctx.filteredMapping, ctx.indexName, dateFieldsJoiner.toString(), ctx.indexInsight);

        // Inject sample docs and data density
        StringBuilder extraContext = new StringBuilder();
        if (ctx.dataDensity24h >= 0) {
            extraContext.append("DATA DENSITY: ").append(ctx.dataDensity24h).append(" documents in the last 24 hours\n");
            if (ctx.dataDensity24h == 0) {
                extraContext.append("WARNING: This index has no data in the last 24 hours.\n");
            }
        }
        if (ctx.sampleDocs != null) {
            String truncated = ctx.sampleDocs.length() > 2000
                ? ctx.sampleDocs.substring(0, 2000) + "..." : ctx.sampleDocs;
            extraContext.append("\nSAMPLE DOCUMENTS:\n").append(truncated).append("\n");
        }

        String fullPrompt = alreadyCreatedContext + extraContext + basePrompt;

        callLLM(fullPrompt, tenantId, ActionListener.wrap(llmResponse -> {
            // Check for NONE signal before parsing — only valid in multi-detector loop
            if (llmResponse != null && llmResponse.contains(NONE_SIGNAL)) {
                log.info("LLM returned NONE for '{}' after {} detectors", ctx.indexName, alreadyCreated.size());
                listener.onResponse((T) gson.toJson(results));
                return;
            }
            parseAndRetryWithLLM(
                llmResponse, ctx.indexName, dateFieldsJoiner.toString(), tenantId, maxRetries, 0, "model", listener,
                (suggestions, listenerCallback) -> {
                    // Run through existing validation pipeline
                    validateDetectorPhase(suggestions, ctx, tenantId, maxRetries, 0, new ActionListener<T>() {
                        @Override
                        public void onResponse(T resultJson) {
                            List<Map<String, Object>> detectorResults = gson.fromJson((String) resultJson, List.class);
                            Map<String, Object> result = detectorResults.get(0);
                            results.add(result);

                            // Stop on systemic failures
                            String error = (String) result.get("error");
                            if (error != null && error.contains("GENERAL_SETTINGS")) {
                                listenerCallback.onResponse((T) gson.toJson(results));
                                return;
                            }

                            // Build summary and continue
                            if (DetectorStatus.SUCCESS.toString().equals(result.get("status"))) {
                                List<String> updated = new ArrayList<>(alreadyCreated);
                                updated.add(buildDetectorSummary(suggestions));
                                createMultipleDetectors(ctx, tenantId, maxRetries, updated, results, totalAttempts + 1, listenerCallback);
                            } else {
                                // Non-systemic failure — still try next detector
                                createMultipleDetectors(ctx, tenantId, maxRetries, alreadyCreated, results, totalAttempts + 1, listenerCallback);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            results.add(DetectorResult.failedValidation(ctx.indexName, e.getMessage()).toMap());
                            listenerCallback.onResponse((T) gson.toJson(results));
                        }
                    });
                }
            );
        }, e -> {
            log.error("LLM call failed for multi-detector on '{}': {}", ctx.indexName, e.getMessage());
            if (results.isEmpty()) {
                // First call failed — propagate failure
                listener.onFailure(e);
            } else {
                // Subsequent call failed — return what we have
                listener.onResponse((T) gson.toJson(results));
            }
        }));
    }

    private String buildAlreadyCreatedContext(List<String> summaries) {
        if (summaries.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        sb.append("ALREADY CREATED DETECTORS FOR THIS INDEX (do NOT create similar detectors):\n\n");
        for (int i = 0; i < summaries.size(); i++) {
            sb.append("Detector ").append(i + 1).append(": ").append(summaries.get(i)).append("\n");
        }
        sb.append("\nCreate a DIFFERENT detector monitoring a DIFFERENT signal.\n");
        sb.append("Do NOT use the same aggregation field or monitor the same type of anomaly.\n");
        sb.append("If no more useful, non-overlapping signals exist, return exactly: ").append(NONE_SIGNAL).append("\n\n");
        sb.append("---\n\n");
        return sb.toString();
    }

    private String buildDetectorSummary(Map<String, String> suggestions) {
        String field = suggestions.get(OUTPUT_KEY_AGGREGATION_FIELD);
        String method = suggestions.get(OUTPUT_KEY_AGGREGATION_METHOD);
        String category = suggestions.get(OUTPUT_KEY_CATEGORY_FIELD);
        String filter = suggestions.getOrDefault("filter", "");
        StringBuilder sb = new StringBuilder();
        sb.append(method).append("(").append(field).append(")");
        if (!filter.isEmpty()) sb.append(" WHERE ").append(filter);
        if (category != null && !category.isEmpty()) sb.append(" per ").append(category);
        return sb.toString();
    }

    /**
     * Step 3-6: Generate config with LLM, parse, and select date field.
     * Note: The multi-detector loop (createMultipleDetectors) reimplements this flow inline.
     * This method is retained as a standalone entry point for single-detector creation.
     */
    private <T> void generateAndParseConfig(
        MappingContext mappingContext,
        String tenantId,
        int maxRetries,
        ActionListener<T> listener,
        java.util.function.BiConsumer<Map<String, String>, ActionListener<T>> nextPhaseCallback
    ) {
        StringJoiner dateFieldsJoiner = new StringJoiner(",");
        mappingContext.dateFields.forEach(dateFieldsJoiner::add);

        String prompt = constructPrompt(mappingContext.filteredMapping, mappingContext.indexName, dateFieldsJoiner.toString(), mappingContext.indexInsight);

        callLLM(prompt, tenantId, ActionListener.wrap(finalResponse -> {
            parseAndRetryWithLLM(
                finalResponse,
                mappingContext.indexName,
                dateFieldsJoiner.toString(),
                tenantId,
                maxRetries,
                0,
                "model",
                listener,
                nextPhaseCallback
            );
        }, e -> {
            log.error("Model prediction failed: {}", e.getMessage());
            listener.onFailure(e);
        }));
    }

    /**
     * The tool factory
     */
    public static class Factory implements WithModelTool.Factory<CreateAnomalyDetectorToolEnhanced> {
        private Client client;
        private NamedWriteableRegistry namedWriteableRegistry;

        private static volatile CreateAnomalyDetectorToolEnhanced.Factory INSTANCE;

        /**
         * Create or return the singleton factory instance
         */
        public static CreateAnomalyDetectorToolEnhanced.Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (CreateAnomalyDetectorToolEnhanced.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new CreateAnomalyDetectorToolEnhanced.Factory();
                return INSTANCE;
            }
        }

        public void init(Client client, NamedWriteableRegistry namedWriteableRegistry) {
            this.client = client;
            this.namedWriteableRegistry = namedWriteableRegistry;
        }

        /**
         *
         * @param map the input parameters
         * @return the instance of this tool
         */
        @Override
        public CreateAnomalyDetectorToolEnhanced create(Map<String, Object> map) {
            String modelId = (String) map.getOrDefault(COMMON_MODEL_ID_FIELD, "");
            if (modelId.isEmpty()) {
                throw new IllegalArgumentException("model_id cannot be empty.");
            }
            String modelType = (String) map.getOrDefault("model_type", ModelType.CLAUDE.toString());
            // if model type is empty, use the default value
            if (modelType.isEmpty()) {
                modelType = ModelType.CLAUDE.toString();
            } else if (!ModelType.OPENAI.toString().equalsIgnoreCase(modelType)
                && !ModelType.CLAUDE.toString().equalsIgnoreCase(modelType)) {
                throw new IllegalArgumentException("Unsupported model_type: " + modelType);
            }
            String prompt = (String) map.getOrDefault("prompt", "");
            return new CreateAnomalyDetectorToolEnhanced(client, modelId, modelType, prompt, namedWriteableRegistry);
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }

        @Override
        public List<String> getAllModelKeys() {
            return List.of(COMMON_MODEL_ID_FIELD);
        }
    }
}
