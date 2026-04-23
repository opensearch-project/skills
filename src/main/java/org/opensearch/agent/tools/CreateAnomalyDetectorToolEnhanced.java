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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.search.SearchRequest;
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
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.opensearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
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

    // LLM output format: {key=value|key=value|...}
    // Parsed into a map by extracting content between { }, splitting on |, splitting on =.
    // Handles any key order, unknown keys, and whitespace.
    private static final Pattern BRACES_PATTERN = Pattern.compile("\\{([^}]+)}");
    private static final Pattern INTERVAL_MINUTES_PATTERN = Pattern.compile("(\\d+)\\s*[Mm]inute");

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
            "ip",
            "date",
            "date_nanos"
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
    private static final int MAX_DETECTOR_NAME_LENGTH = 64;

    private static final int SUGGEST_API_TIMEOUT_SECONDS = 30;
    private static final int DATE_FIELD_QUERY_TIMEOUT_SECONDS = 10;
    private static final String DATE_FIELD_LOOKBACK_PERIOD = "now-30d";

    private static final String DEFAULT_CUSTOM_RESULT_INDEX = "opensearch-ad-plugin-result-auto-insights";
    private static final int MAX_FREQUENCY_MINUTES = 1440; // 24 hours
    private static final int MAX_INDICES_PER_REQUEST = 100;
    private static final String LLM_OUTPUT_FORMAT =
        "{category_field=FIELD_OR_EMPTY|aggregation_field=FIELD1,FIELD2|aggregation_method=METHOD1,METHOD2|date_field=DATE_FIELD"
            + "|filter=FIELD:OP:VALUE_OR_EMPTY|interval=MINUTES|description=ONE_SENTENCE}";
    private static final int MAX_PROMPT_FIELDS = 200;
    private static final int FIELD_FILTER_THRESHOLD = 30;
    private static final int FIELD_FILTER_SAMPLE_SIZE = 100000;
    private static final double FIELD_NULL_THRESHOLD = 0.001; // drop fields present in <0.1% of docs

    private String name = TYPE;
    private String description = DEFAULT_DESCRIPTION;
    private String version;
    private Client client;
    private AnomalyDetectionNodeClient adClient;
    private String modelId;
    private ModelType modelType;
    private String contextPrompt;
    private String customResultIndex;
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
        String customResultIndex,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.client = client;
        this.adClient = new AnomalyDetectionNodeClient(client, namedWriteableRegistry);
        this.modelId = modelId;
        this.customResultIndex = (!Strings.isNullOrEmpty(customResultIndex))
            ? customResultIndex : DEFAULT_CUSTOM_RESULT_INDEX;
        if (!ModelType.OPENAI.toString().equalsIgnoreCase(modelType) && !ModelType.CLAUDE.toString().equalsIgnoreCase(modelType)) {
            throw new IllegalArgumentException("Unsupported model_type: " + modelType);
        }
        this.modelType = ModelType.from(modelType);

        if (Strings.isNullOrEmpty(contextPrompt)) {
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
        log.debug("[CreateAnomalyDetectorToolEnhanced] run() invoked — customResultIndex={}", customResultIndex);
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
            validateIndices(indices);
            int maxRetries = Math.min(
                Integer.parseInt(parameters.getOrDefault("maxRetries", String.valueOf(MAX_FORMAT_FIX_RETRIES))), 3);

            processMultipleIndices(indices, tenantId, maxRetries, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static void validateIndices(List<String> indices) {
        if (indices.size() > MAX_INDICES_PER_REQUEST) {
            throw new IllegalArgumentException("Too many indices: " + indices.size() + ". Maximum is " + MAX_INDICES_PER_REQUEST + ".");
        }
        for (String idx : indices) {
            if (Strings.isNullOrEmpty(idx)) {
                throw new IllegalArgumentException("Index name cannot be empty.");
            }
            if (idx.startsWith(".")) {
                throw new IllegalArgumentException("System indices not supported: " + idx);
            }
            if (idx.length() > 255 || idx.contains("\n") || idx.contains("\r")) {
                throw new IllegalArgumentException("Invalid index name: "
                    + idx.substring(0, Math.min(50, idx.length())).replaceAll("[\\n\\r]", ""));
            }
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

    // Flow: get index insight -> get mappings -> filter null fields -> LLM generates config -> validate -> create detector
    private void processSingleIndex(String indexName, String tenantId, int maxRetries, ActionListener<String> listener) {
        // First, try to get Index Insight analysis (graceful fallback if unavailable)
        getIndexInsight(indexName, tenantId, ActionListener.wrap(
            indexInsight -> {
                getMappingsAndFilterFields(indexName, ActionListener.wrap(
                    mappingContext -> {
                        MappingContext enhancedContext = mappingContext.withIndexInsight(indexInsight);
                        filterNullFieldsIfNeeded(enhancedContext, tenantId, maxRetries, listener);
                    },
                    listener::onFailure
                ));
            },
            e -> {
                log.warn("Index Insight failed for '{}', proceeding without: {}", indexName, e.getMessage());
                getMappingsAndFilterFields(indexName, ActionListener.wrap(
                    mappingContext -> filterNullFieldsIfNeeded(mappingContext, tenantId, maxRetries, listener),
                    listener::onFailure
                ));
            }
        ));
    }

    /**
     * If mapping has more than FIELD_FILTER_THRESHOLD fields, run a sampler aggregation to drop
     * fields that are null in >99.9% of sampled docs (same approach as Index Insight).
     * Otherwise, proceed directly.
     */
    private <T> void filterNullFieldsIfNeeded(MappingContext ctx, String tenantId, int maxRetries, ActionListener<T> listener) {
        if (ctx.filteredMapping.size() <= FIELD_FILTER_THRESHOLD) {
            proceedWithLLM(ctx, tenantId, maxRetries, listener);
            return;
        }
        log.info("Index '{}' has {} fields (>{} threshold), running null field filter",
            ctx.indexName, ctx.filteredMapping.size(), FIELD_FILTER_THRESHOLD);

        filterNullFields(ctx.indexName, ctx.filteredMapping, ActionListener.wrap(
            filtered -> {
                log.info("Null filter reduced '{}' from {} to {} fields", ctx.indexName, ctx.filteredMapping.size(), filtered.size());
                MappingContext filteredCtx = new MappingContext(ctx.indexName, filtered, ctx.dateFields,
                    ctx.indexInsight, ctx.detectorDecisions, ctx.sampleDocs, ctx.dataDensity24h);
                proceedWithLLM(filteredCtx, tenantId, maxRetries, listener);
            },
            e -> {
                log.warn("Null field filter failed for '{}', using unfiltered mapping: {}", ctx.indexName, e.getMessage());
                proceedWithLLM(ctx, tenantId, maxRetries, listener);
            }
        ));
    }

    private <T> void proceedWithLLM(MappingContext mappingContext, String tenantId, int maxRetries, ActionListener<T> listener) {
        // Check for OTel fast-path before LLM
        OtelSignalType otelType = detectOtelSignal(mappingContext.filteredMapping);
        if (otelType != null) {
            log.info("OTel {} mapping detected for '{}', using predefined detectors", otelType, mappingContext.indexName);
            createOtelDetectors(mappingContext.indexName, otelType, mappingContext.filteredMapping, listener);
            return;
        }

        // Pre-filter date fields: drop any with 0 docs in last 30d so LLM only sees viable options
        filterDateFieldsByDensity(mappingContext, ActionListener.wrap(
            filteredCtx -> enrichAndCreateDetectors(filteredCtx, tenantId, maxRetries, listener),
            e -> {
                log.warn("Date field filtering failed for '{}', using all date fields: {}", mappingContext.indexName, e.getMessage());
                enrichAndCreateDetectors(mappingContext, tenantId, maxRetries, listener);
            }
        ));
    }

    /** Gather sample docs and data density, then run the sequential multi-detector loop. */
    private <T> void enrichAndCreateDetectors(MappingContext ctx, String tenantId, int maxRetries, ActionListener<T> listener) {
        String dateField = ctx.dateFields.iterator().next();
        getSampleDocuments(ctx.indexName, 10, ActionListener.wrap(sampleDocs -> {
            getDataDensity(ctx.indexName, dateField, ActionListener.wrap(density -> {
                MappingContext enrichedCtx = ctx.withSampleDocs(sampleDocs).withDataDensity(density);
                createMultipleDetectors(enrichedCtx, tenantId, maxRetries, new ArrayList<>(), new ArrayList<>(), 0, listener);
            }, e -> {
                MappingContext enrichedCtx = ctx.withSampleDocs(sampleDocs);
                createMultipleDetectors(enrichedCtx, tenantId, maxRetries, new ArrayList<>(), new ArrayList<>(), 0, listener);
            }));
        }, e -> {
            createMultipleDetectors(ctx, tenantId, maxRetries, new ArrayList<>(), new ArrayList<>(), 0, listener);
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
    /** Check for field presence, accounting for .keyword sub-fields in text mappings. */
    private static boolean hasField(Map<String, String> fields, String name) {
        return fields.containsKey(name) || fields.containsKey(name + ".keyword");
    }

    /** Resolve to the keyword variant of a field. AD category fields must be keyword type. */
    private static String resolveKeywordField(Map<String, String> fields, String name) {
        if (fields.containsKey(name) && "keyword".equals(fields.get(name))) return name;
        if (fields.containsKey(name + ".keyword")) return name + ".keyword";
        return name; // fallback — validation will catch if wrong type
    }

    private OtelSignalType detectOtelSignal(Map<String, String> fields) {
        if (hasField(fields, "traceId")
            && hasField(fields, "spanId")
            && hasField(fields, "durationInNanos")
            && hasField(fields, "serviceName")) {
            return OtelSignalType.TRACES;
        }
        if (hasField(fields, "severityNumber")
            && hasField(fields, "severityText")
            && hasField(fields, "resource.attributes.service.name")) {
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

    /** Resolve the best time field for OTel detectors from the mapping. Prefers the canonical field for each signal type. */
    private static String resolveOtelTimeField(OtelSignalType type, Map<String, String> fields) {
        // Priority order per signal type based on official OTel/SS4O schemas
        List<String> candidates = type == OtelSignalType.TRACES
            ? List.of("startTime", "@timestamp", "time")
            : List.of("@timestamp", "time", "observedTimestamp");
        for (String candidate : candidates) {
            String fieldType = fields.get(candidate);
            if (fieldType != null && (fieldType.equals("date") || fieldType.equals("date_nanos"))) {
                return candidate;
            }
        }
        return candidates.get(0); // fallback to first preference
    }

    private List<OtelDetectorSpec> buildOtelSpecs(OtelSignalType type, Map<String, String> fields) {
        String timeField = resolveOtelTimeField(type, fields);
        List<OtelDetectorSpec> specs = new ArrayList<>();
        if (type == OtelSignalType.TRACES) {
            specs.add(new OtelDetectorSpec(
                "trace-errors", timeField, resolveKeywordField(fields, "serviceName"), timeField,
                QueryBuilders.termQuery("status.code", 2)
            ));
            specs.add(new OtelDetectorSpec(
                "trace-throughput", timeField, resolveKeywordField(fields, "serviceName"), timeField, null
            ));
        } else {
            specs.add(new OtelDetectorSpec(
                "log-errors", timeField, resolveKeywordField(fields, "resource.attributes.service.name"), timeField,
                QueryBuilders.rangeQuery("severityNumber").gte(17)
            ));
            specs.add(new OtelDetectorSpec(
                "log-volume", timeField, resolveKeywordField(fields, "resource.attributes.service.name"), timeField, null
            ));
        }
        return specs;
    }

    private <T> void createOtelDetectors(String indexName, OtelSignalType type, Map<String, String> fields, ActionListener<T> listener) {
        List<OtelDetectorSpec> specs = buildOtelSpecs(type, fields);
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
        String suffix = "-" + spec.nameSuffix + "-" + UUIDs.randomBase64UUID().substring(0, 6);
        String truncatedIndex = indexName.substring(0, Math.min(indexName.length(), MAX_DETECTOR_NAME_LENGTH - suffix.length()));
        String detectorName = truncatedIndex + suffix;

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
            null, customResultIndex, null, null, null, null, null, null, null, null, null, null,
            new IntervalTimeConfiguration(calculateFrequencyMinutes(DEFAULT_OTEL_INTERVAL_MINUTES), ChronoUnit.MINUTES), true
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
        // Validate config before creating — catches invalid time/category/feature fields
        callValidationAPI(detector, "detector", new ActionListener<ValidateConfigResponse>() {
            @Override
            public void onResponse(ValidateConfigResponse response) {
                if (response.getIssue() != null) {
                    String error = response.getIssue().getMessage();
                    log.warn("OTel detector validation failed for '{}': {}", detector.getName(), error);
                    results.add(DetectorResult.failedValidation(indexName, error).toMap());
                    createOtelDetectorSequentially(specs, indexName, idx + 1, results, listener);
                    return;
                }
                doCreateAndStartOtelDetector(detector, specs, indexName, idx, results, listener);
            }

            @Override
            public void onFailure(Exception e) {
                log.warn("OTel detector validation API failed for '{}', proceeding anyway: {}", detector.getName(), e.getMessage());
                doCreateAndStartOtelDetector(detector, specs, indexName, idx, results, listener);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <T> void doCreateAndStartOtelDetector(
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

    /**
     * Run a sampler aggregation with not_null filters per field to drop fields that are
     * null in >99.9% of sampled docs. Same approach as Index Insight's StatisticalDataTask.
     */
    private void filterNullFields(String indexName, Map<String, String> mapping, ActionListener<Map<String, String>> listener) {
        AggregatorFactories.Builder filters = new AggregatorFactories.Builder();
        // Use index-based names to avoid collisions from dot-to-underscore replacement
        List<String> fieldOrder = new ArrayList<>(mapping.keySet());
        for (int i = 0; i < fieldOrder.size(); i++) {
            filters.addAggregator(AggregationBuilders.filter(
                "f_" + i,
                QueryBuilders.existsQuery(fieldOrder.get(i))
            ));
        }
        SamplerAggregationBuilder sampler = AggregationBuilders.sampler("sample")
            .shardSize(FIELD_FILTER_SAMPLE_SIZE).subAggregations(filters);

        SearchRequest request = new SearchRequest(indexName)
            .source(new SearchSourceBuilder().size(0).query(QueryBuilders.matchAllQuery()).aggregation(sampler));

        client.search(request, ActionListener.wrap(response -> {
            InternalSampler sampleAgg = (InternalSampler) response.getAggregations().getAsMap().get("sample");
            long totalDocs = sampleAgg.getDocCount();
            if (totalDocs == 0) {
                listener.onResponse(mapping);
                return;
            }
            Map<String, Aggregation> aggMap = sampleAgg.getAggregations().getAsMap();
            Map<String, String> result = new LinkedHashMap<>();
            for (int i = 0; i < fieldOrder.size(); i++) {
                Aggregation agg = aggMap.get("f_" + i);
                if (agg instanceof InternalFilter) {
                    long docCount = ((InternalFilter) agg).getDocCount();
                    if (docCount >= FIELD_NULL_THRESHOLD * totalDocs) {
                        String fieldName = fieldOrder.get(i);
                        result.put(fieldName, mapping.get(fieldName));
                    }
                }
            }
            // Safety: if filter is too aggressive, fall back to original
            listener.onResponse(result.size() >= 5 ? result : mapping);
        }, e -> {
            log.warn("Sampler aggregation failed for '{}': {}", indexName, e.getMessage());
            listener.onResponse(mapping);
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

    /**
     * Filter date fields to only those with data in the last 30 days.
     * The LLM picks the semantically best date field from the survivors.
     * Falls back to all date fields if none have recent data.
     */
    private static final int MAX_DATE_FIELDS_TO_CHECK = 10;

    private void filterDateFieldsByDensity(MappingContext ctx, ActionListener<MappingContext> listener) {
        if (ctx.dateFields.size() <= 1) {
            listener.onResponse(ctx);
            return;
        }
        List<String> dateFieldList = new ArrayList<>(ctx.dateFields);
        if (dateFieldList.size() > MAX_DATE_FIELDS_TO_CHECK) {
            dateFieldList = dateFieldList.subList(0, MAX_DATE_FIELDS_TO_CHECK);
        }
        Map<String, Long> counts = new HashMap<>();
        checkNextDateField(ctx, dateFieldList, 0, counts, listener);
    }

    private void checkNextDateField(MappingContext ctx, List<String> dateFieldList, int idx,
                                    Map<String, Long> counts, ActionListener<MappingContext> listener) {
        if (idx >= dateFieldList.size()) {
            // All fields checked — filter to those with data
            Set<String> viable = dateFieldList.stream()
                .filter(f -> counts.getOrDefault(f, 0L) > 0)
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
            if (viable.isEmpty()) {
                log.info("No date fields with recent data for '{}', keeping all", ctx.indexName);
                listener.onResponse(ctx);
            } else {
                log.info("Filtered date fields for '{}': {} → {}", ctx.indexName, ctx.dateFields, viable);
                listener.onResponse(new MappingContext(ctx.indexName, ctx.filteredMapping, viable,
                    ctx.indexInsight, ctx.detectorDecisions, ctx.sampleDocs, ctx.dataDensity24h));
            }
            return;
        }
        String dateField = dateFieldList.get(idx);
        SearchRequest request = new SearchRequest(ctx.indexName)
            .source(new SearchSourceBuilder().size(0)
                .query(QueryBuilders.rangeQuery(dateField).gte(DATE_FIELD_LOOKBACK_PERIOD))
                .timeout(TimeValue.timeValueSeconds(DATE_FIELD_QUERY_TIMEOUT_SECONDS))
                .trackTotalHits(true));
        client.search(request, ActionListener.wrap(
            response -> {
                counts.put(dateField, response.getHits().getTotalHits().value());
                checkNextDateField(ctx, dateFieldList, idx + 1, counts, listener);
            },
            e -> {
                counts.put(dateField, 0L);
                checkNextDateField(ctx, dateFieldList, idx + 1, counts, listener);
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
                Map<String, String> raw = gson.fromJson(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8), Map.class);
                String base = raw.getOrDefault("prompt", "");
                if (!base.isEmpty()) {
                    // Generate both variants from single base prompt
                    Map<String, String> result = new HashMap<>();
                    result.put("OPENAI", base);
                    result.put("CLAUDE", "\n\nHuman: " + base + "\n\nAssistant:");
                    return result;
                }
                return raw; // fallback: legacy format with CLAUDE/OPENAI keys
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
        Map<String, String> fields = fieldsToType;
        if (fields.size() > MAX_PROMPT_FIELDS) {
            log.info("Truncating mapping from {} to {} fields for LLM prompt", fields.size(), MAX_PROMPT_FIELDS);
            fields = fields.entrySet().stream().limit(MAX_PROMPT_FIELDS)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        StringJoiner tableInfoJoiner = new StringJoiner("\n");
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            tableInfoJoiner.add("- " + entry.getKey() + ": " + entry.getValue());
        }

        String insightSection = "";
        if (!Strings.isNullOrEmpty(indexInsight)) {
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

    /**
     * Calculate a jittered frequency as a random multiple of interval between 2×interval and 24h.
     * Spreads detector queries across time to avoid concurrent load spikes.
     */
    private static int calculateFrequencyMinutes(int intervalMinutes) {
        int minFreq = intervalMinutes * 2;
        if (minFreq >= MAX_FREQUENCY_MINUTES) return MAX_FREQUENCY_MINUTES;
        int multiples = (MAX_FREQUENCY_MINUTES - minFreq) / intervalMinutes;
        return minFreq + ThreadLocalRandom.current().nextInt(multiples + 1) * intervalMinutes;
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
        categoryField = categoryField != null ? categoryField.trim() : "";
        boolean isHC = !categoryField.isEmpty()
            && !categoryField.equalsIgnoreCase("null")
            && !categoryField.equalsIgnoreCase("none");

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
            // Filter-in-feature: for HC+count, apply to the first count feature found.
            // This lets HC entities with 0 matches still get a model (0→N detection).
            AggregationBuilder featureAgg;
            if (featureFilter != null && isHC && !filterAppliedInFeature && "count".equalsIgnoreCase(method)) {
                featureAgg = AggregationBuilders.filter("feature_" + cleanField + "_" + method + "_filter", featureFilter)
                    .subAggregation(innerAgg);
                filterAppliedInFeature = true;
            } else {
                featureAgg = innerAgg;
            }
            Feature feature = new Feature(UUIDs.randomBase64UUID(), "feature_" + cleanField + "_" + method, true, featureAgg);
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

        List<String> categoryFields = null;
        if (isHC) {
            categoryFields = java.util.Arrays.stream(categoryField.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
            if (categoryFields.size() > 2) {
                categoryFields = categoryFields.subList(0, 2);
            }
        }

        // If filter was applied inside feature agg, detector-level is matchAll.
        // Otherwise, put filter on detector level.
        QueryBuilder detectorFilter = (featureFilter != null && !filterAppliedInFeature)
            ? featureFilter : QueryBuilders.matchAllQuery();

        String timeField = dateFields.split(",")[0].trim();

        String nameSuffix = "-detector-" + UUIDs.randomBase64UUID().substring(0, 8);
        String truncatedIndex = indexName.substring(0, Math.min(indexName.length(), MAX_DETECTOR_NAME_LENGTH - nameSuffix.length()));

        String description = suggestions.getOrDefault("description", DEFAULT_DETECTOR_DESCRIPTION);
        if (description.isEmpty()) description = DEFAULT_DETECTOR_DESCRIPTION;

        return new AnomalyDetector(
            null,
            null,
            truncatedIndex + nameSuffix,
            description,
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
            customResultIndex,
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
            new IntervalTimeConfiguration(calculateFrequencyMinutes(intervalMinutes), ChronoUnit.MINUTES),
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
                listener.onFailure(new IllegalArgumentException("Unknown error status: " + status));
                return;
        }
        listener.onResponse((T) gson.toJson(List.of(result.toMap())));
    }

    // Note: retry prompts intentionally omit advanced rules (e.g., filter-first-feature for HC).
    // Retry prompts should be focused on fixing the specific error — adding all rules creates
    // cognitive overload and distracts the LLM from the fix. The main prompt covers these rules.
    private <T> void retryWithFormatFix(
        String parseError,
        String indexName,
        String dateFields,
        MappingContext mappingContext,
        String tenantId,
        int maxRetries,
        int currentRetry,
        String validationType,
        ActionListener<T> listener,
        java.util.function.BiConsumer<Map<String, String>, ActionListener<T>> nextPhaseCallback
    ) {
        String contextPrefix = mappingContext != null ? buildRetryContext(mappingContext) : "";
        String fixPrompt = contextPrefix
            + "The previous response had incorrect format. "
            + parseError
            + "\n\nPlease provide suggestions for index '"
            + indexName
            + "' in the exact format: "
            + LLM_OUTPUT_FORMAT
            + "\n\nInterval should be in minutes (default: 10). Only return the configuration in curly braces.";

        callLLM(fixPrompt, tenantId, ActionListener.wrap(fixedResponse -> {
            parseAndRetryWithLLM(
                fixedResponse,
                indexName,
                dateFields,
                mappingContext,
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
        MappingContext mappingContext,
        String tenantId,
        int maxRetries,
        int currentRetry,
        String validationType,
        ActionListener<T> listener,
        java.util.function.BiConsumer<Map<String, String>, ActionListener<T>> nextPhaseCallback
    ) {
        log.debug("LLM_RESPONSE,index={},retry={},response={}", indexName, currentRetry, llmResponse);

        // Parse {key=value|key=value|...} format into a map
        Map<String, String> parsed = parseLLMResponse(llmResponse);

        if (parsed == null) {
            log.error("Parsing failed for response: {}", llmResponse);
            if (currentRetry < maxRetries) {
                String parseError =
                    "Cannot parse response format. Expected: " + LLM_OUTPUT_FORMAT;
                retryWithFormatFix(
                    parseError,
                    indexName,
                    dateFields,
                    mappingContext,
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

        String categoryField = parsed.getOrDefault("category_field", "").strip();
        String aggregationField = parsed.getOrDefault("aggregation_field", "").strip();
        String aggregationMethod = parsed.getOrDefault("aggregation_method", "").strip();
        final String parsedFilter = parsed.getOrDefault("filter", "").strip();
        final String parsedDescription = parsed.getOrDefault("description", "").strip();
        final String parsedInterval = parsed.getOrDefault("interval", String.valueOf(DEFAULT_INTERVAL_MINUTES)).strip();

        // Use LLM's date field choice; validate against known fields, fall back to first if invalid
        String parsedDateField = parsed.getOrDefault("date_field", "").strip();
        Set<String> knownDateFields = java.util.Arrays.stream(dateFields.split(",")).map(String::trim).collect(Collectors.toSet());
        String selectedDateField = (!parsedDateField.isEmpty() && knownDateFields.contains(parsedDateField))
            ? parsedDateField : dateFields.split(",")[0].trim();

        Map<String, String> suggestions = new HashMap<>(Map.of(
            OUTPUT_KEY_INDEX, indexName,
            OUTPUT_KEY_CATEGORY_FIELD, categoryField,
            OUTPUT_KEY_AGGREGATION_FIELD, aggregationField,
            OUTPUT_KEY_AGGREGATION_METHOD, aggregationMethod,
            OUTPUT_KEY_DATE_FIELDS, selectedDateField,
            "interval", parsedInterval
        ));
        if (!parsedFilter.isEmpty()) {
            suggestions.put("filter", parsedFilter);
        }
        if (!parsedDescription.isEmpty()) {
            suggestions.put("description", parsedDescription);
        }
        nextPhaseCallback.accept(suggestions, listener);
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
                    suggestHyperParametersPhase(detector, mappingContext, tenantId, maxRetries, listener);
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
        // Record this failed attempt for retry memory
        MappingContext updatedCtx = mappingContext.withDecision(buildAttemptSummary(originalSuggestions, validationError));

        String fixPrompt = createFixPrompt(originalSuggestions, validationError, updatedCtx.detectorDecisions);
        String fullPrompt = buildRetryContext(updatedCtx) + fixPrompt;

        log.info("LLM_FIX_PROMPT,index={},retry={},error={}", updatedCtx.indexName, currentRetry,
            validationError != null ? validationError.substring(0, Math.min(200, validationError.length())) : "unknown error");

        callLLM(fullPrompt, tenantId, ActionListener.wrap(fixedResponse -> {
            parseAndRetryWithLLM(
                fixedResponse,
                originalSuggestions.get(OUTPUT_KEY_INDEX),
                originalSuggestions.get(OUTPUT_KEY_DATE_FIELDS),
                updatedCtx,
                tenantId,
                maxRetries,
                currentRetry,
                "detector",
                listener,
                (suggestions, listenerCallback) -> validateDetectorPhase(
                    suggestions,
                    updatedCtx,
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

    /** Build mapping context string for retry prompts. Shared by detector and model validation retries. */
    private static String buildRetryContext(MappingContext ctx) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are creating an anomaly detector for the following index:\n\n");
        sb.append("Index: ").append(ctx.indexName).append("\n");
        sb.append("Available fields:\n");
        for (Map.Entry<String, String> field : ctx.filteredMapping.entrySet()) {
            sb.append("- ").append(field.getKey()).append(": ").append(field.getValue()).append("\n");
        }
        sb.append("Available date fields: ").append(String.join(", ", ctx.dateFields)).append("\n");
        if (ctx.dataDensity24h >= 0) {
            sb.append("Data density: ").append(ctx.dataDensity24h).append(" documents in the last 24 hours\n");
        }
        if (ctx.sampleDocs != null) {
            String truncated = ctx.sampleDocs.length() > 1000
                ? ctx.sampleDocs.substring(0, 1000) + "..." : ctx.sampleDocs;
            sb.append("Sample documents: ").append(truncated).append("\n");
        }
        sb.append("\n");
        return sb.toString();
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
        // Recalculate frequency when interval changes to maintain kaituo's invariant: frequency ≥ 2×interval
        TimeConfiguration newFrequency = originalDetector.getFrequency();
        if (response.getInterval() != null) {
            int mins = (int) ((IntervalTimeConfiguration) newInterval).toDuration().toMinutes();
            newFrequency = new IntervalTimeConfiguration(calculateFrequencyMinutes(mins), ChronoUnit.MINUTES);
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
            newFrequency,
            originalDetector.getAutoCreated()
        );
    }

    /**
     * Apply a structured interval or window delay suggestion from model validation directly,
     * avoiding an unnecessary LLM round-trip for issues the validation API already solved.
     */
    private AnomalyDetector applyIntervalSuggestion(AnomalyDetector detector, IntervalTimeConfiguration suggestion, String issueType) {
        TimeConfiguration newInterval = detector.getInterval();
        TimeConfiguration newWindowDelay = detector.getWindowDelay();
        TimeConfiguration newFrequency = detector.getFrequency();
        if ("window_delay".equals(issueType)) {
            newWindowDelay = suggestion;
        } else {
            newInterval = suggestion;
            int mins = (int) suggestion.toDuration().toMinutes();
            newFrequency = new IntervalTimeConfiguration(calculateFrequencyMinutes(mins), ChronoUnit.MINUTES);
        }
        return new AnomalyDetector(
            detector.getId(), detector.getVersion(), detector.getName(), detector.getDescription(),
            detector.getTimeField(), detector.getIndices(), detector.getFeatureAttributes(), detector.getFilterQuery(),
            newInterval, newWindowDelay, detector.getShingleSize(), detector.getUiMetadata(),
            detector.getSchemaVersion(), detector.getLastUpdateTime(), detector.getCategoryFields(),
            detector.getUser(), detector.getCustomResultIndexOrAlias(), detector.getImputationOption(),
            detector.getRecencyEmphasis(), detector.getSeasonIntervals(), detector.getHistoryIntervals(),
            detector.getRules(), detector.getCustomResultIndexMinSize(), detector.getCustomResultIndexMinAge(),
            detector.getCustomResultIndexTTL(), detector.getFlattenResultIndexMapping(), null,
            newFrequency, detector.getAutoCreated()
        );
    }

    // Use AD suggest API to find better interval/window-delay/history based on actual data density.
    // Intentionally overrides LLM's interval — LLM guesses from field names, suggest API uses real data.
    private <T> void suggestHyperParametersPhase(AnomalyDetector detector, MappingContext mappingContext, String tenantId, int maxRetries, ActionListener<T> listener) {
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
                    validateModelPhase(optimizedDetector, mappingContext, tenantId, maxRetries, 0, List.of(), listener);

                } catch (Exception e) {
                    validateModelPhase(detector, mappingContext, tenantId, maxRetries, 0, List.of(), listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // Continue to model validation with original detector even if suggest fails
                validateModelPhase(detector, mappingContext, tenantId, maxRetries, 0, List.of(), listener);
            }
        });
    }

    // Checks if there's enough data to train the model (final validation before creating detector)
    private <T> void validateModelPhase(
        AnomalyDetector detector,
        MappingContext mappingContext,
        String tenantId,
        int maxRetries,
        int currentRetry,
        List<String> previousAttempts,
        ActionListener<T> listener
    ) {
        log.info("Starting model validation");

        callValidationAPI(detector, "model", new ActionListener<ValidateConfigResponse>() {
            @Override
            public void onResponse(ValidateConfigResponse response) {

                if (response.getIssue() != null) {
                    String issueAspect = response.getIssue().getAspect().toString();
                    boolean isBlockingError = "DETECTOR".equals(issueAspect);
                    String errorMessage = response.getIssue().getMessage();

                    // If validation provides a structured interval or window delay suggestion,
                    // apply it directly and re-validate — no LLM call needed for these.
                    IntervalTimeConfiguration intervalSuggestion = response.getIssue().getIntervalSuggestion();
                    if (intervalSuggestion != null && currentRetry < MAX_MODEL_VALIDATION_RETRIES) {
                        String issueType = response.getIssue().getType().getName();
                        log.info("Applying validation suggestion: {}={} for '{}'", issueType, intervalSuggestion, getDetectorIndex(detector));
                        AnomalyDetector adjusted = applyIntervalSuggestion(detector, intervalSuggestion, issueType);
                        validateModelPhase(adjusted, mappingContext, tenantId, maxRetries, currentRetry + 1, previousAttempts, listener);
                        return;
                    }

                    if (currentRetry < MAX_MODEL_VALIDATION_RETRIES) {
                        retryModelValidation(detector, mappingContext, errorMessage, tenantId, maxRetries, currentRetry + 1, previousAttempts, listener);
                    } else {
                        // Max retries reached
                        if (isBlockingError) {
                            log.error("Max retries reached with blocking validation error: {}", errorMessage);
                            respondWithError(listener, getDetectorIndex(detector), "validation", errorMessage);
                        } else {
                            DetectorResult result = DetectorResult
                                .failedValidation(getDetectorIndex(detector), "Non-blocking warning: " + errorMessage);
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
                    retryModelValidation(detector, mappingContext, e.getMessage(), tenantId, maxRetries, currentRetry + 1, previousAttempts, listener);
                } else {
                    DetectorResult result = DetectorResult.failedValidation(getDetectorIndex(detector), "API failure: " + e.getMessage());
                    listener.onResponse((T) gson.toJson(List.of(result.toMap())));
                }
            }
        });
    }

    private <T> void retryModelValidation(
        AnomalyDetector detector,
        MappingContext mappingContext,
        String validationError,
        String tenantId,
        int maxRetries,
        int currentRetry,
        List<String> previousAttempts,
        ActionListener<T> listener
    ) {
        Map<String, String> currentSuggestions = Map
            .of(
                OUTPUT_KEY_INDEX,
                String.join(",", detector.getIndices()),
                OUTPUT_KEY_CATEGORY_FIELD,
                detector.getCategoryFields() == null || detector.getCategoryFields().isEmpty() ? "" : String.join(",", detector.getCategoryFields()),
                OUTPUT_KEY_AGGREGATION_FIELD,
                detector.getFeatureAttributes().stream().map(this::getFeatureField).collect(java.util.stream.Collectors.joining(",")),
                OUTPUT_KEY_AGGREGATION_METHOD,
                detector.getFeatureAttributes().stream().map(this::getAggMethod).collect(java.util.stream.Collectors.joining(",")),
                OUTPUT_KEY_DATE_FIELDS,
                detector.getTimeField(),
                "interval",
                String.valueOf(detector.getIntervalInMinutes())
            );

        List<String> updatedAttempts = new ArrayList<>(previousAttempts);
        updatedAttempts.add(buildAttemptSummary(currentSuggestions, validationError));

        String fixPrompt = createFixPrompt(currentSuggestions, validationError, updatedAttempts);
        String fullPrompt = buildRetryContext(mappingContext) + fixPrompt;

        callLLM(fullPrompt, tenantId, ActionListener.wrap(fixedResponse -> {
            parseAndRetryWithLLM(
                fixedResponse,
                currentSuggestions.get(OUTPUT_KEY_INDEX),
                currentSuggestions.get(OUTPUT_KEY_DATE_FIELDS),
                mappingContext,
                tenantId,
                maxRetries,
                currentRetry,
                "model",
                listener,
                (suggestions, listenerCallback) -> {
                    try {
                        AnomalyDetector newDetector = buildAnomalyDetectorFromSuggestions(suggestions);
                        validateModelPhase(newDetector, mappingContext, tenantId, maxRetries, currentRetry, updatedAttempts, listenerCallback);
                    } catch (Exception e) {
                        log.error("Error building detector from LLM fix: {}", e.getMessage());
                        DetectorResult result = DetectorResult
                            .failedValidation(getDetectorIndex(detector), "Failed to build detector: " + e.getMessage());
                        listenerCallback.onResponse((T) gson.toJson(List.of(result.toMap())));
                    }
                }
            );
        }, e -> {
            log.error("LLM fix request failed: {}", e.getMessage());
            DetectorResult result = DetectorResult
                .failedValidation(getDetectorIndex(detector), "LLM fix request failed: " + e.getMessage());
            listener.onResponse((T) gson.toJson(List.of(result.toMap())));
        }));
    }

    private <T> void createDetector(AnomalyDetector detector, ActionListener<T> listener) {
        IndexAnomalyDetectorRequest request = new IndexAnomalyDetectorRequest("", detector, RestRequest.Method.POST);
        adClient.createAnomalyDetector(request, new ActionListener<IndexAnomalyDetectorResponse>() {
            @Override
            public void onResponse(IndexAnomalyDetectorResponse response) {
                String detectorId = response.getId();
                startDetector(getDetectorIndex(detector), detectorId, detector.getName(), listener);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("Failed to create detector: {}", e.getMessage(), e);
                respondWithError(listener, getDetectorIndex(detector), "create", e.getMessage());
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

    /** Extract the original index field name from a feature, reversing the "feature_{field}_{method}" naming. */
    private String getFeatureField(Feature feature) {
        String name = feature.getName();
        String method = getAggMethod(feature);
        String suffix = "_" + method;
        if (name.startsWith("feature_") && name.endsWith(suffix)) {
            return name.substring("feature_".length(), name.length() - suffix.length());
        }
        return name; // fallback
    }

    /** Safely get the first index from a detector, avoiding IndexOutOfBoundsException. */
    private static String getDetectorIndex(AnomalyDetector detector) {
        List<String> indices = detector.getIndices();
        return (indices != null && !indices.isEmpty()) ? indices.get(0) : "unknown";
    }

    private static final Set<String> KNOWN_LLM_KEYS = Set.of(
        "category_field", "aggregation_field", "aggregation_method", "filter", "interval", "date_field"
    );

    /**
     * Parse LLM response in {key=value|key=value|...} format into a map.
     * Finds the first {...} block containing "category_field", splits on |, splits on =.
     * Known keys are parsed by name (order-independent). "description" is the only free-text
     * key that may contain | — its value is reconstructed from any non-known-key segments.
     * Returns null if no valid block found.
     */
    private static Map<String, String> parseLLMResponse(String llmResponse) {
        if (llmResponse == null) return null;
        Matcher braces = BRACES_PATTERN.matcher(llmResponse);
        while (braces.find()) {
            String content = braces.group(1).strip();
            if (!content.contains("category_field")) continue;

            Map<String, String> result = new HashMap<>();
            StringBuilder descBuilder = new StringBuilder();
            boolean inDescription = false;

            for (String part : content.split("\\|")) {
                int eq = part.indexOf('=');
                String key = eq > 0 ? part.substring(0, eq).strip().replaceAll("\"", "") : "";
                if (KNOWN_LLM_KEYS.contains(key)) {
                    inDescription = false;
                    result.put(key, part.substring(eq + 1).strip().replaceAll("\"", ""));
                } else if ("description".equals(key)) {
                    inDescription = true;
                    descBuilder.append(part.substring(eq + 1).strip());
                } else if (inDescription) {
                    // Continuation of description value that contained |
                    descBuilder.append("|").append(part);
                }
            }
            if (descBuilder.length() > 0) {
                result.put("description", descBuilder.toString().replaceAll("\"", "").strip());
            }
            if (result.containsKey("category_field") && result.containsKey("aggregation_field")) {
                return result;
            }
        }
        return null;
    }

    /**
     * Parse a filter expression (field:operator:value) into a QueryBuilder.
     * Returns null if the expression is empty, invalid, or unparseable.
     */
    QueryBuilder parseFilterExpression(String filterExpr) {
        if (Strings.isNullOrEmpty(filterExpr)) return null;
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

    private String createFixPrompt(Map<String, String> originalSuggestions, String validationError) {
        return createFixPrompt(originalSuggestions, validationError, List.of());
    }

    private String createFixPrompt(Map<String, String> originalSuggestions, String validationError, List<String> previousAttempts) {
        validationError = validationError != null ? validationError : "unknown error";
        String currentInterval = originalSuggestions.getOrDefault("interval", "10");
        String categoryField = originalSuggestions.get(OUTPUT_KEY_CATEGORY_FIELD);
        String filter = originalSuggestions.getOrDefault("filter", "");

        // Check if this is a sparse data issue with unreasonably high suggested interval (>= 4 hours)
        boolean isUnreasonableInterval = false;
        if (validationError.contains("interval")) {
            java.util.regex.Matcher intervalMatcher = INTERVAL_MINUTES_PATTERN.matcher(validationError);
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
            + (Strings.isNullOrEmpty(categoryField) ? "NONE" : categoryField)
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
            + "- Date Field: "
            + originalSuggestions.getOrDefault(OUTPUT_KEY_DATE_FIELDS, "")
            + "\n"
            + "- Filter: "
            + (filter.isEmpty() ? "NONE" : filter)
            + "\n"
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
            + formatPreviousAttempts(previousAttempts)
            + "Return ONLY the corrected configuration in this EXACT format:\n"
            + LLM_OUTPUT_FORMAT + "\n\n"
            + "Use empty string for category_field if removing it. DO NOT include explanations.";
    }

    private static String formatPreviousAttempts(List<String> attempts) {
        if (attempts == null || attempts.isEmpty()) return "";
        StringBuilder sb = new StringBuilder("PREVIOUS ATTEMPTS (do NOT repeat these):\n");
        for (int i = 0; i < attempts.size(); i++) {
            sb.append("- Attempt ").append(i + 1).append(": ").append(attempts.get(i)).append("\n");
        }
        sb.append("Your response MUST be different from all previous attempts.\n\n");
        return sb.toString();
    }

    private static String buildAttemptSummary(Map<String, String> suggestions, String error) {
        return "category=" + suggestions.getOrDefault(OUTPUT_KEY_CATEGORY_FIELD, "")
            + ", field=" + suggestions.getOrDefault(OUTPUT_KEY_AGGREGATION_FIELD, "")
            + ":" + suggestions.getOrDefault(OUTPUT_KEY_AGGREGATION_METHOD, "")
            + ", interval=" + suggestions.getOrDefault("interval", "?")
            + " → " + (error != null ? error.substring(0, Math.min(100, error.length())) : "unknown");
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
            if (llmResponse != null && llmResponse.toUpperCase(Locale.ROOT).contains(NONE_SIGNAL)) {
                log.info("LLM returned NONE for '{}' after {} detectors", ctx.indexName, alreadyCreated.size());
                listener.onResponse((T) gson.toJson(results));
                return;
            }
            parseAndRetryWithLLM(
                llmResponse, ctx.indexName, dateFieldsJoiner.toString(), ctx, tenantId, maxRetries, 0, "model", listener,
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
        if (!Strings.isNullOrEmpty(category)) sb.append(" per ").append(category);
        return sb.toString();
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
            String resultIndex = (String) map.getOrDefault("result_index", "");
            return new CreateAnomalyDetectorToolEnhanced(client, modelId, modelType, prompt, resultIndex, namedWriteableRegistry);
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
