/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.job;

import static org.opensearch.agent.indices.SkillsIndexEnum.SKILLS_INDEX_SUMMARY;
import static org.opensearch.agent.job.Constants.INDEX_SUMMARY_JOB_THREAD_POOL;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.agent.indices.IndicesHelper;
import org.opensearch.agent.tools.IndexRoutingTool;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingType;

import lombok.Builder;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Take index mapping and sample data as summary, embedding it and save to k-NN index as vector store
 */
@Log4j2
public class IndexSummaryEmbeddingJob implements Runnable {

    private Client client;
    private ClusterService clusterService;
    private IndicesHelper indicesHelper;
    private MLClients mlClients;

    public static String INDEX_SUMMARY_EMBEDDING_INDEX = SKILLS_INDEX_SUMMARY.getIndexName();
    public static String INDEX_SUMMARY_EMBEDDING_FIELD_PREFIX = "index_summary_embedding";
    public static String INDEX_NAME_FIELD = "index_name";
    public static String INDEX_SUMMARY_SIGN_FIELD = "index_summary_sign";
    public static String SAMPLE_DATA_INCLUDED_FIELD = "sample_data_included";
    public static String DATA_STREAM_FIELD = "data_stream";
    public static String INDEX_PATTERNS_FIELD = "index_patterns";
    public static String ALIAS_FIELD = "alias";
    public static String INDEX_SUMMARY_FIELD = "index_summary";

    public static String SAMPLE_DATA = "sample_data";
    public static String MAPPING = "mapping";
    public static String INDEX_EMBEDDING = "embedding";
    public static int DEFAULT_TIMEOUT_SECOND = 30;
    public static int TOKEN_LIMIT = 8192;

    public static int DEFAULT_N_SAMPLE_DATA = 5;

    private static final int BATCH_SIZE = 10;

    private final String jobName = this.getClass().getSimpleName();

    @Setter
    private List<String> adhocIndexName;

    @Setter
    private String adhocAgentId;

    private final LockService lockService;
    private final ThreadPool threadPool;

    @Setter
    private boolean incremental;

    public static final TimeValue JOB_LOCK_INTERVAL = TimeValue.timeValueMinutes(5);

    @Builder
    public IndexSummaryEmbeddingJob(
        Client client,
        ClusterService clusterService,
        IndicesHelper indicesHelper,
        MLClients mlClients,
        LockService lockService,
        ThreadPool threadPool
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.indicesHelper = indicesHelper;
        this.mlClients = mlClients;
        this.lockService = lockService;
        this.threadPool = threadPool;
    }

    @Override
    public void run() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        // if cluster have more than 1 node, try to avoid run job on cluster manager node
        if (nodes.isLocalNodeElectedClusterManager() && nodes.getSize() > 1) {
            log.debug("skip {} run on cluster manager node", jobName);
            return;
        }

        String lockId = lockId();
        Runnable releaseLock = () -> lockService
            .deleteLock(
                lockId,
                ActionListener.wrap(d -> log.debug("{} release lock", jobName), ex -> log.error("{} release lock failed", jobName, ex))
            );

        lockService.acquireLockWithId(jobName, JOB_LOCK_INTERVAL.seconds(), lockId, ActionListener.wrap(lockModel -> {

            threadPool.executor(INDEX_SUMMARY_JOB_THREAD_POOL).submit(() -> {
                if (lockModel == null) {
                    log.debug("Can't acquire lock for {} at node {}", jobName, clusterService.localNode().getName());
                    return;
                }

                log.info("{} starts", jobName);

                // search agent with IndexRoutingTool
                mlClients.getModelIdsForIndexRoutingTool(adhocAgentId, ActionListener.wrap(embeddingModelIds -> {
                    // no embedding model
                    if (embeddingModelIds.isEmpty()) {
                        return;
                    }

                    // mapping and simple data
                    Map<String, Map<String, Object>> indexMetaAndSamplesMap = getIndexMappingAndSampleData();

                    // no index at all
                    if (indexMetaAndSamplesMap.isEmpty()) {
                        return;
                    }

                    List<Map<String, Object>> indexMetaAndSamples = new ArrayList<>(indexMetaAndSamplesMap.values());
                    for (String modelId : embeddingModelIds) {
                        try {
                            List<List<Map<String, Object>>> partitions = Lists.partition(indexMetaAndSamples, BATCH_SIZE);
                            for (List<Map<String, Object>> partition : partitions) {
                                List<String> embeddingDocs = partition
                                    .stream()
                                    .map(sample -> (String) sample.get(INDEX_SUMMARY_FIELD))
                                    .collect(Collectors.toList());

                                List<ModelTensors> mlModelOutputs = mlClients
                                    .getEmbeddingResult(modelId, embeddingDocs, true, mlTaskResponse -> {
                                        ModelTensorOutput output = (ModelTensorOutput) mlTaskResponse.getOutput();
                                        return output.getMlModelOutputs();
                                    });

                                for (int i = 0; i < mlModelOutputs.size(); i++) {
                                    Number[] vector = mlModelOutputs.get(i).getMlModelTensors().get(0).getData();
                                    partition.get(i).put(INDEX_EMBEDDING, vector);
                                }

                                // write to k-NN index
                                indexSummaryVector(INDEX_SUMMARY_EMBEDDING_INDEX, partition, modelId);
                            }
                        } catch (Exception e) {
                            log.error("Failed to embedding index summary for model {}", modelId);
                        }
                    }

                    log.debug("{} finished", jobName);
                    releaseLock.run();
                }, e -> log.error("Search agent error happened", e)));
            });
        }, ex -> log.debug("can't acquired lock for {}", jobName, ex)));

    }

    private String lockId() {
        if (adhocIndexName != null && !adhocIndexName.isEmpty()) {
            return String.format(Locale.ROOT, "%s-%s", jobName, String.join("_", adhocIndexName));
        } else if (adhocAgentId != null) {
            return String.format(Locale.ROOT, "%s-%s", jobName, adhocAgentId);
        } else if (incremental) {
            return String.format(Locale.ROOT, "%s-incremental", jobName);
        } else {
            return jobName;
        }
    }

    /**
     * Get all qualified index with its mapping and sample data
     * @return index mapping and sample data map, key is index name, value is mapping and sample data
     */
    private Map<String, Map<String, Object>> getIndexMappingAndSampleData() {
        Metadata metadata = clusterService.state().metadata();
        Map<String, IndexMetadata> indices = new HashMap<>(metadata.indices());

        indices = filterByIndexNames(indices, adhocIndexName);

        if (incremental) {
            try {
                SearchRequest searchRequest = Requests.searchRequest(INDEX_SUMMARY_EMBEDDING_INDEX);
                TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(SAMPLE_DATA_INCLUDED_FIELD, false);
                searchRequest.source(new SearchSourceBuilder().query(termQueryBuilder).fetchField(INDEX_NAME_FIELD).size(1000));

                ActionFuture<SearchResponse> searchFuture = client.search(searchRequest);
                List<String> indexWithoutSampleData = new ArrayList<>();
                SearchResponse response = searchFuture.actionGet(DEFAULT_TIMEOUT_SECOND, TimeUnit.SECONDS);

                for (SearchHit hit : response.getHits()) {
                    String indexName = (String) hit.getSourceAsMap().get(INDEX_NAME_FIELD);
                    indexWithoutSampleData.add(indexName);
                }

                indices = filterByIndexNames(indices, indexWithoutSampleData);
            } catch (Exception e) {
                return Map.of();
            }
        }

        // data streams
        Map<String, DataStream> dataStreamMap = metadata.dataStreams();
        Map<String, String> indexDataStreamLookup = new HashMap<>();
        dataStreamMap.values().forEach(dataStream -> {
            String dsName = dataStream.getName();
            dataStream.getIndices().stream().map(Index::getName).forEach(it -> indexDataStreamLookup.put(it, dsName));
        });

        // index patterns from index template
        List<String> patterns = new ArrayList<>();
        for (final IndexTemplateMetadata template : metadata.templates().values()) {
            patterns.addAll(template.patterns());
        }
        for (ComposableIndexTemplate composableIndexTemplate : metadata.templatesV2().values()) {
            patterns.addAll(composableIndexTemplate.indexPatterns());
        }
        // TODO leverage index-pattern in OSD

        List<Map<String, Object>> indexSummaryList = new ArrayList<>();

        int totalTokens = 0;

        for (Map.Entry<String, IndexMetadata> indexEntry : indices.entrySet()) {
            Map<String, Object> map = new HashMap<>();
            String indexName = indexEntry.getKey();
            IndexMetadata indexMetadata = indexEntry.getValue();

            // if index have no mapping at all
            if (indexMetadata.mapping() == null) {
                log.debug("No mapping for index {}", indexName);
                continue;
            }

            // ignore system index or index name start with dot
            if (indexMetadata.isSystem() || (indexName.startsWith(".") && !indexDataStreamLookup.containsKey(indexName))) {
                log.debug("Skip system index {}", indexName);
                continue;
            }

            map.put(INDEX_NAME_FIELD, indexName);
            map.put(DATA_STREAM_FIELD, indexDataStreamLookup.get(indexName));
            map.put(ALIAS_FIELD, indexMetadata.getAliases().keySet());

            List<String> indexPatterns = patterns
                .stream()
                .filter(pattern -> Regex.simpleMatch(pattern, indexName))
                .collect(Collectors.toList());
            map.put(INDEX_PATTERNS_FIELD, indexPatterns);

            Map<String, Object> sourceAsMap = indexMetadata.mapping().getSourceAsMap();
            try (XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON)) {
                builder.map(sourceAsMap);
                String mapping = builder.toString();

                // sample data
                SearchRequest indexSearchRequest = Requests.searchRequest(indexName);
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(DEFAULT_N_SAMPLE_DATA);
                indexSearchRequest.source(sourceBuilder);

                ActionFuture<SearchResponse> searchFuture = client.search(indexSearchRequest);
                SearchResponse searchResponse = searchFuture.actionGet(DEFAULT_TIMEOUT_SECOND, TimeUnit.SECONDS);

                List<String> sampleDataList = new ArrayList<>();

                for (SearchHit hit : searchResponse.getHits()) {
                    String docContent = Strings.toString(MediaTypeRegistry.JSON, hit);
                    // TODO Remove long content field and knn field
                    sampleDataList.add(docContent);
                }

                String indexSummary = String.format(Locale.ROOT, "Index Mappings:%s\\nSample data:\\n%s", mapping, sampleDataList);
                totalTokens += countToken(indexSummary);

                map.put(MAPPING, mapping);
                map.put(SAMPLE_DATA, sampleDataList);

                indexSummaryList.add(map);

            } catch (Exception e) {
                log.error("Get index mapping and sample data failed for index {}", indexName, e);
            }
        }

        int nSample = adjustNSample(totalTokens, indices.size());

        for (Map<String, Object> summaryMap : indexSummaryList) {
            @SuppressWarnings("unchecked")
            List<String> sampleDataList = Optional.ofNullable((List<String>) summaryMap.remove(SAMPLE_DATA)).orElse(List.of());
            String mapping = (String) summaryMap.remove(MAPPING);
            String indexSummaryStr = String
                .format(Locale.ROOT, "Index Mappings:%s\\nSample data:\\n%s", mapping, sampleDataList.subList(0, nSample));
            summaryMap.put(INDEX_SUMMARY_FIELD, indexSummaryStr);
            summaryMap.put(SAMPLE_DATA_INCLUDED_FIELD, !sampleDataList.isEmpty());
            summaryMap.put(INDEX_SUMMARY_SIGN_FIELD, mappingSignature(mapping));
        }

        // post filter by existing data in system index, it will filter out index that mapping not changed
        Map<String, Map<String, Object>> indexMetaAndSamplesMap = indexSummaryList
            .stream()
            .collect(Collectors.toMap(m -> (String) m.get(INDEX_NAME_FIELD), m -> m));

        // bypass adhoc run
        if (incremental || !adhocIndexName.isEmpty()) {
            return indexMetaAndSamplesMap;
        }

        // below should only run for condition of cluster starts or node changed
        final FetchSourceContext fetchSourceContext = new FetchSourceContext(
            true,
            new String[] { INDEX_NAME_FIELD, INDEX_SUMMARY_SIGN_FIELD, SAMPLE_DATA_INCLUDED_FIELD },
            Strings.EMPTY_ARRAY
        );

        List<List<Map<String, Object>>> indexMetaAndSamplesPartitions = Lists.partition(indexSummaryList, 100);

        for (List<Map<String, Object>> indexMetaAndSamplesPartition : indexMetaAndSamplesPartitions) {
            // build mGet request
            MultiGetRequest multiGetRequest = new MultiGetRequest();

            for (Map<String, Object> indexMetaAndSample : indexMetaAndSamplesPartition) {
                String indexName = (String) indexMetaAndSample.get(INDEX_NAME_FIELD);
                MultiGetRequest.Item item = new MultiGetRequest.Item(
                    IndexSummaryEmbeddingJob.INDEX_SUMMARY_EMBEDDING_INDEX,
                    generateDocId(indexName)
                );
                item.fetchSourceContext(fetchSourceContext);
                multiGetRequest.add(item);
            }

            MultiGetResponse multiGetItemResponses = client.multiGet(multiGetRequest).actionGet(DEFAULT_TIMEOUT_SECOND, TimeUnit.SECONDS);
            for (MultiGetItemResponse response : multiGetItemResponses.getResponses()) {
                Map<String, Object> map = response.getResponse().getSourceAsMap();
                String indexName = (String) map.get(INDEX_NAME_FIELD);
                String signature = (String) map.get(INDEX_SUMMARY_SIGN_FIELD);

                Map<String, Object> indexSummaryEntry = indexMetaAndSamplesMap.get(indexName);
                // remove iff mapping not changed and have sample data included in embedding
                if (indexSummaryEntry.get(INDEX_SUMMARY_SIGN_FIELD).equals(signature)
                    && Boolean.TRUE.equals(map.get(SAMPLE_DATA_INCLUDED_FIELD))) {
                    indexMetaAndSamplesMap.remove(indexName);
                }
            }
        }

        return indexMetaAndSamplesMap;
    }

    private static String mappingSignature(String mapping) {
        return Hashing.sha512().hashString(mapping, StandardCharsets.UTF_8).toString();
    }

    private Map<String, IndexMetadata> filterByIndexNames(Map<String, IndexMetadata> indices, List<String> filteredIndexes) {
        if (Objects.isNull(filteredIndexes) || filteredIndexes.isEmpty()) {
            return indices;
        }

        return Maps.filterEntries(indices, entry -> filteredIndexes.contains(entry.getKey()));
    }

    /**
     * Adjust the number of document for sampling, if top k index summary token are too large(exceed limit 8K), we will try use 1 doc instead of 5 docs
     *
     * @param totalTokens total tokens
     * @param indexNumber total index number
     * @return adjusted number of documents for sampling
     */
    private static int adjustNSample(int totalTokens, int indexNumber) {
        // top k * singleIndexAverage token
        int singleIndexAverage = totalTokens / indexNumber;

        int nSample = DEFAULT_N_SAMPLE_DATA;
        if (singleIndexAverage * IndexRoutingTool.DEFAULT_K > TOKEN_LIMIT) {
            nSample = 1;
        }
        return nSample;
    }

    /**
     * encoding name: cl100k_base, model: gpt-4, gpt-3.5-turbo, text-embedding-ada-002
     *
     * @param sentence input need to encode
     * @return token numbers
     */
    private int countToken(String sentence) {
        EncodingRegistry registry = Encodings.newDefaultEncodingRegistry();
        Encoding enc = registry.getEncoding(EncodingType.CL100K_BASE);
        List<Integer> encoded = enc.encode(sentence);
        return encoded.size();
    }

    private void indexSummaryVector(String writeIndex, List<Map<String, Object>> docs, String modelId) {
        // init index and mapping
        indicesHelper.initIndexSummaryEmbeddingIndex(ActionListener.wrap(initialed -> {
            if (initialed) {
                BulkUpdateVectorField(writeIndex, docs, modelId);
            }
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                BulkUpdateVectorField(writeIndex, docs, modelId);
            } else {
                log.error("Failed to create index for index summary embedding", e);
            }
        }));
    }

    private void BulkUpdateVectorField(String writeIndex, List<Map<String, Object>> docs, String modelId) {
        indicesHelper.addNewVectorField(SKILLS_INDEX_SUMMARY.getIndexName(), modelId, ActionListener.wrap(r -> {
            if (r) {
                bulkWrite(writeIndex, docs, modelId);
            } else {
                log.error("Add vector field for model {} failed", modelId);
            }
        }, ex -> log.error("Add vector field for model {} failed", modelId, ex)));
    }

    private void bulkWrite(String writeIndex, List<Map<String, Object>> docs, String modelId) {
        BulkRequest bulkRequest = Requests.bulkRequest();
        for (Map<String, Object> doc : docs) {
            Number[] embedding = (Number[]) doc.get(INDEX_EMBEDDING);

            Map<String, Object> docMap = new HashMap<>();
            String indexName = (String) doc.get(INDEX_NAME_FIELD);
            docMap.put(INDEX_NAME_FIELD, indexName);
            docMap.put(INDEX_PATTERNS_FIELD, doc.get(INDEX_PATTERNS_FIELD));
            docMap.put(DATA_STREAM_FIELD, doc.get(DATA_STREAM_FIELD));
            docMap.put(ALIAS_FIELD, doc.get(ALIAS_FIELD));
            docMap.put(INDEX_SUMMARY_SIGN_FIELD, doc.get(INDEX_SUMMARY_SIGN_FIELD));
            docMap.put(SAMPLE_DATA_INCLUDED_FIELD, doc.get(SAMPLE_DATA_INCLUDED_FIELD));
            docMap.put(INDEX_SUMMARY_FIELD, doc.get(INDEX_SUMMARY_FIELD));
            docMap.put(INDEX_SUMMARY_EMBEDDING_FIELD_PREFIX + "_" + modelId, embedding);

            bulkRequest.add(new UpdateRequest(writeIndex, generateDocId(indexName)).doc(docMap, MediaTypeRegistry.JSON).docAsUpsert(true));
        }
        client.bulk(bulkRequest, ActionListener.wrap(r -> {
            if (r.hasFailures()) {
                log.error("Bulk create index summary embedding with failure {}", r.buildFailureMessage());
            } else {
                log.debug("Bulk create index summary embedding finished with {}", r.getTook());
            }
        }, exception -> log.error("Bulk create index summary embedding failed", exception)));
    }

    public void bulkDelete(String writeIndex, List<String> indexNames) {
        BulkRequest bulkRequest = Requests.bulkRequest();
        for (String indexName : indexNames) {
            bulkRequest.add(new DeleteRequest(writeIndex, generateDocId(indexName)));
        }
        client.bulk(bulkRequest, ActionListener.wrap(r -> {
            if (r.hasFailures()) {
                log.error("Bulk delete index summary embedding with failure {}", r.buildFailureMessage());
            } else {
                log.debug("Bulk delete index summary embedding finished with {}", r.getTook());
            }
        }, exception -> log.error("Bulk delete index summary embedding failed", exception)));
    }

    private String generateDocId(String indexName) {
        return Hashing.sha256().hashString(indexName, StandardCharsets.UTF_8).toString();
    }
}
