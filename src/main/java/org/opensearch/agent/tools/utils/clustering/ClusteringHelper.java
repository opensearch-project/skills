/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils.clustering;

import static org.opensearch.agent.tools.utils.clustering.HierarchicalAgglomerativeClustering.calculateCosineSimilarity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.DistanceMeasure;

import com.google.common.collect.Lists;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class ClusteringHelper {
    private final double logVectorsClusteringThreshold;

    /**
     * Constructor for ClusteringHelper
     * 
     * @param logVectorsClusteringThreshold Threshold for determining when two vectors are similar
     *                                     Should be between 0 and 1.0 (inclusive)
     * @throws IllegalArgumentException if threshold is outside valid range
     */
    public ClusteringHelper(double logVectorsClusteringThreshold) {
        if (logVectorsClusteringThreshold < 0.0 || logVectorsClusteringThreshold > 1.0) {
            throw new IllegalArgumentException("Clustering threshold must be between 0.0 and 1.0, got: " + logVectorsClusteringThreshold);
        }
        this.logVectorsClusteringThreshold = logVectorsClusteringThreshold;
    }

    /**
     * Cluster log vectors using a two-phase approach and get representative vectors.
     * Input validation is performed to ensure log vectors are valid.
     *
     * @param logVectors Map of trace IDs to their vector representations
     * @return List of trace IDs representing the centroids of each cluster
     * @throws IllegalArgumentException if logVectors contains invalid entries
     */
    public List<String> clusterLogVectorsAndGetRepresentative(Map<String, double[]> logVectors) {
        if (logVectors == null || logVectors.isEmpty()) {
            return new ArrayList<>();
        }

        // Validate input vectors
        validateLogVectors(logVectors);

        log.debug("Starting two-phase clustering for {} log vectors", logVectors.size());

        // Convert map to arrays for processing
        double[][] vectors = new double[logVectors.size()][];
        Map<Integer, String> indexTraceIdMap = new HashMap<>();
        convertLogVectorsToArrays(logVectors, vectors, indexTraceIdMap);

        List<String> finalCentroids;

        // Choose clustering approach based on dataset size
        if (logVectors.size() > 1000) {
            finalCentroids = processTwoPhaseClusteringForLargeDataset(vectors, indexTraceIdMap);
        } else {
            // Small dataset - use hierarchical clustering directly
            finalCentroids = performClustering(vectors, indexTraceIdMap);
        }

        log
            .debug(
                "Two-phase clustering completed: {} input vectors -> {} representative centroids",
                logVectors.size(),
                finalCentroids.size()
            );

        return finalCentroids;
    }

    /**
     * Converts log vectors map to arrays for processing
     *
     * @param logVectors Map of trace IDs to vector representations
     * @param vectors Output array for vectors
     * @param indexTraceIdMap Output map for index to trace ID mapping
     */
    private void convertLogVectorsToArrays(Map<String, double[]> logVectors, double[][] vectors, Map<Integer, String> indexTraceIdMap) {
        int i = 0;
        for (Map.Entry<String, double[]> entry : logVectors.entrySet()) {
            vectors[i] = entry.getValue();
            indexTraceIdMap.put(i, entry.getKey());
            i++;
        }
    }

    /**
     * Processes large datasets using two-phase clustering approach
     *
     * @param vectors Array of vectors
     * @param indexTraceIdMap Mapping from vector index to trace ID
     * @return List of trace IDs representing cluster centroids
     */
    private List<String> processTwoPhaseClusteringForLargeDataset(double[][] vectors, Map<Integer, String> indexTraceIdMap) {
        List<String> finalCentroids = new ArrayList<>();
        log.debug("Large dataset detected ({}), applying K-means pre-clustering", vectors.length);

        // Calculate optimal number of K-means clusters (target 500 points per cluster)
        int targetClusterSize = 500;
        int numKMeansClusters = (vectors.length + (targetClusterSize - 1)) / targetClusterSize;

        log.debug("Using {} K-means clusters for pre-clustering", numKMeansClusters);

        try {
            List<List<Integer>> kMeansClusters = performKMeansClustering(vectors, numKMeansClusters);

            // Process each K-means cluster
            for (int clusterIdx = 0; clusterIdx < kMeansClusters.size(); clusterIdx++) {
                List<Integer> kMeansCluster = kMeansClusters.get(clusterIdx);
                log.debug("Processing K-means cluster {} with {} points", clusterIdx, kMeansCluster.size());

                List<String> clusterCentroids = processCluster(kMeansCluster, vectors, indexTraceIdMap, clusterIdx);
                finalCentroids.addAll(clusterCentroids);
            }

        } catch (Exception e) {
            log.warn("K-means clustering failed, falling back to hierarchical clustering only: {}", e.getMessage());
            // Fallback to hierarchical clustering only
            finalCentroids = performClustering(vectors, indexTraceIdMap);
        }

        return finalCentroids;
    }

    /**
     * Processes a single K-means cluster
     *
     * @param kMeansCluster List of indices in the K-means cluster
     * @param vectors Original vector array
     * @param indexTraceIdMap Original mapping from indices to trace IDs
     * @param clusterIdx Index of the cluster (for logging)
     * @return List of trace IDs representing cluster centroids
     */
    private List<String> processCluster(
        List<Integer> kMeansCluster,
        double[][] vectors,
        Map<Integer, String> indexTraceIdMap,
        int clusterIdx
    ) {
        if (kMeansCluster.isEmpty()) {
            return List.of();
        }

        if (kMeansCluster.size() == 1) {
            return List.of(indexTraceIdMap.get(kMeansCluster.getFirst()));
        }

        if (kMeansCluster.size() > 500) {
            log.debug("The cluster size is greater than 500, performing partitioned clustering");
            return performHierarchicalClusteringOfPartition(kMeansCluster, vectors, indexTraceIdMap);
        }

        log.debug("Applying hierarchical clustering to K-means cluster {} with {} points", clusterIdx, kMeansCluster.size());

        // Extract vectors for this K-means cluster
        double[][] clusterVectors = extractVectors(kMeansCluster, vectors);
        Map<Integer, String> clusterIndexTraceIdMap = createTraceIdMapping(kMeansCluster, indexTraceIdMap);

        // Apply hierarchical clustering within this K-means cluster
        return performClustering(clusterVectors, clusterIndexTraceIdMap);
    }

    /**
     * Perform K-means clustering using Apache Commons Math3
     *
     * @param vectors Input vectors for clustering
     * @param numClusters Number of K-means clusters
     * @return List of clusters, each containing indices of points in that cluster
     * @throws RuntimeException if clustering fails
     */
    private List<List<Integer>> performKMeansClustering(double[][] vectors, int numClusters) {
        if (vectors == null || vectors.length == 0) {
            return new ArrayList<>();
        }

        if (numClusters <= 0) {
            numClusters = 1;
        }

        // Cap number of clusters to vector size
        numClusters = Math.min(numClusters, vectors.length);

        try {
            KMeansPlusPlusClusterer<DoublePoint> clusterer = createKMeansClusterer(numClusters);
            List<DoublePoint> points = convertVectorsToPoints(vectors);
            List<CentroidCluster<DoublePoint>> clusters = clusterer.cluster(points);
            return extractClusterIndices(clusters, vectors);
        } catch (Exception e) {
            log.error("K-means clustering failed: {}", e.getMessage(), e);
            throw new RuntimeException("K-means clustering failed: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a KMeansPlusPlusClusterer with cosine distance metric
     *
     * @param numClusters Number of clusters to create
     * @return Configured KMeansPlusPlusClusterer
     */
    private KMeansPlusPlusClusterer<DoublePoint> createKMeansClusterer(int numClusters) {
        return new KMeansPlusPlusClusterer<>(
            numClusters,
            300, // Maximum iterations
            (DistanceMeasure) (a, b) -> 1 - calculateCosineSimilarity(a, b)
        );
    }

    /**
     * Converts vector array to list of DoublePoint objects
     *
     * @param vectors Array of vectors
     * @return List of DoublePoint objects
     */
    private List<DoublePoint> convertVectorsToPoints(double[][] vectors) {
        List<DoublePoint> points = new ArrayList<>(vectors.length);
        for (double[] vector : vectors) {
            points.add(new DoublePoint(vector));
        }
        return points;
    }

    /**
     * Validates log vectors to ensure they are valid for clustering
     *
     * @param logVectors Map of trace IDs to vector representations
     * @throws IllegalArgumentException if vectors are invalid
     */
    private void validateLogVectors(Map<String, double[]> logVectors) {
        int vectorDimension = -1;

        for (Map.Entry<String, double[]> entry : logVectors.entrySet()) {
            String traceId = entry.getKey();
            double[] vector = entry.getValue();

            if (traceId == null || traceId.isEmpty()) {
                throw new IllegalArgumentException("Trace ID cannot be null or empty");
            }

            if (vector == null) {
                throw new IllegalArgumentException("Vector for trace ID '" + traceId + "' is null");
            }

            if (vector.length == 0) {
                throw new IllegalArgumentException("Vector for trace ID '" + traceId + "' is empty");
            }

            // Ensure all vectors have the same dimension
            if (vectorDimension == -1) {
                vectorDimension = vector.length;
            } else if (vector.length != vectorDimension) {
                throw new IllegalArgumentException(
                    "Vector dimension mismatch: expected "
                        + vectorDimension
                        + " but got "
                        + vector.length
                        + " for trace ID '"
                        + traceId
                        + "'"
                );
            }

            // Check for NaN or Infinity values
            for (int i = 0; i < vector.length; i++) {
                if (Double.isNaN(vector[i]) || Double.isInfinite(vector[i])) {
                    throw new IllegalArgumentException(
                        "Vector for trace ID '" + traceId + "' contains invalid value at index " + i + ": " + vector[i]
                    );
                }
            }
        }
    }

    /**
     * Extracts original vector indices for each K-means cluster
     *
     * @param clusters K-means clustering result
     * @param vectors Original vector array
     * @return List of clusters with original vector indices
     */
    private List<List<Integer>> extractClusterIndices(List<CentroidCluster<DoublePoint>> clusters, double[][] vectors) {
        List<List<Integer>> result = new ArrayList<>();
        for (CentroidCluster<DoublePoint> cluster : clusters) {
            List<Integer> clusterIndices = new ArrayList<>();
            for (DoublePoint point : cluster.getPoints()) {
                // Find the original index of this point
                for (int i = 0; i < vectors.length; i++) {
                    if (Arrays.equals(vectors[i], point.getPoint())) {
                        clusterIndices.add(i);
                        break;
                    }
                }
            }
            if (!clusterIndices.isEmpty()) {
                result.add(clusterIndices);
            }
        }
        return result;
    }

    /**
     * Generic method to perform clustering with specified linkage method
     *
     * @param vectors         Input vectors for clustering
     * @param indexTraceIdMap Mapping from vector index to trace ID
     * @return List of trace IDs representing cluster centroids
     */
    private List<String> performClustering(double[][] vectors, Map<Integer, String> indexTraceIdMap) {
        if (vectors == null || vectors.length == 0) {
            return List.of();
        }

        if (vectors.length == 1) {
            String traceId = indexTraceIdMap.get(0);
            return List.of(traceId);
        }

        List<String> centroids = new ArrayList<>();
        try {
            HierarchicalAgglomerativeClustering hac = new HierarchicalAgglomerativeClustering(vectors);
            List<HierarchicalAgglomerativeClustering.ClusterNode> clusters = hac
                .fit(HierarchicalAgglomerativeClustering.LinkageMethod.COMPLETE, this.logVectorsClusteringThreshold);

            for (HierarchicalAgglomerativeClustering.ClusterNode cluster : clusters) {
                int centroidIndex = hac.getClusterCentroid(cluster);
                String traceId = indexTraceIdMap.get(centroidIndex);
                centroids.add(traceId);
            }
        } catch (Exception e) {
            log.error("Hierarchical clustering failed: {}", e.getMessage(), e);
            // Fallback: return first point as representative if available
            String traceId = indexTraceIdMap.get(0);
            centroids.add(traceId);
        }

        return centroids;
    }

    /**
     * If the first stage K-means clustering results exceed 500 clusters, implement batch processing and merge the results.
     * @param kMeansCluster Clustering results from the first stage.
     * @param vectors List of vectors by index.
     * @param indexTraceIdMap Map of index to their trace id.
     * @return List of trace IDs representing cluster centroids after partitioned processing
     */
    private List<String> performHierarchicalClusteringOfPartition(
        List<Integer> kMeansCluster,
        double[][] vectors,
        Map<Integer, String> indexTraceIdMap
    ) {
        List<List<Integer>> partition = Lists.partition(kMeansCluster, 500);

        List<double[]> vectorRes = new ArrayList<>();
        Map<Integer, String> index2Trace = new HashMap<>();

        for (List<Integer> partList : partition) {
            double[][] clusterVectors = extractVectors(partList, vectors);
            Map<Integer, String> clusterIndexTraceIdMap = createTraceIdMapping(partList, indexTraceIdMap);

            log.debug("Starting performHierarchicalClusteringOfPartition!");
            processPartition(clusterVectors, clusterIndexTraceIdMap, vectorRes, index2Trace);
        }

        return removeSimilarVectors(vectorRes, index2Trace);
    }

    /**
     * Extracts vectors for a partition based on indices
     * 
     * @param partList List of indices in the partition
     * @param vectors Original vector array
     * @return Array of vectors for the partition
     */
    private double[][] extractVectors(List<Integer> partList, double[][] vectors) {
        double[][] clusterVectors = new double[partList.size()][];
        for (int j = 0; j < partList.size(); j++) {
            int originalIndex = partList.get(j);
            clusterVectors[j] = vectors[originalIndex];
        }
        return clusterVectors;
    }

    /**
     * Creates a mapping from partition indices to trace IDs
     * 
     * @param partList List of indices in the partition
     * @param indexTraceIdMap Original mapping from indices to trace IDs
     * @return Mapping from partition indices to trace IDs
     */
    private Map<Integer, String> createTraceIdMapping(List<Integer> partList, Map<Integer, String> indexTraceIdMap) {
        Map<Integer, String> clusterIndexTraceIdMap = new HashMap<>();
        for (int j = 0; j < partList.size(); j++) {
            int originalIndex = partList.get(j);
            clusterIndexTraceIdMap.put(j, indexTraceIdMap.get(originalIndex));
        }
        return clusterIndexTraceIdMap;
    }

    /**
     * Processes a partition for hierarchical clustering
     * 
     * @param clusterVectors Vectors in the partition
     * @param clusterIndexTraceIdMap Mapping from partition indices to trace IDs
     * @param vectorRes Result vector collection to append to
     * @param index2Trace Result mapping from indices to trace IDs to append to
     */
    private void processPartition(
        double[][] clusterVectors,
        Map<Integer, String> clusterIndexTraceIdMap,
        List<double[]> vectorRes,
        Map<Integer, String> index2Trace
    ) {
        if (clusterVectors.length == 0) {
            return;
        }

        if (clusterVectors.length == 1) {
            vectorRes.add(clusterVectors[0]);
            index2Trace.put(vectorRes.size() - 1, clusterIndexTraceIdMap.get(0));
            return;
        }

        try {
            HierarchicalAgglomerativeClustering hac = new HierarchicalAgglomerativeClustering(clusterVectors);
            List<HierarchicalAgglomerativeClustering.ClusterNode> clusters = hac
                .fit(HierarchicalAgglomerativeClustering.LinkageMethod.COMPLETE, this.logVectorsClusteringThreshold);
            log.info("Completing performHierarchicalClusteringOfPartition!");

            for (HierarchicalAgglomerativeClustering.ClusterNode cluster : clusters) {
                int centroidIndex = hac.getClusterCentroid(cluster);
                vectorRes.add(clusterVectors[centroidIndex]);
                index2Trace.put(vectorRes.size() - 1, clusterIndexTraceIdMap.get(centroidIndex));
            }
        } catch (Exception e) {
            log.error("Hierarchical clustering failed: {}", e.getMessage(), e);
            // Fallback: return first point as representative
            vectorRes.add(clusterVectors[0]);
            index2Trace.put(vectorRes.size() - 1, clusterIndexTraceIdMap.get(0));
        }
    }

    /**
     * Compute the cosine similarity pairwise and remove vectors that are too similar.
     * Vectors with similarity higher than threshold are considered duplicates.
     * 
     * @param vectorRes List of vectors
     * @param index2Trace Map of index to their trace id
     * @return List of trace IDs after removing similar vectors
     */
    private List<String> removeSimilarVectors(List<double[]> vectorRes, Map<Integer, String> index2Trace) {
        Set<Integer> toRemove = new HashSet<>();

        for (int i = 0; i < vectorRes.size(); i++) {
            if (toRemove.contains(i)) {
                continue;
            }

            for (int j = i + 1; j < vectorRes.size(); j++) {
                if (toRemove.contains(j)) {
                    continue;
                }

                double similarity = calculateCosineSimilarity(vectorRes.get(i), vectorRes.get(j));
                // If similarity is higher than threshold, vectors are considered similar enough to remove one
                if (similarity > this.logVectorsClusteringThreshold) {
                    log.debug("Removing similar vector with similarity: {}", similarity);
                    toRemove.add(j);
                }
            }
        }

        log.debug("Removed {} similar vectors out of {}", toRemove.size(), vectorRes.size());
        return collectNonRemovedTraceIds(vectorRes, index2Trace, toRemove);
    }

    /**
     * Collects trace IDs for vectors that are not marked for removal
     * 
     * @param vectors List of vectors
     * @param indexToTraceMap Mapping from indices to trace IDs
     * @param indicesToRemove Set of indices to exclude
     * @return List of trace IDs for non-removed vectors
     */
    private List<String> collectNonRemovedTraceIds(
        List<double[]> vectors,
        Map<Integer, String> indexToTraceMap,
        Set<Integer> indicesToRemove
    ) {
        List<String> result = new ArrayList<>(vectors.size() - indicesToRemove.size());
        for (int i = 0; i < vectors.size(); i++) {
            if (!indicesToRemove.contains(i)) {
                result.add(indexToTraceMap.get(i));
            }
        }
        return result;
    }

}
