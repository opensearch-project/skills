/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Simplified Hierarchical Agglomerative Clustering for cosine distance with threshold-based stopping
 *
 * This implementation is optimized for a specific use case:
 * - Uses cosine distance metric only
 * - Stops clustering when distance threshold is exceeded
 * - Memory-efficient implementation without caching
 * - Supports standard linkage methods: single, complete, average
 *
 * Usage:
 * HierarchicalAgglomerativeClustering clustering = new HierarchicalAgglomerativeClustering(data);
 * List<ClusterNode> clusters = clustering.fit(LinkageMethod.COMPLETE, 0.5);
 */
public class HierarchicalAgglomerativeClustering {

    private final double[][] data;
    private final double[][] distanceMatrix;
    private final int nSamples;
    private final int nFeatures;

    public enum LinkageMethod {
        SINGLE,    // Minimum distance between clusters
        COMPLETE,  // Maximum distance between clusters
        AVERAGE    // Average distance between clusters
    }

    /**
     * Internal cluster node for tracking during clustering process
     */
    public static class ClusterNode {
        final int id;
        final List<Integer> samples;
        final int size;

        ClusterNode(int id, int sample) {
            this.id = id;
            this.samples = new ArrayList<>();
            this.samples.add(sample);
            this.size = 1;
        }

        ClusterNode(int id, ClusterNode left, ClusterNode right) {
            this.id = id;
            this.samples = new ArrayList<>();
            this.samples.addAll(left.samples);
            this.samples.addAll(right.samples);
            this.size = left.size + right.size;
        }
    }

    /**
     * Constructor - computes cosine distance matrix
     */
    public HierarchicalAgglomerativeClustering(double[][] data) {
        this.data = data;
        this.nSamples = data.length;
        this.nFeatures = data[0].length;
        this.distanceMatrix = new double[nSamples][nSamples];

        if (nSamples == 0) {
            throw new IllegalArgumentException("Input data cannot be empty");
        }

        // Compute cosine distance matrix
        computeCosineDistanceMatrix();
    }

    /**
     * Compute pairwise cosine distances
     * Cosine distance = 1 - cosine similarity
     */
    private void computeCosineDistanceMatrix() {
        // Pre-calculate norms for efficiency
        double[] norms = new double[nSamples];
        for (int i = 0; i < nSamples; i++) {
            double norm = 0.0;
            for (int j = 0; j < nFeatures; j++) {
                norm += data[i][j] * data[i][j];
            }
            norms[i] = Math.sqrt(norm);
        }

        // Calculate cosine distances
        for (int i = 0; i < nSamples; i++) {
            distanceMatrix[i][i] = 0.0;
            for (int j = i + 1; j < nSamples; j++) {
                double similarity = calculateCosineSimilarity(data[i], data[j], norms[i], norms[j]);
                double distance = 1.0 - similarity;
                distanceMatrix[i][j] = distanceMatrix[j][i] = distance;
            }
        }
    }

    /**
     * Optimized cosine similarity calculation with pre-calculated norms
     */
    private static double calculateCosineSimilarity(double[] a, double[] b, double normA, double normB) {
        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }

        double dotProduct = 0.0;
        for (int i = 0; i < a.length; i++) {
            dotProduct += a[i] * b[i];
        }

        return dotProduct / (normA * normB);
    }

    /**
     * Perform hierarchical clustering with distance threshold
     * 
     * @param linkage The linkage method to use
     * @param threshold Distance threshold - clustering stops when minimum distance exceeds this value
     * @return List of final clusters
     */
    public List<ClusterNode> fit(LinkageMethod linkage, double threshold) {
        if (threshold < 0) {
            throw new IllegalArgumentException("Distance threshold must be non-negative");
        }

        // Initialize clusters - each sample starts as its own cluster
        List<ClusterNode> activeClusters = new ArrayList<>();
        for (int i = 0; i < nSamples; i++) {
            activeClusters.add(new ClusterNode(i, i));
        }

        int nextClusterId = nSamples;

        // Main clustering loop
        while (activeClusters.size() > 1) {
            // Find closest pair of clusters
            int[] closestPair = findClosestClusters(activeClusters, linkage);
            if (closestPair == null) {
                break;
            }

            int i = closestPair[0];
            int j = closestPair[1];
            double minDistance = computeClusterDistance(activeClusters.get(i), activeClusters.get(j), linkage);

            // Stop if minimum distance exceeds threshold
            if (minDistance > threshold) {
                break;
            }

            // Merge the two closest clusters
            ClusterNode newCluster = new ClusterNode(nextClusterId++, activeClusters.get(i), activeClusters.get(j));

            // Remove old clusters and add new one
            activeClusters.remove(Math.max(i, j));
            activeClusters.remove(Math.min(i, j));
            activeClusters.add(newCluster);
        }

        return activeClusters;
    }

    /**
     * Find the two closest clusters
     */
    private int[] findClosestClusters(List<ClusterNode> clusters, LinkageMethod linkage) {
        double minDistance = Double.MAX_VALUE;
        int bestI = -1, bestJ = -1;

        for (int i = 0; i < clusters.size(); i++) {
            for (int j = i + 1; j < clusters.size(); j++) {
                double distance = computeClusterDistance(clusters.get(i), clusters.get(j), linkage);
                if (distance < minDistance) {
                    minDistance = distance;
                    bestI = i;
                    bestJ = j;
                }
            }
        }

        return (bestI == -1) ? null : new int[] { bestI, bestJ };
    }

    /**
     * Compute distance between clusters using specified linkage method
     */
    private double computeClusterDistance(ClusterNode c1, ClusterNode c2, LinkageMethod linkage) {
        return switch (linkage) {
            case SINGLE -> singleLinkage(c1, c2);
            case COMPLETE -> completeLinkage(c1, c2);
            case AVERAGE -> averageLinkage(c1, c2);
        };
    }

    /**
     * Single linkage: minimum distance between any two points in different clusters
     */
    private double singleLinkage(ClusterNode c1, ClusterNode c2) {
        double minDist = Double.MAX_VALUE;

        for (int i : c1.samples) {
            for (int j : c2.samples) {
                double dist = distanceMatrix[i][j];
                if (dist < minDist) {
                    minDist = dist;
                    // Early termination for very small distances
                    if (minDist < 1e-10) {
                        return minDist;
                    }
                }
            }
        }

        return minDist;
    }

    /**
     * Complete linkage: maximum distance between any two points in different clusters
     */
    private double completeLinkage(ClusterNode c1, ClusterNode c2) {
        double maxDist = Double.MIN_VALUE;

        for (int i : c1.samples) {
            for (int j : c2.samples) {
                double dist = distanceMatrix[i][j];
                if (dist > maxDist) {
                    maxDist = dist;
                }
            }
        }

        return maxDist;
    }

    /**
     * Average linkage: average distance between all pairs of points in different clusters
     */
    private double averageLinkage(ClusterNode c1, ClusterNode c2) {
        double sumDist = 0.0;
        int count = 0;

        for (int i : c1.samples) {
            for (int j : c2.samples) {
                sumDist += distanceMatrix[i][j];
                count++;
            }
        }

        return sumDist / count;
    }

    /**
     * Get cluster centroid (medoid) - the point with minimum total distance to other points in cluster
     */
    public int getClusterCentroid(ClusterNode cluster) {
        if (cluster.samples.size() == 1) {
            return cluster.samples.get(0);
        }

        int medoidIndex = cluster.samples.get(0);
        double minTotalDistance = Double.MAX_VALUE;

        for (int pointI : cluster.samples) {
            double totalDistance = 0.0;
            for (int pointJ : cluster.samples) {
                if (pointI != pointJ) {
                    totalDistance += distanceMatrix[pointI][pointJ];
                }
            }

            if (totalDistance < minTotalDistance) {
                minTotalDistance = totalDistance;
                medoidIndex = pointI;
            }
        }

        return medoidIndex;
    }

    /**
     * Convert clusters to label array
     */
    public int[] getLabels(List<ClusterNode> clusters) {
        int[] labels = new int[nSamples];

        for (int clusterId = 0; clusterId < clusters.size(); clusterId++) {
            ClusterNode cluster = clusters.get(clusterId);
            for (int pointIndex : cluster.samples) {
                labels[pointIndex] = clusterId;
            }
        }

        return labels;
    }

    /**
     * Backward compatibility method for cosine similarity calculation
     */
    public static double calculateCosineSimilarity(double[] a, double[] b) {
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < a.length; i++) {
            dotProduct += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }

        if (normA == 0 || normB == 0) {
            return 0;
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * Example usage for the simplified clustering
     */
    public static void main(String[] args) {
        System.out.println("Simplified Hierarchical Clustering - Cosine Distance with Threshold");
        System.out.println("===================================================================");

        // Example data - document vectors
        double[][] data = {
            { 1.0, 1.0, 1.0, 0.0, 0.0, 0.0 },    // Document 1: topic A
            { 1.0, 1.0, 0.5, 0.0, 0.0, 0.0 },    // Document 2: similar to topic A
            { 0.0, 0.0, 0.0, 1.0, 0.5, 1.0 },    // Document 3: topic B
            { 0.0, 0.0, 0.0, 0.9, 1.0, 1.0 },    // Document 4: similar to topic B
            { 0.5, 0.5, 0.5, 0.5, 0.5, 0.5 }     // Document 5: mixed topics
        };

        System.out.println("Input data: " + data.length + " documents with " + data[0].length + " features");

        HierarchicalAgglomerativeClustering clustering = new HierarchicalAgglomerativeClustering(data);

        // Test different thresholds
        double[] thresholds = { 0.3, 0.5, 0.7 };

        for (double threshold : thresholds) {
            System.out.println("\n--- Threshold: " + threshold + " ---");

            // Test different linkage methods
            for (LinkageMethod linkage : LinkageMethod.values()) {
                List<ClusterNode> clusters = clustering.fit(linkage, threshold);
                int[] labels = clustering.getLabels(clusters);

                System.out.printf("%s linkage: %d clusters, labels: %s%n", linkage, clusters.size(), Arrays.toString(labels));

                // Show cluster details
                for (int i = 0; i < clusters.size(); i++) {
                    ClusterNode cluster = clusters.get(i);
                    int centroid = clustering.getClusterCentroid(cluster);
                    System.out.printf("  Cluster %d: points %s (size: %d, centroid: %d)%n", i, cluster.samples, cluster.size, centroid);
                }
            }
        }

        // Performance test
        System.out.println("\n--- Performance Test ---");
        double[][] largeData = generateTestData(500, 50);

        long startTime = System.nanoTime();
        HierarchicalAgglomerativeClustering largeClustering = new HierarchicalAgglomerativeClustering(largeData);
        List<ClusterNode> largeClusters = largeClustering.fit(LinkageMethod.AVERAGE, 0.6);
        long endTime = System.nanoTime();

        double executionTime = (endTime - startTime) / 1_000_000.0;
        System.out.printf("Clustered %d documents into %d clusters in %.2f ms%n", largeData.length, largeClusters.size(), executionTime);

        // Show cluster size distribution
        Map<Integer, Integer> sizeDistribution = new HashMap<>();
        for (ClusterNode cluster : largeClusters) {
            sizeDistribution.merge(cluster.size, 1, Integer::sum);
        }
        System.out.println("Cluster size distribution: " + sizeDistribution);
    }

    /**
     * Generate random test data for performance testing
     */
    private static double[][] generateTestData(int nSamples, int nFeatures) {
        Random random = new Random(42); // Fixed seed for reproducibility
        double[][] data = new double[nSamples][nFeatures];

        for (int i = 0; i < nSamples; i++) {
            // Generate sparse vectors (common in text/document clustering)
            for (int j = 0; j < nFeatures; j++) {
                if (random.nextDouble() < 0.3) { // 30% chance of non-zero value
                    data[i][j] = random.nextDouble();
                }
            }

            // Normalize to unit vector (common preprocessing for cosine similarity)
            double norm = 0.0;
            for (double val : data[i]) {
                norm += val * val;
            }
            norm = Math.sqrt(norm);

            if (norm > 0) {
                for (int j = 0; j < nFeatures; j++) {
                    data[i][j] /= norm;
                }
            }
        }

        return data;
    }
}
