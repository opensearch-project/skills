/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

public class ToolConstants {
    // Detector state is not cleanly defined on the backend plugin. So, we persist a standard
    // set of states here for users to interface with when fetching and filtering detectors.
    // This follows what frontend AD users are familiar with, as we use the same parsing logic
    // in SearchAnomalyDetectorsTool.
    public static enum DetectorStateString {
        Running,
        Disabled,
        Failed,
        Initializing
    }

    // System indices constants are not cleanly exposed from the AD plugin, so we persist our
    // own constant here.
    public static final String AD_RESULTS_INDEX_PATTERN = ".opendistro-anomaly-results*";
    public static final String AD_DETECTORS_INDEX = ".opendistro-anomaly-detectors";
}
