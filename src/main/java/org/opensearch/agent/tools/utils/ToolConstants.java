/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import java.util.Locale;

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

    public enum ModelType {
        CLAUDE,
        OPENAI;

        public static ModelType from(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    // System indices constants are not cleanly exposed from the AD & Alerting plugins, so we persist our
    // own constants here.
    public static final String AD_RESULTS_INDEX_PATTERN = ".opendistro-anomaly-results*";
    public static final String AD_RESULTS_INDEX = ".opendistro-anomaly-results";
    public static final String AD_DETECTORS_INDEX = ".opendistro-anomaly-detectors";

    public static final String ALERTING_CONFIG_INDEX = ".opendistro-alerting-config";
    public static final String ALERTING_ALERTS_INDEX = ".opendistro-alerting-alerts";

    public static final String TOOL_REQUIRED_PARAMS = "required_parameters";
}
