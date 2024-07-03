/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.agent.tools.utils;

import lombok.AllArgsConstructor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.Optional;

/**
 * This class is to encapsulate the {@link Settings} and {@link ClusterService} and provide a general method to retrieve dynamical cluster settings conveniently.
 */
@AllArgsConstructor
public class ClusterSettingHelper {

    private Settings settings;

    private ClusterService clusterService;

    /**
     * Retrieves the cluster settings for the specified setting.
     *
     * @param  setting   the setting to retrieve cluster settings for
     * @return           the cluster setting value, or the default setting value if not found
     */
    public <T> T getClusterSettings(Setting<T> setting) {
        return Optional.ofNullable(clusterService.getClusterSettings().get(setting))
            .orElse(setting.get(settings));
    }
}
