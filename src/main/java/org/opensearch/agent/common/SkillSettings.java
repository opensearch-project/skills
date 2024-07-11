/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.common;

import org.opensearch.common.settings.Setting;

/**
 * Settings for skills plugin
 */
public final class SkillSettings {

    private SkillSettings() {}

    /**
     * This setting controls whether PPL execution is enabled or not
     */
    public static final Setting<Boolean> PPL_EXECUTION_ENABLED = Setting
        .boolSetting("plugins.skills.ppl_execution_enabled", false, Setting.Property.NodeScope, Setting.Property.Dynamic);
}
