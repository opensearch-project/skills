/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.indices;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@AllArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public enum SkillsIndexEnum {

    SKILLS_INDEX_SUMMARY(
        ".plugins-skills-index-summary",
        "/.plugins-skills-index-summary-setting.json",
        "/.plugins-skills-index-summary-mapping.json",
        0
    );

    private String indexName;
    private String setting;
    private String mapping;
    private int version;
}
