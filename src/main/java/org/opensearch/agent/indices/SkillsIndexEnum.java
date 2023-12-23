/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.indices;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum SkillsIndexEnum {

    SKILLS_INDEX_SUMMARY_EMBEDDING_INDEX(
        ".index_summary_embedding_index",
        "/.index_summary_embedding_index_setting.json",
        "/.index_summary_embedding_index_mapping.json",
        0
    );

    private String indexName;
    private String setting;
    private String mapping;
    private int version;
}
