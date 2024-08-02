/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import java.util.List;

import lombok.Getter;

@Getter
public class RCADoc {

    public String phenomenon;
    public List<RCACause> causes;

    public RCADoc() {}
}
